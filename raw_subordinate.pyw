import base64
import json
import os
import queue
import re
import socket
import threading
import time
import tkinter as tk
import tkinter.font as tkfont
from tkinter import filedialog, messagebox, ttk
from tkinter.scrolledtext import ScrolledText

import requests
try:
    import pystray  # type: ignore
    from PIL import Image  # type: ignore

    _PYSTRAY_AVAILABLE = True
except Exception:  # noqa: BLE001
    pystray = None
    Image = None
    _PYSTRAY_AVAILABLE = False

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
PRESET_SETTINGS_FILE = os.path.join(os.path.dirname(__file__), "preset_settings.json")

DEFAULT_SETTINGS = {
    "api_timeout_seconds": 10.0,
    "friend_scan_request_delay_seconds": 1.0 / 3.0,
    "friend_collection_min_interval_seconds": 1.0 / 50.0,
    "rate_limit_wait_seconds": 60,
}
PEER_PROTOCOL_VERSION = 1
PEER_SOCKET_TIMEOUT_SECONDS = 45.0
PEER_PING_TIMEOUT_SECONDS = 4.0
DEFAULT_AVATAR_FILES = {
    "idle": os.path.join(BASE_DIR, "assets", "avatars", "idle.gif"),
    "timeout": os.path.join(BASE_DIR, "assets", "avatars", "timedout.gif"),
    "working": os.path.join(BASE_DIR, "assets", "avatars", "working.gif"),
    "offline": os.path.join(BASE_DIR, "assets", "avatars", "offline.gif"),
}


def _load_preset_settings(file_path=PRESET_SETTINGS_FILE):
    defaults = {
        "peer_name": "Unnamed Peer",
        "listen_host": "0.0.0.0",
        "listen_port": 47931,
        "idle_image": "",
        "timeout_image": "",
        "working_image": "",
        "offline_image": "",
    }
    if not os.path.exists(file_path):
        return defaults, False
    try:
        with open(file_path, "r", encoding="utf-8") as handle:
            data = json.load(handle)
        if isinstance(data, dict):
            defaults.update(data)
    except (OSError, json.JSONDecodeError):
        return defaults, False
    return defaults, True


def _save_preset_settings(settings, file_path=PRESET_SETTINGS_FILE):
    payload = settings if isinstance(settings, dict) else {}
    with open(file_path, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=True, indent=2)


def _recv_exact(sock, size):
    chunks = []
    remaining = int(size)
    while remaining > 0:
        chunk = sock.recv(remaining)
        if not chunk:
            raise ConnectionError("Socket closed while receiving data.")
        chunks.append(chunk)
        remaining -= len(chunk)
    return b"".join(chunks)


def _send_json_message(sock, payload):
    body = json.dumps(payload, ensure_ascii=True).encode("utf-8")
    header = f"{len(body):08d}".encode("ascii")
    sock.sendall(header + body)


def _recv_json_message(sock):
    header = _recv_exact(sock, 8)
    size_text = header.decode("ascii")
    if not size_text.isdigit():
        raise ValueError("Invalid message header.")
    body_size = int(size_text)
    body = _recv_exact(sock, body_size) if body_size > 0 else b"{}"
    data = json.loads(body.decode("utf-8"))
    return data if isinstance(data, dict) else {}


def _request_json_with_backoff(
    url,
    timeout_seconds=10,
    params=None,
    max_attempts=4,
    should_abort=None,
    fast_fail_429=False,
    rate_limit_wait_seconds=0,
):
    last_error = None
    for attempt in range(1, max_attempts + 1):
        if should_abort and should_abort():
            raise RuntimeError("Request cancelled.")
        try:
            response = requests.get(url, timeout=timeout_seconds, params=params)
            if response.status_code == 429:
                if fast_fail_429:
                    raise requests.HTTPError(
                        f"429 Too Many Requests for url: {response.url}",
                        response=response,
                    )
                if should_abort and should_abort():
                    raise RuntimeError("Request cancelled.")
                wait_seconds = max(1, int(rate_limit_wait_seconds or 0))
                if wait_seconds <= 0:
                    retry_after = response.headers.get("Retry-After")
                    try:
                        wait_seconds = int(max(1.0, float(retry_after)))
                    except (TypeError, ValueError):
                        wait_seconds = int(min(30.0, 1.5 * attempt))
                time.sleep(wait_seconds)
                last_error = requests.HTTPError(
                    f"429 Too Many Requests for url: {response.url}",
                    response=response,
                )
                continue
            response.raise_for_status()
            data = response.json() if response.content else {}
            return data if isinstance(data, dict) else {}
        except requests.RequestException as exc:
            last_error = exc
            if attempt < max_attempts:
                if should_abort and should_abort():
                    raise RuntimeError("Request cancelled.")
                time.sleep(min(10.0, float(attempt)))
    if last_error:
        raise last_error
    return {}


def _scan_friend_groups_chunk(
    friend_infos,
    blacklist,
    whitelist,
    report_non_blacklisted=False,
    timeout_seconds=10.0,
    rate_limit_wait_seconds=60,
    request_delay_seconds=0.0,
    should_stop=None,
    on_progress=None,
):
    entries = []
    report_hits = 0
    caught_friend_ids = set()

    friends_list = friend_infos if isinstance(friend_infos, list) else []
    total = max(1, len(friends_list))
    for idx, friend in enumerate(friends_list, start=1):
        if should_stop and should_stop():
            raise RuntimeError("Scan cancelled.")
        if on_progress:
            on_progress(idx, total)

        info = friend if isinstance(friend, dict) else {}
        friend_id = str(info.get("id") or "").strip()
        if not friend_id:
            continue
        username = str(info.get("name") or friend_id).strip()
        similar = bool(info.get("similar_to_target", False))

        groups = []
        try:
            groups_data = _request_json_with_backoff(
                f"https://groups.roblox.com/v2/users/{friend_id}/groups/roles",
                timeout_seconds=timeout_seconds,
                max_attempts=4,
                should_abort=should_stop,
                rate_limit_wait_seconds=rate_limit_wait_seconds,
            )
            groups = groups_data.get("data", []) if isinstance(groups_data, dict) else []
        except requests.HTTPError as exc:
            status_code = getattr(exc.response, "status_code", None)
            if status_code not in (400, 404):
                raise
        if request_delay_seconds > 0:
            time.sleep(float(request_delay_seconds))

        friend_hits = []
        has_blacklisted_group = False
        for raw_entry in groups if isinstance(groups, list) else []:
            entry = raw_entry if isinstance(raw_entry, dict) else {}
            group_info = entry.get("group") if isinstance(entry.get("group"), dict) else {}
            group_id = group_info.get("id")
            if group_id is None:
                continue
            link = f"https://www.roblox.com/communities/{group_id}"
            in_blacklist = link in blacklist
            if in_blacklist:
                has_blacklisted_group = True
            is_whitelisted = link in whitelist
            if (report_non_blacklisted and not in_blacklist and not is_whitelisted) or (
                not report_non_blacklisted and in_blacklist
            ):
                friend_hits.append(link)

        if has_blacklisted_group:
            caught_friend_ids.add(friend_id)

        should_write_friend = has_blacklisted_group if report_non_blacklisted else bool(friend_hits)
        if should_write_friend:
            report_hits += len(friend_hits)

        entries.append(
            {
                "friend_id": friend_id,
                "username": username,
                "similar_to_target": similar,
                "friend_hits": friend_hits,
                "has_blacklisted_group": has_blacklisted_group,
                "should_write_friend": should_write_friend,
            }
        )

    return {
        "entries": entries,
        "report_hits": report_hits,
        "caught_friend_ids": sorted(list(caught_friend_ids)),
    }


def _collect_friend_graph_chunk(
    user_depth_pairs,
    recursions,
    timeout_seconds=10.0,
    rate_limit_wait_seconds=60,
    request_delay_seconds=0.0,
    should_stop=None,
    on_progress=None,
):
    results = []
    processed = 0
    pairs = user_depth_pairs if isinstance(user_depth_pairs, list) else []
    total = max(1, len(pairs))
    for idx, (user_id, depth) in enumerate(pairs, start=1):
        if should_stop and should_stop():
            raise RuntimeError("Collect cancelled.")
        processed += 1
        if on_progress:
            on_progress(idx, total)
        if int(depth) >= int(recursions):
            continue
        data = _request_json_with_backoff(
            f"https://friends.roblox.com/v1/users/{user_id}/friends",
            timeout_seconds=timeout_seconds,
            max_attempts=4,
            should_abort=should_stop,
            rate_limit_wait_seconds=rate_limit_wait_seconds,
        )
        friends = data.get("data", []) if isinstance(data, dict) else []
        for friend in friends:
            friend_info = friend if isinstance(friend, dict) else {}
            friend_id = friend_info.get("id")
            if friend_id is None:
                continue
            friend_id = str(friend_id)
            results.append(
                {
                    "id": friend_id,
                    "name": str(friend_info.get("name", "")),
                    "display_name": str(friend_info.get("displayName", "")),
                    "depth": int(depth) + 1,
                    "source_user": str(user_id),
                }
            )
        if request_delay_seconds > 0:
            time.sleep(float(request_delay_seconds))
    return {"friends": results, "processed_users": processed}


def _resolve_usernames_chunk(
    friend_ids,
    timeout_seconds=10.0,
    rate_limit_wait_seconds=60,
    request_delay_seconds=0.0,
    should_stop=None,
    on_progress=None,
):
    resolved = []
    processed = 0
    ids = friend_ids if isinstance(friend_ids, list) else []
    total = max(1, len(ids))
    for idx, friend_id in enumerate(ids, start=1):
        if should_stop and should_stop():
            raise RuntimeError("Resolve cancelled.")
        processed += 1
        if on_progress:
            on_progress(idx, total)
        profile_data = {}
        try:
            profile_data = _request_json_with_backoff(
                f"https://users.roblox.com/v1/users/{friend_id}",
                timeout_seconds=timeout_seconds,
                max_attempts=4,
                should_abort=should_stop,
                rate_limit_wait_seconds=rate_limit_wait_seconds,
            )
        except requests.HTTPError as exc:
            status_code = getattr(exc.response, "status_code", None)
            if status_code not in (400, 404):
                raise
        name_value = str(profile_data.get("name") or friend_id) if isinstance(profile_data, dict) else str(friend_id)
        display_value = str(profile_data.get("displayName") or "") if isinstance(profile_data, dict) else ""
        resolved.append({"id": str(friend_id), "name": name_value, "display_name": display_value})
        if request_delay_seconds > 0:
            time.sleep(float(request_delay_seconds))
    return {"resolved": resolved, "processed_users": processed}

class PeerWorkerServer:
    def __init__(self, host, port, on_log=None, on_progress=None, on_chat=None):
        self.host = str(host).strip() or "0.0.0.0"
        self.port = int(port)
        self.on_log = on_log
        self.on_progress = on_progress
        self.on_chat = on_chat
        self.peer_name = "Unnamed Peer"
        self.peer_images = {}
        self.last_manager_host = None
        self.last_manager_chat_port = None
        self.current_task = {"label": "Idle", "current": 0, "total": 1, "status": "Idle"}
        self._stop_event = threading.Event()
        self._abort_task_event = threading.Event()
        self._thread = None
        self._socket = None

    def _log(self, message):
        if self.on_log:
            self.on_log(message)

    def _progress(self, label, current, total):
        self.current_task = {
            "label": str(label),
            "current": int(current),
            "total": int(total),
            "status": "Working" if int(current) < int(total) else "Idle",
        }
        if self.on_progress:
            self.on_progress(label, current, total)

    def _chat(self, sender, message):
        if self.on_chat:
            self.on_chat(str(sender), str(message))

    def start(self):
        if self._thread and self._thread.is_alive():
            return
        self._stop_event.clear()
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def stop(self):
        self._stop_event.set()
        try:
            if self._socket:
                self._socket.close()
        except OSError:
            pass

    def is_running(self):
        return bool(self._thread and self._thread.is_alive())

    def _run(self):
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_sock:
                self._socket = server_sock
                server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                server_sock.bind((self.host, self.port))
                server_sock.listen(24)
                server_sock.settimeout(1.0)
                self._log(f"Peer worker listening on {self.host}:{self.port}")
                while not self._stop_event.is_set():
                    try:
                        conn, _addr = server_sock.accept()
                    except socket.timeout:
                        continue
                    except OSError:
                        break
                    threading.Thread(target=self._handle_client, args=(conn,), daemon=True).start()
        except Exception as exc:  # noqa: BLE001
            self._log(f"Peer worker stopped due to error: {exc}")
        finally:
            self._socket = None
            self._log("Peer worker stopped.")

    def _handle_client(self, conn):
        with conn:
            try:
                conn.settimeout(PEER_SOCKET_TIMEOUT_SECONDS)
                payload = _recv_json_message(conn)
                message_type = str(payload.get("type") or "").strip().lower()
                if message_type == "ping":
                    _send_json_message(
                        conn,
                        {
                            "type": "pong",
                            "status": "ok",
                            "protocol": PEER_PROTOCOL_VERSION,
                        },
                    )
                    return
                if message_type == "peer_info":
                    _send_json_message(
                        conn,
                        {
                            "type": "peer_info_result",
                            "status": "ok",
                            "protocol": PEER_PROTOCOL_VERSION,
                            "name": self.peer_name,
                            "images": self.peer_images,
                        },
                    )
                    return
                if message_type == "peer_stop":
                    self._abort_task_event.set()
                    snapshot = dict(self.current_task)
                    _send_json_message(
                        conn,
                        {
                            "type": "peer_stop_result",
                            "status": "ok",
                            "protocol": PEER_PROTOCOL_VERSION,
                            "task": snapshot,
                        },
                    )
                    return
                if message_type == "session_start":
                    try:
                        peer_host, _peer_port = conn.getpeername()
                        self.last_manager_host = peer_host
                    except Exception:
                        pass
                    manager_chat_port = payload.get("manager_chat_port")
                    try:
                        if manager_chat_port is not None:
                            self.last_manager_chat_port = int(manager_chat_port)
                    except (TypeError, ValueError):
                        pass
                    session_action = str(payload.get("action") or "started")
                    session_id = str(payload.get("session_id") or "")
                    phase = str(payload.get("phase") or "")
                    self._log(
                        f"Manager {session_action} a friend report {session_id} (phase: {phase}). "
                        "Waiting for tasks..."
                    )
                    self._progress("Peer Task - Waiting", 0, 1)
                    _send_json_message(
                        conn,
                        {
                            "type": "session_ack",
                            "status": "ok",
                            "protocol": PEER_PROTOCOL_VERSION,
                        },
                    )
                    return
                if message_type == "chat":
                    try:
                        peer_host, _peer_port = conn.getpeername()
                        self.last_manager_host = peer_host
                    except Exception:
                        pass
                    manager_chat_port = payload.get("manager_chat_port")
                    try:
                        if manager_chat_port is not None:
                            self.last_manager_chat_port = int(manager_chat_port)
                    except (TypeError, ValueError):
                        pass
                    sender = str(payload.get("from") or "Peer")
                    message = str(payload.get("message") or "")
                    if message:
                        self._chat(sender, message)
                    _send_json_message(
                        conn,
                        {
                            "type": "chat_ack",
                            "status": "ok",
                            "protocol": PEER_PROTOCOL_VERSION,
                        },
                    )
                    return
                if message_type != "scan_chunk":
                    if message_type not in ("collect_chunk", "resolve_chunk"):
                        raise ValueError("Unsupported request type.")

                protocol = int(payload.get("protocol", 0))
                if protocol != PEER_PROTOCOL_VERSION:
                    raise ValueError("Protocol mismatch.")

                settings = payload.get("settings") if isinstance(payload.get("settings"), dict) else {}
                request_delay_seconds = float(
                    settings.get("friend_scan_request_delay_seconds", DEFAULT_SETTINGS["friend_scan_request_delay_seconds"])
                )
                timeout_seconds = float(settings.get("api_timeout_seconds", DEFAULT_SETTINGS["api_timeout_seconds"]))
                rate_limit_wait_seconds = int(settings.get("rate_limit_wait_seconds", DEFAULT_SETTINGS["rate_limit_wait_seconds"]))
                self._abort_task_event.clear()

                def send_progress(label, current, total, status="Working"):
                    self._progress(label, current, total)
                    try:
                        _send_json_message(
                            conn,
                            {
                                "type": "peer_progress",
                                "status": status,
                                "label": label,
                                "current": int(current),
                                "total": int(total),
                                "peer_name": self.peer_name,
                            },
                        )
                    except Exception:
                        pass

                if message_type == "collect_chunk":
                    pairs = payload.get("users") if isinstance(payload.get("users"), list) else []
                    recursions = int(payload.get("recursions", 1))
                    self._log(f"Peer task: collect friend graph chunk ({len(pairs)} users).")
                    send_progress("Peer Task - Collecting graph", 0, max(1, len(pairs)), status="Working")
                    result = _collect_friend_graph_chunk(
                        user_depth_pairs=pairs,
                        recursions=recursions,
                        timeout_seconds=timeout_seconds,
                        rate_limit_wait_seconds=rate_limit_wait_seconds,
                        request_delay_seconds=float(
                            settings.get(
                                "friend_collection_min_interval_seconds",
                                DEFAULT_SETTINGS["friend_collection_min_interval_seconds"],
                            )
                        ),
                        should_stop=lambda: self._stop_event.is_set() or self._abort_task_event.is_set(),
                        on_progress=lambda current, total: send_progress(
                            "Peer Task - Collecting graph", current, total, status="Working"
                        ),
                    )
                    send_progress("Peer Task - Collecting graph", len(pairs), max(1, len(pairs)), status="Idle")
                    _send_json_message(
                        conn,
                        {
                            "type": "collect_chunk_result",
                            "status": "ok",
                            "chunk_id": payload.get("chunk_id"),
                            "friends": result.get("friends", []),
                            "processed_users": int(result.get("processed_users", 0)),
                        },
                    )
                    return

                if message_type == "resolve_chunk":
                    ids = payload.get("friend_ids") if isinstance(payload.get("friend_ids"), list) else []
                    self._log(f"Peer task: resolve usernames ({len(ids)} users).")
                    send_progress("Peer Task - Resolving usernames", 0, max(1, len(ids)), status="Working")
                    result = _resolve_usernames_chunk(
                        friend_ids=ids,
                        timeout_seconds=timeout_seconds,
                        rate_limit_wait_seconds=rate_limit_wait_seconds,
                        request_delay_seconds=request_delay_seconds,
                        should_stop=lambda: self._stop_event.is_set() or self._abort_task_event.is_set(),
                        on_progress=lambda current, total: send_progress(
                            "Peer Task - Resolving usernames", current, total, status="Working"
                        ),
                    )
                    send_progress("Peer Task - Resolving usernames", len(ids), max(1, len(ids)), status="Idle")
                    _send_json_message(
                        conn,
                        {
                            "type": "resolve_chunk_result",
                            "status": "ok",
                            "chunk_id": payload.get("chunk_id"),
                            "resolved": result.get("resolved", []),
                            "processed_users": int(result.get("processed_users", 0)),
                        },
                    )
                    return

                friends = payload.get("friends") if isinstance(payload.get("friends"), list) else []
                blacklist = set(str(x).strip() for x in payload.get("blacklist", []) if str(x).strip())
                whitelist = set(str(x).strip() for x in payload.get("whitelist", []) if str(x).strip())
                self._log(f"Peer task: scan friend groups ({len(friends)} users).")
                send_progress("Peer Task - Scanning groups", 0, max(1, len(friends)), status="Working")
                result = _scan_friend_groups_chunk(
                    friend_infos=friends,
                    blacklist=blacklist,
                    whitelist=whitelist,
                    report_non_blacklisted=bool(payload.get("report_non_blacklisted", False)),
                    timeout_seconds=timeout_seconds,
                    rate_limit_wait_seconds=rate_limit_wait_seconds,
                    request_delay_seconds=request_delay_seconds,
                    should_stop=lambda: self._stop_event.is_set() or self._abort_task_event.is_set(),
                    on_progress=lambda current, total: send_progress(
                        "Peer Task - Scanning groups", current, total, status="Working"
                    ),
                )
                send_progress("Peer Task - Scanning groups", len(friends), max(1, len(friends)), status="Idle")
                _send_json_message(
                    conn,
                    {
                        "type": "scan_chunk_result",
                        "status": "ok",
                        "chunk_id": payload.get("chunk_id"),
                        "entries": result.get("entries", []),
                        "report_hits": int(result.get("report_hits", 0)),
                        "caught_friend_ids": result.get("caught_friend_ids", []),
                    },
                )
            except Exception as exc:  # noqa: BLE001
                _send_json_message(
                    conn,
                    {
                        "type": "scan_chunk_result",
                        "status": "error",
                        "error": str(exc),
                    },
                )


class RawSubordinateApp(tk.Tk):
    def __init__(self):
        super().__init__()
        self._setup_theme()
        self.title("Raw Subordinate - Erin's Roblox Blacklist Clocker")
        self.geometry("900x620")
        self.minsize(820, 560)

        self.event_queue = queue.Queue()
        self.peer_worker_server = None
        self.tray_icon = None
        self.tray_visible = False
        self.tray_image = None

        preset, loaded_preset = _load_preset_settings()
        self.peer_name_var = tk.StringVar(value=str(preset.get("peer_name", "Unnamed Peer")))
        self.listen_host_var = tk.StringVar(value=str(preset.get("listen_host", "0.0.0.0")))
        self.listen_port_var = tk.StringVar(value=str(preset.get("listen_port", 47931)))

        self.idle_image_var = tk.StringVar(value=str(preset.get("idle_image", "")))
        self.timeout_image_var = tk.StringVar(value=str(preset.get("timeout_image", "")))
        self.working_image_var = tk.StringVar(value=str(preset.get("working_image", "")))
        self.offline_image_var = tk.StringVar(value=str(preset.get("offline_image", "")))

        self.status_var = tk.StringVar(value="Idle")
        self.progress_var = tk.DoubleVar(value=0)
        self.progress_max = 1

        self._build_ui()
        self.after(80, self._process_events)
        self.protocol("WM_DELETE_WINDOW", self.destroy)
        if loaded_preset:
            self.after(200, self._on_start_peer_worker)

    def _setup_theme(self):
        self.configure(bg="black")
        default_font = tkfont.nametofont("TkDefaultFont")
        default_font.configure(family="NSimSun", size=11)
        text_font = tkfont.nametofont("TkTextFont")
        text_font.configure(family="NSimSun", size=11)
        style = ttk.Style(self)
        style.theme_use("clam")
        style.configure(".", background="black", foreground="white", fieldbackground="black", font=("NSimSun", 11))
        style.configure("TFrame", background="black")
        style.configure("TLabel", background="black", foreground="white")
        style.configure("TCheckbutton", background="black", foreground="white")
        style.configure("TLabelframe", background="black", foreground="white")
        style.configure("TLabelframe.Label", background="black", foreground="white")
        style.configure("TEntry", fieldbackground="black", foreground="white")
        style.configure("TButton", background="black", foreground="white", bordercolor="white")
        style.map("TButton", background=[("active", "#111111")], foreground=[("active", "white")])
        style.configure(
            "Red.Horizontal.TProgressbar",
            troughcolor="black",
            background="#ff0000",
            bordercolor="white",
            lightcolor="#ff0000",
            darkcolor="#ff0000",
        )

    def _build_ui(self):
        root = ttk.Frame(self)
        root.pack(fill="both", expand=True, padx=10, pady=10)
        root.columnconfigure(0, weight=1)
        root.rowconfigure(2, weight=1)

        top_frame = ttk.LabelFrame(root, text="Peer Listener")
        top_frame.grid(row=0, column=0, sticky="ew", padx=4, pady=(4, 8))
        top_frame.columnconfigure(1, weight=1)

        ttk.Label(top_frame, text="Peer Name:").grid(row=0, column=0, sticky="w", padx=8, pady=6)
        ttk.Entry(top_frame, textvariable=self.peer_name_var, width=24).grid(
            row=0, column=1, sticky="w", padx=(0, 8), pady=6
        )

        ttk.Label(top_frame, text="Listen Host:").grid(row=1, column=0, sticky="w", padx=8, pady=6)
        ttk.Entry(top_frame, textvariable=self.listen_host_var, width=20).grid(
            row=1, column=1, sticky="w", padx=(0, 8), pady=6
        )
        ttk.Label(top_frame, text="Listen Port:").grid(row=1, column=2, sticky="e", padx=(6, 4), pady=6)
        ttk.Entry(top_frame, textvariable=self.listen_port_var, width=12).grid(
            row=1, column=3, sticky="w", padx=(0, 8), pady=6
        )

        buttons = ttk.Frame(top_frame)
        buttons.grid(row=2, column=0, columnspan=4, sticky="w", padx=8, pady=(6, 8))
        self.start_button = ttk.Button(buttons, text="Start Peer Worker", command=self._on_start_peer_worker)
        self.start_button.grid(row=0, column=0, padx=(0, 8))
        self.stop_button = ttk.Button(buttons, text="Stop Peer Worker", command=self._on_stop_peer_worker)
        self.stop_button.grid(row=0, column=1, padx=(0, 8))
        self.reload_button = ttk.Button(buttons, text="Reload Images", command=self._on_reload_images)
        self.reload_button.grid(row=0, column=2)
        self.save_preset_button = ttk.Button(buttons, text="Save Preset", command=self._on_save_preset)
        self.save_preset_button.grid(row=0, column=3, padx=(8, 0))
        self._refresh_buttons()

        images_frame = ttk.LabelFrame(root, text="Peer Images (optional)")
        images_frame.grid(row=1, column=0, sticky="ew", padx=4, pady=(0, 8))
        images_frame.columnconfigure(1, weight=1)
        images_frame.columnconfigure(3, weight=1)

        self._add_image_picker(images_frame, 0, "Idle", self.idle_image_var, "idle")
        self._add_image_picker(images_frame, 1, "Timed Out", self.timeout_image_var, "timeout")
        self._add_image_picker(images_frame, 2, "Working", self.working_image_var, "working")
        self._add_image_picker(images_frame, 3, "Offline", self.offline_image_var, "offline")

        status_frame = ttk.LabelFrame(root, text="Status")
        status_frame.grid(row=2, column=0, sticky="nsew", padx=4, pady=(0, 4))
        status_frame.columnconfigure(0, weight=1)
        status_frame.rowconfigure(2, weight=1)

        ttk.Label(status_frame, textvariable=self.status_var).grid(row=0, column=0, sticky="w", padx=8, pady=(8, 4))
        self.progress = ttk.Progressbar(
            status_frame,
            mode="determinate",
            maximum=1,
            value=0,
            style="Red.Horizontal.TProgressbar",
        )
        self.progress.grid(row=1, column=0, sticky="ew", padx=8, pady=(0, 8))

        self.log = ScrolledText(status_frame, height=12, wrap="word")
        self.log.grid(row=2, column=0, sticky="nsew", padx=8, pady=(0, 8))
        self.log.configure(
            state="disabled",
            bg="black",
            fg="white",
            insertbackground="white",
            selectbackground="#222222",
            selectforeground="white",
            font=("NSimSun", 11),
        )
        self.tray_button = ttk.Button(
            status_frame,
            text="Hide To Tray",
            command=self._on_hide_to_tray_clicked,
        )
        self.tray_button.grid(row=3, column=0, sticky="w", padx=8, pady=(0, 8))
        if not _PYSTRAY_AVAILABLE:
            self.tray_button.configure(state="disabled")

    def _add_image_picker(self, parent, row, label, var, kind):
        ttk.Label(parent, text=f"{label} Image:").grid(row=row, column=0, sticky="w", padx=8, pady=4)
        ttk.Entry(parent, textvariable=var, width=32).grid(row=row, column=1, sticky="ew", padx=(0, 6), pady=4)
        ttk.Button(parent, text="Browse", command=lambda: self._browse_image(kind)).grid(
            row=row, column=2, sticky="w", padx=(0, 8), pady=4
        )

    def _browse_image(self, kind):
        file_path = filedialog.askopenfilename(
            title="Select peer status image",
            filetypes=[("Images", "*.png;*.jpg;*.jpeg;*.gif"), ("All Files", "*.*")],
        )
        if not file_path:
            return
        kind = str(kind or "").lower()
        if kind == "idle":
            self.idle_image_var.set(file_path)
        elif kind == "timeout":
            self.timeout_image_var.set(file_path)
        elif kind == "working":
            self.working_image_var.set(file_path)
        elif kind == "offline":
            self.offline_image_var.set(file_path)

    def _queue_event(self, event):
        self.event_queue.put(event)

    def _queue_log(self, message):
        self._queue_event(("log", message))

    def _queue_progress(self, label, current, total):
        self._queue_event(("progress", label, current, total))

    def _process_events(self):
        while True:
            try:
                event = self.event_queue.get_nowait()
            except queue.Empty:
                break
            kind = event[0]
            if kind == "log":
                self._append_log(event[1])
            elif kind == "progress":
                _, label, current, total = event
                total = max(int(total), 1)
                current = max(0, min(int(current), total))
                self.progress.configure(maximum=total, value=current)
                percent = (current / total) * 100.0
                self.status_var.set(f"{label}: {current}/{total} ({percent:.1f}%)")
        self.after(80, self._process_events)

    def _append_log(self, message):
        self.log.configure(state="normal")
        self.log.insert("end", message + "\n")
        self.log.see("end")
        self.log.configure(state="disabled")

    def _build_peer_image_payload(self, path):
        if not path:
            return None
        try:
            with open(path, "rb") as img_file:
                raw = img_file.read()
        except OSError:
            return None
        if not raw:
            return None
        ext = os.path.splitext(path)[1].lower().lstrip(".")
        if ext == "jpeg":
            ext = "jpg"
        return {
            "data": base64.b64encode(raw).decode("ascii"),
            "format": ext,
        }

    def _apply_peer_profile_to_server(self):
        if not self.peer_worker_server:
            return
        peer_name = self.peer_name_var.get().strip() or "Unnamed Peer"
        images = {}

        def pick_image(user_value, key):
            if user_value and os.path.exists(user_value):
                return user_value
            default_path = DEFAULT_AVATAR_FILES.get(key)
            return default_path if default_path and os.path.exists(default_path) else ""

        idle_path = pick_image(self.idle_image_var.get().strip(), "idle")
        timeout_path = pick_image(self.timeout_image_var.get().strip(), "timeout")
        working_path = pick_image(self.working_image_var.get().strip(), "working")
        offline_path = pick_image(self.offline_image_var.get().strip(), "offline")

        for key, path in (
            ("idle", idle_path),
            ("timeout", timeout_path),
            ("working", working_path),
            ("offline", offline_path),
        ):
            payload = self._build_peer_image_payload(path)
            if payload:
                images[key] = payload

        self.peer_worker_server.peer_name = peer_name
        self.peer_worker_server.peer_images = images

    def _refresh_buttons(self):
        running = bool(self.peer_worker_server and self.peer_worker_server.is_running())
        if running:
            self.start_button.configure(state="disabled")
            self.stop_button.configure(state="normal")
            self.reload_button.configure(state="normal")
        else:
            self.start_button.configure(state="normal")
            self.stop_button.configure(state="disabled")
            self.reload_button.configure(state="disabled")

    def _on_start_peer_worker(self):
        try:
            if self.peer_worker_server and self.peer_worker_server.is_running():
                self._queue_log("Peer worker is already running.")
                return
            host = self.listen_host_var.get().strip() or "0.0.0.0"
            port_text = self.listen_port_var.get().strip()
            try:
                port = int(port_text)
            except ValueError as exc:
                raise ValueError("Listen port must be a whole number.") from exc
            if port < 1 or port > 65535:
                raise ValueError("Listen port must be between 1 and 65535.")
            self.peer_worker_server = PeerWorkerServer(
                host=host,
                port=port,
                on_log=self._queue_log,
                on_progress=self._queue_progress,
            )
            self._apply_peer_profile_to_server()
            self.peer_worker_server.start()
            self._queue_log(f"Peer worker listening on {host}:{port}")
            self._refresh_buttons()
        except ValueError as exc:
            messagebox.showerror("Peer Worker", str(exc))
        except Exception as exc:  # noqa: BLE001
            messagebox.showerror("Peer Worker", str(exc))

    def _on_stop_peer_worker(self):
        if self.peer_worker_server and self.peer_worker_server.is_running():
            self.peer_worker_server.stop()
            self._queue_log("Peer worker stopped.")
            self.status_var.set("Idle")
        else:
            self._queue_log("Peer worker is not running.")
        self._refresh_buttons()

    def _on_reload_images(self):
        if not self.peer_worker_server or not self.peer_worker_server.is_running():
            self._queue_log("Peer worker is not running.")
            return
        self._apply_peer_profile_to_server()
        self._queue_log("Peer images reloaded.")

    def _on_save_preset(self):
        try:
            port_value = int(str(self.listen_port_var.get()).strip())
        except ValueError as exc:
            messagebox.showerror("Save Preset", "Listen port must be a whole number.")
            return
        if port_value < 1 or port_value > 65535:
            messagebox.showerror("Save Preset", "Listen port must be between 1 and 65535.")
            return
        payload = {
            "peer_name": str(self.peer_name_var.get()).strip() or "Unnamed Peer",
            "listen_host": str(self.listen_host_var.get()).strip() or "0.0.0.0",
            "listen_port": port_value,
            "idle_image": str(self.idle_image_var.get()).strip(),
            "timeout_image": str(self.timeout_image_var.get()).strip(),
            "working_image": str(self.working_image_var.get()).strip(),
            "offline_image": str(self.offline_image_var.get()).strip(),
        }
        try:
            _save_preset_settings(payload)
        except OSError as exc:
            messagebox.showerror("Save Preset", str(exc))
            return
        self._queue_log(f"Preset saved to {PRESET_SETTINGS_FILE}.")

    def _on_hide_to_tray_clicked(self):
        if not _PYSTRAY_AVAILABLE:
            messagebox.showerror(
                "Tray Support Missing",
                "Install 'pystray' and 'Pillow' to enable tray hiding.",
            )
            return
        self._hide_to_tray()

    def _load_tray_image(self):
        if self.tray_image is not None:
            return self.tray_image
        if Image is None:
            return None
        for path in (
            os.path.join(BASE_DIR, "assets", "tray_icon.png"),
            os.path.join(BASE_DIR, "assets", "tray_icon.ico"),
            os.path.join(BASE_DIR, "assets", "avatars", "idle.png"),
            os.path.join(BASE_DIR, "assets", "avatars", "idle.gif"),
        ):
            if os.path.exists(path):
                try:
                    self.tray_image = Image.open(path)
                    return self.tray_image
                except Exception:
                    continue
        try:
            self.tray_image = Image.new("RGB", (64, 64), color="black")
        except Exception:
            self.tray_image = None
        return self.tray_image

    def _show_from_tray(self, _icon=None, _item=None):
        self.tray_visible = False
        try:
            if self.tray_icon:
                self.tray_icon.stop()
        except Exception:
            pass
        self.tray_icon = None
        self.after(0, self.deiconify)

    def _hide_to_tray(self):
        if self.tray_visible:
            return
        image = self._load_tray_image()
        if image is None:
            messagebox.showerror("Tray Support Missing", "Tray icon image could not be loaded.")
            return
        menu = pystray.Menu(
            pystray.MenuItem("Show", self._show_from_tray, default=True),
            pystray.MenuItem("Exit", lambda *_: self.destroy()),
        )
        self.tray_icon = pystray.Icon("raw_subordinate", image, "Raw Subordinate", menu)
        self.tray_visible = True
        self.withdraw()
        try:
            self.tray_icon.run_detached()
        except Exception:
            self.tray_visible = False
            self.deiconify()
            self.tray_icon = None
            messagebox.showerror("Tray Error", "Unable to create tray icon.")

    def destroy(self):
        try:
            if self.tray_icon:
                try:
                    self.tray_icon.stop()
                except Exception:
                    pass
            if self.peer_worker_server and self.peer_worker_server.is_running():
                self.peer_worker_server.stop()
        finally:
            super().destroy()


def run_gui():
    app = RawSubordinateApp()
    app.mainloop()


if __name__ == "__main__":
    run_gui()
