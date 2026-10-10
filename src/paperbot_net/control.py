"""Local, authenticated control channel between CLI commands and the running forward-runner owner.

Transport (stdlib ``multiprocessing.connection``):

* Linux/macOS: a Unix-domain socket ``<control dir>/<key>.sock`` (directory 0700, socket 0600).
* Windows: a named pipe ``\\\\.\\pipe\\paperbot-<key>-<random>`` created with ``PIPE_REJECT_REMOTE_CLIENTS`` (local
  clients only) and the default pipe security descriptor (full access for the creator, SYSTEM and Administrators;
  other accounts cannot open it for writing). The random suffix makes the name unguessable, so another process
  cannot create it first.

There is no TCP listener. Every connection must pass ``multiprocessing.connection``'s mutual keyed-digest
challenge (SHA-256; both sides prove knowledge of a 32-byte key generated for each run) before a request is read.
The key and the endpoint address are written to ``<control dir>/<key>.json``; the control dir is per user
(``paperbot.userdirs``). Requests and replies are JSON bytes, never pickles. Only ``cmd``, ``reason`` and ``latch``
are taken from a request; the owner thread executes it, so every mutation still goes through the single state
owner.
"""

from __future__ import annotations

import hashlib
import json
import os
import queue
import secrets
import threading
from multiprocessing import connection as mpc

from paperbot.storage import canonical_state_path, path_key
from paperbot.userdirs import default_control_dir

MAX_MESSAGE = 64 * 1024
MAX_HANDLERS = 4
OWNER_REPLY_S = 15.0
REQUEST_KEYS = ("cmd", "reason", "latch")
CONTROL_ERRORS = (OSError, EOFError, mpc.AuthenticationError, ValueError)

if os.name == "nt":  # pragma: no cover - exercised on Windows only (scripts/verify_windows.py)
    import _winapi

    PIPE_REJECT_REMOTE_CLIENTS = 0x00000008

    class LocalPipeListener(mpc.PipeListener):
        """``multiprocessing``'s named-pipe listener, with remote (SMB) clients rejected by the kernel."""

        def _new_handle(self, first=False):
            flags = _winapi.PIPE_ACCESS_DUPLEX | _winapi.FILE_FLAG_OVERLAPPED
            if first:
                flags |= _winapi.FILE_FLAG_FIRST_PIPE_INSTANCE
            return _winapi.CreateNamedPipe(
                self._address, flags,
                _winapi.PIPE_TYPE_MESSAGE | _winapi.PIPE_READMODE_MESSAGE | _winapi.PIPE_WAIT
                | PIPE_REJECT_REMOTE_CLIENTS,
                _winapi.PIPE_UNLIMITED_INSTANCES, mpc.BUFSIZE, mpc.BUFSIZE, _winapi.NMPWAIT_WAIT_FOREVER,
                _winapi.NULL)


def endpoint_key(state_path: str) -> str:
    return hashlib.sha256(path_key(canonical_state_path(state_path)).encode("utf-8")).hexdigest()[:24]


def control_dir() -> str:
    d = default_control_dir()
    os.makedirs(d, mode=0o700, exist_ok=True)
    if os.name != "nt":
        st = os.stat(d)
        if st.st_uid != os.getuid():
            raise PermissionError(f"control directory {d} is owned by another user")
        if st.st_mode & 0o077:
            os.chmod(d, 0o700)
    return d


def info_path(state_path: str) -> str:
    return os.path.join(control_dir(), endpoint_key(state_path) + ".json")


class ControlServer(threading.Thread):
    """Accepts local control connections; requests are executed by the owner thread through ``out``."""

    def __init__(self, state_canonical: str, out, stop_event: threading.Event):
        super().__init__(name="paperbot-control", daemon=True)
        self.state = state_canonical
        self.out = out
        self.stop_event = stop_event
        self.authkey = secrets.token_bytes(32)
        self.closing = False
        self.handlers: list[threading.Thread] = []
        directory = control_dir()
        key = endpoint_key(state_canonical)
        if os.name == "nt":  # pragma: no cover - Windows only
            self.family = "AF_PIPE"
            self.address = rf"\\.\pipe\paperbot-{key}-{secrets.token_hex(8)}"
            self.listener = LocalPipeListener(self.address)
        else:
            self.family = "AF_UNIX"
            self.address = os.path.join(directory, key + ".sock")
            if len(self.address.encode()) > 100:
                raise OSError(f"control socket path too long for AF_UNIX: {self.address}")
            if os.path.exists(self.address):
                os.unlink(self.address)  # safe: we hold the state lock, so no other owner is listening
            old = os.umask(0o177)
            try:
                self.listener = mpc.Listener(self.address, family="AF_UNIX", backlog=8)
            finally:
                os.umask(old)
            os.chmod(self.address, 0o600)
        self.path = self.address  # shown in the startup log
        self.info_path = os.path.join(directory, key + ".json")
        self._write_info()

    def _write_info(self) -> None:
        info = {"version": 1, "state": self.state, "family": self.family, "address": self.address,
                "authkey": self.authkey.hex(), "pid": os.getpid()}
        tmp = f"{self.info_path}.{os.getpid()}.tmp"
        fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
        with os.fdopen(fd, "w", encoding="utf-8", newline="\n") as fh:
            json.dump(info, fh)
        os.replace(tmp, self.info_path)

    def run(self) -> None:
        while not self.stop_event.is_set() and not self.closing:
            try:
                conn = self.listener.accept()
            except OSError:
                break
            if self.closing or self.stop_event.is_set():
                conn.close()
                break
            self.handlers = [t for t in self.handlers if t.is_alive()]
            if len(self.handlers) >= MAX_HANDLERS:
                conn.close()  # bounded: a client stuck in the handshake cannot exhaust threads
                continue
            t = threading.Thread(target=self._serve, args=(conn,), name="paperbot-control-conn", daemon=True)
            self.handlers.append(t)
            t.start()

    def _serve(self, conn) -> None:
        try:
            mpc.deliver_challenge(conn, self.authkey)  # the client proves it holds the key ...
            mpc.answer_challenge(conn, self.authkey)  # ... and so do we
            if not conn.poll(OWNER_REPLY_S):
                return
            req = json.loads(conn.recv_bytes(MAX_MESSAGE).decode("utf-8"))
            if not isinstance(req, dict):
                raise ValueError("request must be a JSON object")
            reply: queue.Queue = queue.Queue(maxsize=1)
            self.out.put("control", {**{k: str(req[k]) for k in REQUEST_KEYS if k in req}, "_reply": reply})
            try:
                resp = reply.get(timeout=OWNER_REPLY_S)
            except queue.Empty:
                resp = {"ok": False, "error": f"owner did not answer within {OWNER_REPLY_S:.0f} s"}
            conn.send_bytes(json.dumps(resp).encode("utf-8"))
        except mpc.AuthenticationError:
            pass  # wrong or missing key: nothing was read or executed
        except (OSError, EOFError, ValueError) as exc:
            try:
                conn.send_bytes(json.dumps({"ok": False, "error": f"bad request: {exc}"}).encode("utf-8"))
            except (OSError, EOFError, ValueError):
                pass
        finally:
            conn.close()

    def close(self) -> None:
        self.closing = True
        if self.is_alive():
            try:  # wake the blocking accept(); the woken loop sees ``closing`` and exits without serving
                mpc.Client(self.address, family=self.family).close()
            except CONTROL_ERRORS:
                pass
            self.join(timeout=5)
        try:
            self.listener.close()
        except OSError:
            pass
        if self.family == "AF_UNIX" and os.path.exists(self.address):
            os.unlink(self.address)
        try:
            with open(self.info_path, encoding="utf-8") as fh:
                ours = json.load(fh).get("authkey") == self.authkey.hex()
        except (OSError, ValueError):
            ours = False
        if ours:
            os.unlink(self.info_path)


def send_control(state_path: str, request: dict, timeout: float = 20.0) -> dict:
    """Client side (CLI): authenticate to the running owner of ``state_path`` and return its JSON reply."""
    path = info_path(state_path)
    with open(path, encoding="utf-8") as fh:  # FileNotFoundError: no owner has published an endpoint
        info = json.load(fh)
    if path_key(info.get("state", "")) != path_key(canonical_state_path(state_path)):
        raise ValueError(f"control endpoint {path} belongs to another state")
    conn = mpc.Client(info["address"], family=info["family"], authkey=bytes.fromhex(info["authkey"]))
    try:
        conn.send_bytes(json.dumps(request).encode("utf-8"))
        if not conn.poll(timeout):
            raise TimeoutError(f"no reply from the owner within {timeout:.0f} s")
        return json.loads(conn.recv_bytes(MAX_MESSAGE).decode("utf-8"))
    finally:
        conn.close()
