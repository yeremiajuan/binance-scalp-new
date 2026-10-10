"""Threaded I/O around the deterministic ``paperbot.forward.ForwardSession`` (the single state owner).

Threads only *deliver* timestamped items to one queue; the owner thread (the caller of ``run_forward``) is the
only one that touches the engine or writes the database:

* ``StreamFeed`` (WebSocket frames and connection status),
* ``RestWorker`` (warm-up/backfill klines, metadata, references, server time; rate-limit aware),
* ``ControlServer`` (``control.py``: authenticated local socket/named pipe: kill/reset/stop/ping),
* ``Notifier`` (optional Telegram; reads the outbox read-only and reports delivery results back).

Items are stamped with a monotonic-derived UTC clock when they are queued, so receipt times never go backwards.
"""

from __future__ import annotations

import json
import os
import queue
import signal
import threading
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

from paperbot.config import Config, load_config
from paperbot.forward import ForwardSession, Item
from paperbot.recorded import code_revision, manifest_items, open_forward
from paperbot.storage import ProfileLock, StateLock
from paperbot.timeutil import US_PER_S

from .control import ControlServer
from .rest import PublicRest, RateLimited, RestError, Throttled, UrllibTransport
from .ws import StreamFeed, check_websockets, stream_url


def wall_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


class MonotonicClock:
    """UTC microseconds anchored at start and advanced by the monotonic clock (immune to wall-clock steps)."""

    def __init__(self, floor_us: int = 0):
        self.anchor_wall = time.time_ns() // 1000
        self.anchor_mono = time.monotonic_ns() // 1000
        self.offset = max(0, floor_us + 1 - self.anchor_wall)

    def now_us(self) -> int:
        return self.anchor_wall + self.offset + (time.monotonic_ns() // 1000 - self.anchor_mono)


class StampedQueue:
    def __init__(self, clock):
        self.clock = clock
        self.q: queue.Queue = queue.Queue()
        self.lock = threading.Lock()
        self.last = 0

    def put(self, kind: str, data: dict) -> None:
        with self.lock:
            now = max(self.clock.now_us(), self.last)
            self.last = now
            self.q.put(Item(kind, now, data))

    def now(self) -> int:
        """A timestamp ordered with every queued item (no later put can be stamped earlier)."""
        with self.lock:
            self.last = max(self.clock.now_us(), self.last)
            return self.last

    def tick_time(self) -> int | None:
        """A tick timestamp, or None while items are still queued: a tick must never be stamped later than an
        observation that is waiting to be processed (that observation would then be rejected as out of order)."""
        with self.lock:
            if not self.q.empty():
                return None
            self.last = max(self.clock.now_us(), self.last)
            return self.last

    def get(self, timeout: float) -> Item | None:
        try:
            return self.q.get(timeout=timeout)
        except queue.Empty:
            return None


class RawRecorder:
    """Append-only raw observations (every WS frame, REST response and connection status), gzip-compressed.

    Audit/provenance evidence: flushed and fsynced about once per second, so a crash can lose up to ~1 s of raw
    lines. The normalized inputs that drive decisions are committed transactionally in SQLite with each decision.
    """

    def __init__(self, directory: str, session_id: str, max_bytes: int = 64 * 1024 * 1024):
        self.dir = Path(directory) / session_id
        self.dir.mkdir(parents=True, exist_ok=True)
        self.max_bytes = max_bytes
        self.part = 0
        self.raw = None
        self.gz = None
        self.last_sync = time.monotonic()
        self._open()

    def _open(self) -> None:
        import gzip

        self.part += 1
        self.raw = open(self.dir / f"raw-{self.part:04d}.jsonl.gz", "ab")
        self.gz = gzip.GzipFile(fileobj=self.raw, mode="ab")

    def _sync(self) -> None:
        import zlib

        self.gz.flush(zlib.Z_SYNC_FLUSH)
        self.raw.flush()
        os.fsync(self.raw.fileno())
        self.last_sync = time.monotonic()

    def __call__(self, record: dict) -> None:
        self.gz.write((json.dumps(record, sort_keys=True, default=str) + "\n").encode())
        if time.monotonic() - self.last_sync > 1.0:
            self._sync()
            if self.raw.tell() > self.max_bytes:
                self.gz.close()
                self.raw.close()
                self._open()

    def close(self) -> None:
        if self.gz is not None:
            self._sync()
            self.gz.close()
            self.raw.close()
            self.gz = self.raw = None


class RestWorker(threading.Thread):
    """Performs REST tasks requested by the owner plus periodic refreshes; results go back through the queue."""

    def __init__(self, rest: PublicRest, out: StampedQueue, cfg: Config, clock, stop_event: threading.Event):
        super().__init__(name="paperbot-rest", daemon=True)
        self.rest = rest
        self.out = out
        self.fwd = cfg.forward
        self.clock = clock
        self.stop_event = stop_event
        self.tasks: queue.Queue = queue.Queue()
        self.next_due = {"metadata": time.monotonic() + self.fwd.metadata_refresh_s,
                         "time": time.monotonic() + self.fwd.clock_check_s,
                         "references": time.monotonic() + self.fwd.reference_refresh_s}

    def request(self, name: str, params: dict) -> None:
        self.tasks.put((name, dict(params)))

    def _call(self, name: str, params: dict) -> None:
        purpose = params.pop("purpose", None)
        sent = self.clock.now_us()
        data: dict = {"name": name, "params": {**params, **({"purpose": purpose} if purpose else {})},
                      "sent_us": sent, "host": self.rest.host}
        try:
            if name == "metadata":
                info = self.rest.get("exchangeInfo", symbol="BTCUSDT")
                self.rest.set_weight_limit(info)
                try:
                    rules = self.rest.get("executionRules", symbol="BTCUSDT")
                except RestError as exc:
                    rules = None
                    data["execution_rules_error"] = str(exc)
                data.update(ok=True, parsed={"exchangeInfo": info, "executionRules": rules},
                            fetched_us=self.clock.now_us())
            else:
                data.update(ok=True, parsed=self.rest.get(name, **params))
        except Throttled as exc:
            self.stop_event.wait(exc.wait_s)
            self.tasks.put((name, {**params, **({"purpose": purpose} if purpose else {})}))
            return
        except RateLimited as exc:
            data.update(ok=False, error=str(exc), rate_limited=True, banned=exc.banned)
            self.out.put("rest", data)
            self.stop_event.wait(min(exc.retry_after_s, 3600))
            self.tasks.put((name, {**params, **({"purpose": purpose} if purpose else {})}))
            return
        except RestError as exc:
            data.update(ok=False, error=str(exc))
        calls, self.rest.calls = self.rest.calls, []
        data["calls"] = [c.__dict__ for c in calls]
        self.out.put("rest", data)

    def run(self) -> None:
        while not self.stop_event.is_set():
            try:
                name, params = self.tasks.get(timeout=0.5)
                self._call(name, params)
                continue
            except queue.Empty:
                pass
            now = time.monotonic()
            if now >= self.next_due["metadata"]:
                self.next_due["metadata"] = now + self.fwd.metadata_refresh_s
                self.request("metadata", {})
            if now >= self.next_due["time"]:
                self.next_due["time"] = now + self.fwd.clock_check_s
                self.request("time", {})
            if now >= self.next_due["references"]:
                self.next_due["references"] = now + self.fwd.reference_refresh_s
                self.request("avgPrice", {"symbol": "BTCUSDT"})
                self.request("referencePrice", {"symbol": "BTCUSDT"})


BACKFILL_CHUNK = 20  # bars committed per chunk while applying a warm-up/backfill result
INPUTS_PER_CHUNK = 20  # queued inputs go first; a chunk still runs after this many, so a busy stream cannot starve it


def owner_turn(session: ForwardSession, inbox: StampedQueue, clock, *, store, notifier, tick_s: float,
               since_chunk: int) -> int:
    """One iteration of the owner loop: at most one queued input, a tick when the queue is empty, and at most one
    backfill chunk. Returns the number of inputs processed since the last chunk."""
    # never sleep while a backfill result is still being applied
    item = inbox.get(timeout=0 if session.pending_work() else tick_s)
    if item is not None:
        lag = clock.now_us() - item.recv_us  # wall time this input waited for the owner
        if item.kind == "control":
            reply_q = item.data.pop("_reply")
            reply_q.put(session.control({**item.data, "wall_utc": wall_iso()}, item.recv_us))
        elif item.kind == "outbox_result":
            d = item.data
            store.outbox_update(d["msg_id"], d["status"], d["attempts"], wall_iso(), d.get("error"))
        else:
            session.handle(item)
        session.observe_lag(lag, max(item.recv_us, session.engine.state.clock_us or 0))
        since_chunk += 1
    now = inbox.tick_time()
    if now is not None:
        session.observe_lag(0, now)  # the queue is empty: nothing is waiting
        session.tick(now)
        if notifier is not None:
            notifier.after_tick(session, now)
    if session.pending_work() and (inbox.q.empty() or since_chunk >= INPUTS_PER_CHUNK):
        session.work()  # one bounded chunk, then back to queued inputs and ticks
        since_chunk = 0
    return since_chunk


def run_forward(config_path: str, state_path: str, *, run_seconds: float | None = None, transport=None,
                ws_url: str | None = None, ws_connect=None, notifier_factory=None, install_signals: bool = True,
                log=print) -> int:
    """Run the PAPER forward runner until stopped. Returns a process exit code (0 = graceful stop)."""
    cfg = load_config(config_path)
    if cfg.forward is None:
        raise ValueError("configuration has no [forward] section")
    fwd = cfg.forward
    if cfg.telegram is not None and cfg.telegram.enabled and notifier_factory is not None \
            and not os.environ.get(cfg.telegram.env_var):
        raise ValueError(f"telegram.enabled but environment variable {cfg.telegram.env_var} is not set")
    if ws_connect is None:
        check_websockets()  # the real connector: fail before taking locks, not at the first connection attempt
    stop_event = threading.Event()
    with StateLock(state_path) as lock, ProfileLock(fwd.profile_lock_dir, cfg.account_id):
        mocked = transport is not None or ws_url is not None or ws_connect is not None
        engine, restart = open_forward(lock, cfg, provenance="MOCKED" if mocked else "BINANCE_PUBLIC")
        store = engine.store
        session_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "-" + uuid.uuid4().hex[:6]
        clock = MonotonicClock(floor_us=engine.state.clock_us or 0)
        inbox = StampedQueue(clock)
        rest = PublicRest(fwd.rest_host, transport or UrllibTransport(fwd.rest_timeout_s),
                          budget_fraction=float(fwd.rest_weight_fraction))
        worker = RestWorker(rest, inbox, cfg, clock, stop_event)
        recorder = RawRecorder(os.path.join(fwd.recordings_dir, cfg.account_id), session_id)
        store.put_manifest({**manifest_items(cfg), "recordings_dir": fwd.recordings_dir})
        store.start_session(session_id, "restart" if restart else "start", wall_iso(), engine.state.cursor,
                            code_revision())
        session = ForwardSession(engine, session_id=session_id, restart=restart, raw_sink=recorder,
                                 rest_request=worker.request, backfill_chunk=BACKFILL_CHUNK)
        feed = StreamFeed(ws_url or stream_url(fwd.ws_host), lambda k, d: inbox.put(k, d),
                          stop_event=stop_event, initial_backoff_s=fwd.reconnect_initial_ms / 1000,
                          max_backoff_s=fwd.reconnect_max_ms / 1000, silence_s=fwd.ws_silence_s,
                          **({"connect": ws_connect} if ws_connect else {}))
        control = ControlServer(lock.canonical, inbox, stop_event)
        notifier = notifier_factory(cfg, lock.canonical, inbox, stop_event) if notifier_factory else None
        stop_reason = {"why": None}

        def on_signal(signum, frame):  # noqa: ARG001
            stop_reason["why"] = f"signal {signal.Signals(signum).name}"
            stop_event.set()

        if install_signals:
            # Ctrl+C (SIGINT) everywhere; SIGTERM on Linux/macOS; Ctrl+Break (SIGBREAK) on Windows. `paperbot stop`
            # works on every platform through the control channel. Closing a Windows console window or killing the
            # process is a crash: the OS releases both locks and the next start reconciles and recovers.
            for name in ("SIGINT", "SIGTERM", "SIGBREAK"):
                sig = getattr(signal, name, None)
                if sig is not None:
                    signal.signal(sig, on_signal)
        code = 0
        deadline = None if run_seconds is None else time.monotonic() + run_seconds
        try:
            session.start(inbox.now())
            for t in (worker, feed, control) + ((notifier,) if notifier else ()):
                t.start()
            log(f"PAPER | PUBLIC DATA | forward runner {session_id} ({'restart' if restart else 'start'}) owns "
                f"{lock.canonical}; control endpoint {control.path}")
            tick_s = min(fwd.heartbeat_ms, fwd.quote_sample_ms) / 1000 / 2
            since_chunk = 0
            while not stop_event.is_set() and not session.stopped:
                if deadline is not None and time.monotonic() >= deadline:
                    stop_reason["why"] = f"run_seconds={run_seconds}"
                    break
                since_chunk = owner_turn(session, inbox, clock, store=store, notifier=notifier, tick_s=tick_s,
                                         since_chunk=since_chunk)
            why = stop_reason["why"] or session.stop_reason or "stopped"
            discarded = inbox.q.qsize()  # observations queued after the stop decision are not processed
            session.stop(inbox.now(), why, discarded_inputs=discarded)
        except Exception as exc:  # noqa: BLE001 - halt safely; the last commit stands
            code = 5
            why = f"halted: {type(exc).__name__}: {exc}"
            log(f"PAPER | PUBLIC DATA | runner halted: {why}")
        finally:
            stop_event.set()
            feed.stop()
            control.close()
            for t in (worker, feed, control) + ((notifier,) if notifier else ()):
                if t.is_alive():
                    t.join(timeout=5)
            recorder.close()
            try:
                store.stop_session(session_id, wall_iso(), engine.state.cursor, why)
            except Exception:  # noqa: BLE001 - a failed bookkeeping write must not mask the halt
                pass
            store.close()
        log(f"PAPER | PUBLIC DATA | forward runner {session_id} stopped ({why}); cursor {engine.state.cursor}")
        return code


def default_seconds(us: int) -> float:
    return us / US_PER_S
