"""SQLite persistence, the canonical state-path lock, and atomic commits.

* Local-disk SQLite only. Network filesystems are refused (best-effort check
  on Linux via /proc/self/mountinfo).
* Rollback journal (``journal_mode=DELETE``) with ``synchronous=FULL`` so the
  database file alone is a consistent artifact, plus foreign keys.
* Monetary values are stored as Decimal TEXT; there is no REAL column.
* One input event == one ``BEGIN IMMEDIATE`` .. ``COMMIT``: input log, cursor,
  decision, reservation, order, fill, ledger deltas, position/risk/health
  transitions and the engine snapshot are written together or not at all.
* The OS lock is ``flock`` on ``<realpath(state)>.lock``, taken before the
  database is opened for writing and held for the process lifetime. Symlink and
  relative-path spellings resolve to the same lock; a hard-linked database is
  refused. The kernel releases the lock when a process dies.
"""

from __future__ import annotations

import json
import os
import sqlite3
from decimal import Decimal
from pathlib import Path

from . import codec
from .engine import EngineState, Recorder, detail_json
from .events import RawEvent
from .money import dtext

SCHEMA_VERSION = "1"
NETWORK_FS = {"nfs", "nfs4", "cifs", "smb3", "smbfs", "fuse.sshfs", "9p", "afs", "ceph", "glusterfs",
              "fuse.glusterfs", "davfs", "fuse.davfs2", "lustre", "gpfs", "fuse.rclone", "fuse.s3fs"}

DDL = """
CREATE TABLE meta(key TEXT PRIMARY KEY, value TEXT NOT NULL);
CREATE TABLE cursor(id INTEGER PRIMARY KEY CHECK (id = 1), seq INTEGER NOT NULL, clock_us INTEGER);
CREATE TABLE engine_state(id INTEGER PRIMARY KEY CHECK (id = 1), json TEXT NOT NULL);
CREATE TABLE balances(asset TEXT PRIMARY KEY CHECK (asset IN ('USDT','BTC')), free TEXT NOT NULL,
    locked TEXT NOT NULL);
CREATE TABLE input_log(seq INTEGER PRIMARY KEY, event_id TEXT NOT NULL, event_type TEXT NOT NULL,
    recv_us INTEGER, disposition TEXT NOT NULL CHECK (disposition IN ('accepted','rejected','duplicate')),
    detail TEXT NOT NULL, committed_us INTEGER);
CREATE TABLE source_events(event_id TEXT PRIMARY KEY, seq INTEGER NOT NULL UNIQUE REFERENCES input_log(seq));
CREATE TABLE candidates(candidate_id TEXT PRIMARY KEY, seq INTEGER NOT NULL REFERENCES input_log(seq),
    bar_start_us INTEGER NOT NULL UNIQUE, bar_end_us INTEGER NOT NULL, candle_recv_us INTEGER NOT NULL,
    decision_us INTEGER NOT NULL, close TEXT NOT NULL, h TEXT, atr TEXT, stop_distance TEXT,
    quote_event_id TEXT, qty TEXT, limit_price TEXT,
    status TEXT NOT NULL CHECK (status IN ('submitted','skipped')), skip_reason TEXT, detail TEXT NOT NULL);
CREATE TABLE positions(position_id TEXT PRIMARY KEY, candidate_id TEXT NOT NULL UNIQUE REFERENCES candidates,
    opened_us INTEGER NOT NULL, entry_price TEXT NOT NULL, entry_qty TEXT NOT NULL, stop_distance TEXT NOT NULL,
    stop_price TEXT NOT NULL, target_price TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('open','closed')), exit_reason TEXT, closed_us INTEGER,
    residual_qty TEXT, close_detail TEXT);
CREATE TABLE orders(order_id TEXT PRIMARY KEY, candidate_id TEXT REFERENCES candidates,
    position_id TEXT REFERENCES positions, purpose TEXT NOT NULL CHECK (purpose IN ('entry','exit')),
    side TEXT NOT NULL CHECK (side IN ('BUY','SELL')), attempt INTEGER NOT NULL, order_type TEXT NOT NULL,
    time_in_force TEXT NOT NULL, qty TEXT NOT NULL, limit_price TEXT NOT NULL, submitted_us INTEGER NOT NULL,
    ready_us INTEGER NOT NULL, submitted_seq INTEGER NOT NULL, signal_us INTEGER, reference_quote_id TEXT NOT NULL,
    reserved_asset TEXT NOT NULL, reserved_amount TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('pending','filled','partial','zero','canceled')),
    outcome_reason TEXT, outcome_quote_id TEXT, filled_qty TEXT NOT NULL, closed_us INTEGER, closed_seq INTEGER,
    ineligible_quotes_seen INTEGER NOT NULL, exit_reason TEXT);
CREATE TABLE fills(fill_id TEXT PRIMARY KEY, order_id TEXT NOT NULL UNIQUE REFERENCES orders,
    quote_event_id TEXT NOT NULL UNIQUE, seq INTEGER NOT NULL REFERENCES input_log(seq),
    side TEXT NOT NULL CHECK (side IN ('BUY','SELL')), qty TEXT NOT NULL, price TEXT NOT NULL,
    gross_notional TEXT NOT NULL, fee_asset TEXT NOT NULL, fee_amount TEXT NOT NULL, fee_usdt TEXT NOT NULL,
    usdt_delta TEXT NOT NULL, btc_delta TEXT NOT NULL, basis_gross TEXT NOT NULL, basis_fee TEXT NOT NULL,
    gross_pnl TEXT, net_pnl TEXT, bid TEXT NOT NULL, ask TEXT NOT NULL, bid_qty TEXT NOT NULL,
    ask_qty TEXT NOT NULL, quote_recv_us INTEGER NOT NULL, quote_exchange_us INTEGER, signal_us INTEGER,
    submitted_us INTEGER NOT NULL, ready_us INTEGER NOT NULL, fill_us INTEGER NOT NULL,
    committed_us INTEGER NOT NULL,
    CHECK (quote_recv_us >= ready_us), CHECK (ready_us >= submitted_us),
    CHECK (quote_exchange_us IS NULL OR quote_exchange_us >= ready_us));
CREATE TABLE ledger(entry_id INTEGER PRIMARY KEY AUTOINCREMENT, seq INTEGER NOT NULL, ts_us INTEGER,
    asset TEXT NOT NULL CHECK (asset IN ('USDT','BTC')), free_delta TEXT NOT NULL, locked_delta TEXT NOT NULL,
    kind TEXT NOT NULL, order_id TEXT REFERENCES orders, fill_id TEXT REFERENCES fills);
CREATE TABLE exit_intents(intent_id TEXT PRIMARY KEY, position_id TEXT NOT NULL UNIQUE REFERENCES positions,
    reason TEXT NOT NULL, created_us INTEGER NOT NULL, status TEXT NOT NULL, attempts INTEGER NOT NULL,
    closed_us INTEGER, detail TEXT);
CREATE TABLE risk_events(id INTEGER PRIMARY KEY AUTOINCREMENT, seq INTEGER, ts_us INTEGER, kind TEXT NOT NULL,
    detail TEXT NOT NULL);
CREATE TABLE health_events(id INTEGER PRIMARY KEY AUTOINCREMENT, seq INTEGER, ts_us INTEGER, kind TEXT NOT NULL,
    detail TEXT NOT NULL);
CREATE TABLE control_events(id INTEGER PRIMARY KEY AUTOINCREMENT, at_cursor INTEGER NOT NULL,
    effective_us INTEGER, wall_utc TEXT NOT NULL, kind TEXT NOT NULL CHECK (kind IN ('kill','reset')),
    latch TEXT NOT NULL, reason TEXT NOT NULL, latches_after TEXT NOT NULL);
CREATE INDEX ledger_seq ON ledger(seq);
"""

FaultHook = "Callable[[str, int], None]"


class StateError(RuntimeError):
    """The state database is missing, incompatible, corrupted or otherwise unusable."""


class StateLocked(StateError):
    pass


# --------------------------------------------------------------------- locking


def canonical_state_path(path: str | os.PathLike) -> str:
    return os.path.realpath(os.path.abspath(os.fspath(path)))


def _check_local_filesystem(directory: str) -> None:
    mountinfo = Path("/proc/self/mountinfo")
    if not mountinfo.exists():
        return  # non-Linux: documented limitation, verify the user's OS before a trial
    best, fstype = "", None
    for line in mountinfo.read_text().splitlines():
        parts = line.split(" - ")
        if len(parts) != 2:
            continue
        left, right = parts[0].split(), parts[1].split()
        mount_point = left[4].replace("\\040", " ")
        if (directory == mount_point or directory.startswith(mount_point.rstrip("/") + "/")) and len(
                mount_point) > len(best):
            best, fstype = mount_point, right[0]
    if fstype in NETWORK_FS or (fstype or "").startswith("nfs"):
        raise StateError(f"state directory {directory} is on a network filesystem ({fstype}); use a local disk")


class StateLock:
    """Exclusive OS-backed lock for one canonical state path, held for the process lifetime."""

    def __init__(self, state_path: str | os.PathLike):
        try:
            import fcntl  # noqa: F401
        except ImportError as exc:  # pragma: no cover - Windows
            raise StateError("process locking currently requires Linux/macOS (fcntl.flock)") from exc
        self.canonical = canonical_state_path(state_path)
        self.lock_path = self.canonical + ".lock"
        self.fd: int | None = None

    def acquire(self) -> StateLock:
        import fcntl

        directory = os.path.dirname(self.canonical)
        if not os.path.isdir(directory):
            raise StateError(f"state directory does not exist: {directory}")
        _check_local_filesystem(directory)
        fd = os.open(self.lock_path, os.O_RDWR | os.O_CREAT, 0o600)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            os.close(fd)
            raise StateLocked(
                f"state {self.canonical} is locked by another process; refusing to mutate it"
            ) from None
        if os.path.exists(self.canonical) and os.stat(self.canonical).st_nlink > 1:
            fcntl.flock(fd, fcntl.LOCK_UN)
            os.close(fd)
            raise StateError(f"state {self.canonical} has multiple hard links; refusing an aliased database")
        os.ftruncate(fd, 0)
        os.write(fd, f"pid={os.getpid()}\n".encode())  # informational only; the flock is the guard
        self.fd = fd
        return self

    def release(self) -> None:
        if self.fd is not None:
            import fcntl

            fcntl.flock(self.fd, fcntl.LOCK_UN)
            os.close(self.fd)
            self.fd = None

    def __enter__(self) -> StateLock:
        return self.acquire()

    def __exit__(self, *exc) -> None:
        self.release()


# --------------------------------------------------------------------- storage


def _sql_value(v: object) -> object:
    if isinstance(v, Decimal):
        return dtext(v)
    if isinstance(v, dict) or isinstance(v, list):
        return detail_json(v) if isinstance(v, dict) else json.dumps(v)
    if isinstance(v, bool):
        return int(v)
    return v


class Storage:
    def __init__(self, conn: sqlite3.Connection, path: str, readonly: bool):
        self.conn = conn
        self.path = path
        self.readonly = readonly
        self.fault_hook = None  # tests/evidence only: callable(point, seq) raising to simulate a crash

    # -- opening

    @staticmethod
    def _connect(path: str, readonly: bool) -> sqlite3.Connection:
        if readonly:
            conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True, isolation_level=None)
        else:
            conn = sqlite3.connect(path, isolation_level=None)
            conn.execute("PRAGMA journal_mode=DELETE")
            conn.execute("PRAGMA synchronous=FULL")
        conn.execute("PRAGMA foreign_keys=ON")
        conn.row_factory = sqlite3.Row
        return conn

    @classmethod
    def create(cls, lock: StateLock, meta: dict[str, str], state: EngineState) -> Storage:
        path = lock.canonical
        if lock.fd is None:
            raise StateError("lock must be held before creating state")
        if os.path.exists(path) and os.path.getsize(path) > 0:
            raise StateError(
                f"refusing to overwrite nonempty state {path}; use 'resume' or choose a new state path"
            )
        conn = cls._connect(path, readonly=False)
        store = cls(conn, path, readonly=False)
        conn.execute("BEGIN IMMEDIATE")
        try:
            for stmt in DDL.strip().split(";\n"):
                if stmt.strip():
                    conn.execute(stmt)
            for k, v in {**meta, "schema_version": SCHEMA_VERSION}.items():
                conn.execute("INSERT INTO meta(key, value) VALUES (?, ?)", (k, v))
            for asset, free, locked in (("USDT", state.balances.usdt_free, state.balances.usdt_locked),
                                        ("BTC", state.balances.btc_free, state.balances.btc_locked)):
                conn.execute("INSERT INTO balances VALUES (?, ?, ?)", (asset, dtext(free), dtext(locked)))
                conn.execute(
                    "INSERT INTO ledger(seq, ts_us, asset, free_delta, locked_delta, kind) "
                    "VALUES (0, NULL, ?, ?, ?, ?)",
                    (asset, dtext(free), dtext(locked), "initial_balance"))
            conn.execute("INSERT INTO cursor VALUES (1, 0, NULL)")
            conn.execute("INSERT INTO engine_state VALUES (1, ?)", (codec.dumps(state),))
            conn.execute("COMMIT")
        except BaseException:
            conn.execute("ROLLBACK")
            conn.close()
            raise
        return store

    @classmethod
    def open_existing(cls, lock: StateLock) -> Storage:
        if lock.fd is None:
            raise StateError("lock must be held before opening state for writing")
        return cls._open(lock.canonical, readonly=False)

    @classmethod
    def open_readonly(cls, path: str | os.PathLike) -> Storage:
        return cls._open(canonical_state_path(path), readonly=True)

    @classmethod
    def _open(cls, path: str, readonly: bool) -> Storage:
        if not os.path.exists(path) or os.path.getsize(path) == 0:
            raise StateError(f"no state database at {path}")
        try:
            conn = cls._connect(path, readonly)
            ok = conn.execute("PRAGMA integrity_check").fetchone()[0]
        except sqlite3.DatabaseError as exc:
            raise StateError(f"state database {path} is unreadable or corrupted: {exc}") from exc
        if ok != "ok":
            conn.close()
            raise StateError(f"state database {path} failed integrity_check: {ok}")
        store = cls(conn, path, readonly)
        try:
            version = store.meta().get("schema_version")
        except sqlite3.DatabaseError as exc:
            raise StateError(f"state database {path} has no readable meta table: {exc}") from exc
        if version != SCHEMA_VERSION:
            conn.close()
            raise StateError(f"incompatible schema version {version!r}; expected {SCHEMA_VERSION!r}")
        return store

    def close(self) -> None:
        self.conn.close()

    # -- reads

    def meta(self) -> dict[str, str]:
        return {r["key"]: r["value"] for r in self.conn.execute("SELECT key, value FROM meta")}

    def load_state(self) -> EngineState:
        row = self.conn.execute("SELECT json FROM engine_state WHERE id = 1").fetchone()
        if row is None:
            raise StateError("engine_state row missing")
        try:
            return codec.loads(EngineState, row["json"])
        except (ValueError, TypeError, KeyError) as exc:
            raise StateError(f"engine snapshot cannot be decoded: {exc}") from exc

    def cursor(self) -> tuple[int, int | None]:
        row = self.conn.execute("SELECT seq, clock_us FROM cursor WHERE id = 1").fetchone()
        return row["seq"], row["clock_us"]

    def source_event_seq(self, event_id: str) -> int | None:
        row = self.conn.execute("SELECT seq FROM source_events WHERE event_id = ?", (event_id,)).fetchone()
        return None if row is None else row["seq"]

    # -- writes

    def _fault(self, point: str, seq: int) -> None:
        if self.fault_hook is not None:
            self.fault_hook(point, seq)

    def _apply_ops(self, rec: Recorder) -> None:
        c = self.conn
        for op in rec.ops:
            if op[0] == "insert":
                _, table, row = op
                cols = list(row)
                c.execute(
                    f"INSERT INTO {table}({', '.join(cols)}) VALUES ({', '.join('?' for _ in cols)})",
                    [_sql_value(row[k]) for k in cols],
                )
            elif op[0] == "update":
                _, table, key, row = op
                cols = [k for k in row if k != key]
                cur = c.execute(
                    f"UPDATE {table} SET {', '.join(f'{k} = ?' for k in cols)} WHERE {key} = ?",
                    [_sql_value(row[k]) for k in cols] + [row[key]],
                )
                if cur.rowcount != 1:
                    raise StateError(f"update of missing {table} row {row[key]!r}")
            else:  # pragma: no cover
                raise AssertionError(op[0])
            self._fault("mid_write", rec.seq or 0)

    def _write_state(self, state: EngineState) -> None:
        c = self.conn
        b = state.balances
        for asset, free, locked in (("USDT", b.usdt_free, b.usdt_locked), ("BTC", b.btc_free, b.btc_locked)):
            c.execute("UPDATE balances SET free = ?, locked = ? WHERE asset = ?", (dtext(free), dtext(locked), asset))
        c.execute("UPDATE engine_state SET json = ? WHERE id = 1", (codec.dumps(state),))
        c.execute("UPDATE cursor SET seq = ?, clock_us = ? WHERE id = 1", (state.cursor, state.clock_us))

    def commit_event(self, raw: RawEvent, disposition: str, detail: dict, state: EngineState, rec: Recorder) -> None:
        if self.readonly:
            raise StateError("read-only storage")
        c = self.conn
        c.execute("BEGIN IMMEDIATE")
        try:
            c.execute(
                "INSERT INTO input_log(seq, event_id, event_type, recv_us, disposition, detail, committed_us)"
                " VALUES (?, ?, ?, ?, ?, ?, ?)",
                (raw.seq, raw.event_id, raw.event_type, raw.recv_us, disposition, detail_json(detail), state.clock_us),
            )
            if rec.source_event_id is not None:
                c.execute("INSERT INTO source_events(event_id, seq) VALUES (?, ?)", (rec.source_event_id, raw.seq))
            self._apply_ops(rec)
            self._write_state(state)
            self._fault("before_commit", raw.seq)
            c.execute("COMMIT")
        except BaseException:
            if c.in_transaction:
                c.execute("ROLLBACK")
            raise
        self._fault("after_commit", raw.seq)

    def commit_control(self, kind: str, latch: str, reason: str, wall_utc: str, state: EngineState) -> None:
        if self.readonly:
            raise StateError("read-only storage")
        c = self.conn
        c.execute("BEGIN IMMEDIATE")
        try:
            c.execute(
                "INSERT INTO control_events(at_cursor, effective_us, wall_utc, kind, latch, reason, latches_after)"
                " VALUES (?, ?, ?, ?, ?, ?, ?)",
                (state.cursor, state.clock_us, wall_utc, kind, latch, reason, json.dumps(state.risk.latches)),
            )
            self._write_state(state)
            self._fault("before_commit", state.cursor)
            c.execute("COMMIT")
        except BaseException:
            if c.in_transaction:
                c.execute("ROLLBACK")
            raise
