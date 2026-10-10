"""SQLite persistence, the canonical state-path lock, and atomic commits.

* Local-disk SQLite only. Network filesystems are refused (Linux: /proc/self/mountinfo;
  Windows: UNC paths and GetDriveTypeW == DRIVE_REMOTE).
* Rollback journal (``journal_mode=DELETE``) with ``synchronous=FULL`` so the
  database file alone is a consistent artifact, plus foreign keys.
* Monetary values are stored as Decimal TEXT; there is no REAL column.
* One input event == one ``BEGIN IMMEDIATE`` .. ``COMMIT``: input log, cursor,
  decision, reservation, order, fill, ledger deltas, position/risk/health
  transitions and the engine snapshot are written together or not at all.
* The OS lock (``paperbot.oslock``: flock on Linux, LockFileEx on Windows) is on
  ``<realpath(state)>.lock``, taken before the database is opened for writing and
  held for the process lifetime. Symlink, junction, relative, ``..``, short-name
  and (Windows) letter-case spellings resolve to the same lock; a hard-linked
  database is refused. The kernel releases the lock when a process dies.
"""

from __future__ import annotations

import json
import ntpath
import os
import sqlite3
from decimal import Decimal
from pathlib import Path

from . import codec
from .engine import EngineState, Recorder, detail_json
from .events import RawEvent
from .money import dtext
from .oslock import LockUnavailable, OsLock

SCHEMA_VERSION = "2"
# v1 (Phase 1) databases remain readable and resumable for synthetic replay: Phase 2 tables are additive and are
# only written by forward/public-data sessions.
SUPPORTED_SCHEMAS = ("1", "2")
PUBLIC_EVIDENCE = ("PUBLIC", "PUBLIC_RECORDED_REPLAY")
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
CREATE TABLE input_payloads(seq INTEGER PRIMARY KEY REFERENCES input_log(seq), payload TEXT NOT NULL);
CREATE TABLE metadata_versions(sha256 TEXT PRIMARY KEY, fetched_us INTEGER NOT NULL, first_seq INTEGER NOT NULL,
    json TEXT NOT NULL);
CREATE TABLE outbox(msg_id TEXT PRIMARY KEY, seq INTEGER, created_us INTEGER NOT NULL, kind TEXT NOT NULL,
    text TEXT NOT NULL, status TEXT NOT NULL CHECK (status IN ('pending','sent','ambiguous','failed','dropped')),
    attempts INTEGER NOT NULL DEFAULT 0, last_attempt_wall TEXT, last_error TEXT, sent_wall TEXT);
CREATE TABLE sessions(session_id TEXT PRIMARY KEY, kind TEXT NOT NULL, started_wall TEXT NOT NULL,
    start_cursor INTEGER NOT NULL, code_revision TEXT NOT NULL, stopped_wall TEXT, end_cursor INTEGER,
    stop_reason TEXT);
CREATE TABLE manifest(key TEXT PRIMARY KEY, value TEXT NOT NULL);
"""

FaultHook = "Callable[[str, int], None]"


class StateError(RuntimeError):
    """The state database is missing, incompatible, corrupted or otherwise unusable."""


class StateLocked(StateError):
    pass


# --------------------------------------------------------------------- locking


def canonical_state_path(path: str | os.PathLike) -> str:
    """Absolute path with symlinks/junctions and ``..`` resolved (on Windows also 8.3 short names and the on-disk
    letter case of existing components). Every spelling of one database maps to one lock file."""
    return os.path.realpath(os.path.abspath(os.fspath(path)))


def readonly_uri(path: str | os.PathLike) -> str:
    """SQLite read-only URI for a filesystem path: ``file:///C:/...`` on Windows, ``file:///home/...`` on Linux, with
    characters such as ``?``, ``#``, ``%`` and spaces percent-encoded."""
    return Path(os.path.abspath(os.fspath(path))).as_uri() + "?mode=ro"


def path_key(canonical: str) -> str:
    """Comparison key for a canonical path: case-folded on Windows, where paths are case-insensitive."""
    return os.path.normcase(canonical)


def _check_local_filesystem(directory: str) -> None:
    if os.name == "nt":
        _check_local_windows(directory)
        return
    mountinfo = Path("/proc/self/mountinfo")
    if not mountinfo.exists():
        return  # macOS: documented limitation (unverified)
    best, fstype = "", None
    for line in mountinfo.read_text(encoding="utf-8", errors="replace").splitlines():
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


DRIVE_REMOTE = 4  # GetDriveTypeW


def _check_local_windows(directory: str, drive_type=None) -> None:
    """Refuse UNC paths and network (mapped) drives. realpath already turns a mapped drive into its UNC target."""
    plain = directory
    for prefix in ("\\\\?\\", "\\\\.\\"):  # extended-length / device prefixes: look at what follows them
        if plain.startswith(prefix):
            plain = plain[len(prefix):]
            break
    if plain.upper().startswith("UNC\\") or plain.startswith("\\\\"):
        raise StateError(f"state directory {directory} is a network (UNC) path; use a local disk")
    drive, _ = ntpath.splitdrive(plain)
    if drive_type is None:
        import ctypes

        drive_type = ctypes.windll.kernel32.GetDriveTypeW
    kind = drive_type(drive + "\\")
    if kind == DRIVE_REMOTE:
        raise StateError(f"state directory {directory} is on a network drive ({drive}); use a local disk")


class StateLock:
    """Exclusive OS-backed lock for one canonical state path, held for the process lifetime."""

    def __init__(self, state_path: str | os.PathLike):
        self.canonical = canonical_state_path(state_path)
        self.lock_path = self.canonical + ".lock"
        self._os = OsLock(self.lock_path, f"pid={os.getpid()}\n")

    @property
    def held(self) -> bool:
        return self._os.held

    def acquire(self) -> StateLock:
        directory = os.path.dirname(self.canonical)
        if not os.path.isdir(directory):
            raise StateError(f"state directory does not exist: {directory}")
        _check_local_filesystem(directory)
        try:
            got = self._os.try_acquire()
        except LockUnavailable as exc:
            raise StateError(str(exc)) from None
        if not got:
            raise StateLocked(f"state {self.canonical} is locked by another process; refusing to mutate it")
        if os.path.exists(self.canonical) and os.stat(self.canonical).st_nlink > 1:
            self._os.release()
            raise StateError(f"state {self.canonical} has multiple hard links; refusing an aliased database")
        return self

    def release(self) -> None:
        self._os.release()

    def __enter__(self) -> StateLock:
        return self.acquire()

    def __exit__(self, *exc) -> None:
        self.release()


class ProfileLock:
    """Exclusive OS lock for one paper account id (profile), independent of the state path, so two state
    databases cannot run the same paper account at the same time on this machine."""

    def __init__(self, lock_dir: str | os.PathLike, account_id: str):
        import hashlib

        self.account_id = account_id
        self.lock_dir = canonical_state_path(os.path.expanduser(os.fspath(lock_dir)))
        digest = hashlib.sha256(account_id.encode()).hexdigest()[:24]
        self.lock_path = os.path.join(self.lock_dir, f"account-{digest}.lock")
        self._os = OsLock(self.lock_path, f"pid={os.getpid()} account={account_id}\n")

    @property
    def held(self) -> bool:
        return self._os.held

    def acquire(self) -> ProfileLock:
        os.makedirs(self.lock_dir, mode=0o700, exist_ok=True)
        _check_local_filesystem(self.lock_dir)
        try:
            got = self._os.try_acquire()
        except LockUnavailable as exc:
            raise StateError(str(exc)) from None
        if not got:
            raise StateLocked(f"paper account {self.account_id!r} is already owned by another process "
                              f"(profile lock {self.lock_path})")
        return self

    def release(self) -> None:
        self._os.release()

    def __enter__(self) -> ProfileLock:
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
        # Public-data sessions persist every normalized input line with its commit (recorded-session replay).
        self.record_payloads = False

    # -- opening

    @staticmethod
    def _connect(path: str, readonly: bool) -> sqlite3.Connection:
        if readonly:
            conn = sqlite3.connect(readonly_uri(path), uri=True, isolation_level=None)
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
        if not lock.held:
            raise StateError("lock must be held before creating state")
        if os.path.exists(path) and os.path.getsize(path) > 0:
            raise StateError(
                f"refusing to overwrite nonempty state {path}; use 'resume' or choose a new state path"
            )
        conn = cls._connect(path, readonly=False)
        store = cls(conn, path, readonly=False)
        store.record_payloads = meta.get("evidence") in PUBLIC_EVIDENCE
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
        if not lock.held:
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
        if version not in SUPPORTED_SCHEMAS:
            conn.close()
            raise StateError(f"incompatible schema version {version!r}; expected one of {SUPPORTED_SCHEMAS}")
        store.record_payloads = store.meta().get("evidence") in PUBLIC_EVIDENCE
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

    def has_table(self, name: str) -> bool:
        return self.conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (name,)).fetchone() \
            is not None

    def metadata_bundle(self, sha256: str) -> str | None:
        if not self.has_table("metadata_versions"):
            return None
        row = self.conn.execute("SELECT json FROM metadata_versions WHERE sha256 = ?", (sha256,)).fetchone()
        return None if row is None else row["json"]

    def payloads(self) -> list[tuple[int, str]]:
        if not self.has_table("input_payloads"):
            return []
        return [(r[0], r[1]) for r in self.conn.execute("SELECT seq, payload FROM input_payloads ORDER BY seq")]

    def manifest(self) -> dict[str, str]:
        if not self.has_table("manifest"):
            return {}
        return {r["key"]: r["value"] for r in self.conn.execute("SELECT key, value FROM manifest")}

    # -- non-economic owner writes (sessions, manifest, outbox delivery state); each is its own transaction

    def _small_write(self, sql_args: list[tuple[str, tuple]]) -> None:
        if self.readonly:
            raise StateError("read-only storage")
        c = self.conn
        c.execute("BEGIN IMMEDIATE")
        try:
            for sql, args in sql_args:
                c.execute(sql, args)
            c.execute("COMMIT")
        except BaseException:
            if c.in_transaction:
                c.execute("ROLLBACK")
            raise

    def put_manifest(self, items: dict[str, str]) -> None:
        self._small_write([("INSERT INTO manifest(key, value) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET "
                            "value = excluded.value", (k, v)) for k, v in items.items()])

    def start_session(self, session_id: str, kind: str, wall: str, cursor: int, code_revision: str) -> None:
        self._small_write([("INSERT INTO sessions(session_id, kind, started_wall, start_cursor, code_revision) "
                            "VALUES (?, ?, ?, ?, ?)", (session_id, kind, wall, cursor, code_revision))])

    def stop_session(self, session_id: str, wall: str, cursor: int, reason: str) -> None:
        self._small_write([("UPDATE sessions SET stopped_wall = ?, end_cursor = ?, stop_reason = ? "
                            "WHERE session_id = ?", (wall, cursor, reason, session_id))])

    def outbox_update(self, msg_id: str, status: str, attempts: int, wall: str, error: str | None) -> None:
        sent = wall if status == "sent" else None
        self._small_write([("UPDATE outbox SET status = ?, attempts = ?, last_attempt_wall = ?, last_error = ?, "
                            "sent_wall = coalesce(?, sent_wall) WHERE msg_id = ?",
                            (status, attempts, wall, error, sent, msg_id))])

    def outbox_insert(self, msg_id: str, created_us: int, kind: str, text: str) -> None:
        self._small_write([("INSERT OR IGNORE INTO outbox(msg_id, seq, created_us, kind, text, status, attempts) "
                            "VALUES (?, NULL, ?, ?, ?, 'pending', 0)", (msg_id, created_us, kind, text))])

    def outbox_prune(self, before_us: int) -> None:
        self._small_write([("DELETE FROM outbox WHERE status IN ('sent','failed','dropped') AND created_us < ?",
                            (before_us,))])

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
            elif op[0] == "insert_ignore":
                _, table, row = op
                cols = list(row)
                c.execute(
                    f"INSERT OR IGNORE INTO {table}({', '.join(cols)}) VALUES ({', '.join('?' for _ in cols)})",
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
            if self.record_payloads:
                if raw.line is None:
                    raise StateError("public-data sessions must persist the normalized input line")
                c.execute("INSERT INTO input_payloads(seq, payload) VALUES (?, ?)", (raw.seq, raw.line))
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
