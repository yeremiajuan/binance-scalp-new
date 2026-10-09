"""Offline synthetic replay: new run, resume and local controls.

Order of operations for every state-changing command:

1. Validate configuration (live mode rejected) and load the dated metadata.
2. Acquire the canonical state-path OS lock (fails fast if held).
3. Open/create the database, verify schema, identity and reconciliation.
4. Mutate.

Offline replay resumes the exact committed cursor and pending simulation
state (pending order, exit intent, indicators). It is not the Phase 2
forward-runner restart policy, which is not implemented here.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone

from .config import Config, config_from_canonical, load_config
from .constraints import SymbolRules, load_metadata, parse_metadata
from .engine import Engine, initial_state
from .events import InputFile, load_input
from .reconcile import require_reconciled
from .storage import StateError, StateLock, Storage


@dataclass
class RunResult:
    processed: int
    cursor: int
    total: int
    dispositions: dict[str, int]


def _check_input(cfg: Config, inp: InputFile) -> None:
    sym = inp.header.get("symbol")
    if sym != cfg.symbol:
        raise StateError(f"input symbol {sym!r} does not match configuration {cfg.symbol!r}")


def _meta(cfg: Config, rules: SymbolRules, inp: InputFile) -> dict[str, str]:
    return {
        "mode": "PAPER",
        "evidence": "SYNTHETIC",
        "account_id": cfg.account_id,
        "symbol": cfg.symbol,
        "strategy": cfg.strategy,
        "config_canonical": cfg.canonical,
        "config_sha256": cfg.sha256,
        "metadata_json": rules.raw_json,
        "metadata_sha256": rules.sha256,
        "metadata_label": rules.label,
        "metadata_retrieved_at": rules.retrieved_at,
        "input_path": inp.path,
        "input_sha256": inp.sha256,
        "input_events": str(len(inp.events)),
        "input_header": str(inp.header),
        "starting_usdt": str(cfg.starting_usdt),
        "starting_btc": str(cfg.starting_btc),
    }


def _drive(engine: Engine, inp: InputFile, stop_after: int | None) -> RunResult:
    counts: dict[str, int] = {}
    processed = 0
    for raw in inp.events[engine.state.cursor:]:
        if stop_after is not None and processed >= stop_after:
            break
        disp = engine.process(raw)
        counts[disp] = counts.get(disp, 0) + 1
        processed += 1
    return RunResult(processed, engine.state.cursor, len(inp.events), counts)


def new_run(config_path: str, input_path: str, state_path: str, *, stop_after: int | None = None,
            fault_hook=None) -> RunResult:
    cfg = load_config(config_path)
    rules = load_metadata(cfg.metadata_path)
    inp = load_input(input_path)
    _check_input(cfg, inp)
    with StateLock(state_path) as lock:
        state = initial_state(cfg)
        store = Storage.create(lock, _meta(cfg, rules, inp), state)
        try:
            store.fault_hook = fault_hook
            engine = Engine(cfg, rules, store, state)
            return _drive(engine, inp, stop_after)
        finally:
            store.close()


def open_engine(lock: StateLock, *, config: Config | None = None, input_sha: str | None = None,
                input_events: list | None = None) -> Engine:
    """Open an existing account under ``lock`` and reconcile it before any mutation."""
    store = Storage.open_existing(lock)
    try:
        meta = store.meta()
        if meta.get("mode") != "PAPER" or meta.get("evidence") != "SYNTHETIC":
            raise StateError("state is not a PAPER/SYNTHETIC Phase 1 account")
        stored_cfg = config_from_canonical(meta["config_canonical"])
        rules = parse_metadata(meta["metadata_json"])
        if config is not None and config.sha256 != stored_cfg.sha256:
            raise StateError(
                f"configuration changed: state {stored_cfg.sha256[:12]} vs supplied {config.sha256[:12]}. "
                "A configuration change is a new run/version; it cannot resume this account."
            )
        state = store.load_state()
        require_reconciled(store, state, config_sha=stored_cfg.sha256, metadata_sha=rules.sha256,
                           input_sha=input_sha, input_events=input_events)
        return Engine(stored_cfg, rules, store, state)
    except BaseException:
        store.close()
        raise


def resume_run(config_path: str, input_path: str, state_path: str, *, stop_after: int | None = None,
               fault_hook=None) -> RunResult:
    cfg = load_config(config_path)
    rules = load_metadata(cfg.metadata_path)
    inp = load_input(input_path)
    _check_input(cfg, inp)
    with StateLock(state_path) as lock:
        engine = open_engine(lock, config=cfg, input_sha=inp.sha256, input_events=inp.events)
        try:
            if engine.rules.sha256 != rules.sha256:
                raise StateError("metadata fixture changed since the account was created")
            engine.store.fault_hook = fault_hook
            return _drive(engine, inp, stop_after)
        finally:
            engine.store.close()


def control(state_path: str, kind: str, latch: str, reason: str, wall_utc: str | None = None) -> None:
    """Audited local kill/reset. Requires the lock and a reconciled state; never deletes history."""
    with StateLock(state_path) as lock:
        engine = open_engine(lock)
        try:
            wall = wall_utc or datetime.now(timezone.utc).isoformat(timespec="milliseconds")
            engine.apply_control(kind, latch, reason, wall)
        finally:
            engine.store.close()
