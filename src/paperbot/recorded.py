"""Forward-account state, the trial manifest, and recorded public-data session export/replay.

A forward account's database commits every normalized input line with the decision it produced, plus the
audited local controls with the cursor they were applied at. ``export_recording`` writes those inputs to a
JSON Lines file (header evidence ``PUBLIC``); ``replay_recording`` processes them into a fresh database through
the same engine, applying each control at its recorded cursor. The result is labeled PAPER / PUBLIC DATA /
RECORDED REPLAY and must reproduce the original decisions and accounting exactly.
"""

from __future__ import annotations

import hashlib
import json
import subprocess
from pathlib import Path

from .config import Config, load_config
from .engine import Engine, initial_state
from .events import load_input
from .reconcile import require_reconciled
from .storage import SCHEMA_VERSION, StateError, StateLock, Storage

EVALUATION_RULES = (
    "Predeclared (plan/PROJECT_PLAN.md, Phase 3): approximately 30 consecutive calendar days of forward PAPER; "
    "extend for outages or inadequate observations and never disguise uncovered intervals; any strategy, fee, "
    "risk or fill-model change starts a new identified version; reliability requires zero unreconciled ledger "
    "discrepancies, zero duplicate fills, no fills on stale/missing observations, successful restart "
    "reconciliation, persistent risk halts and explained threshold overshoots; initial availability target 99% "
    "with every gap over one minute explained; trading sufficiency screen of at least 100 completed trades over "
    "at least 15 active days and more than one regime, otherwise inconclusive; results never authorize live "
    "trading."
)


def code_revision() -> str:
    """git commit (+dirty flag) when available, plus a hash of the installed source files."""
    root = Path(__file__).resolve().parents[1]
    h = hashlib.sha256()
    for f in sorted(list(root.glob("paperbot/*.py")) + list(root.glob("paperbot_net/*.py"))):
        h.update(f.name.encode())
        h.update(f.read_bytes())
    rev = "unknown"
    try:
        out = subprocess.run(["git", "-C", str(root), "rev-parse", "HEAD"], capture_output=True, text=True,
                             timeout=5)
        if out.returncode == 0:
            rev = out.stdout.strip()
            dirty = subprocess.run(["git", "-C", str(root), "status", "--porcelain", "--", "."],
                                   capture_output=True, text=True, timeout=5).stdout.strip()
            rev += "+dirty" if dirty else ""
    except (OSError, subprocess.SubprocessError):
        pass
    return f"{rev} src-sha256:{h.hexdigest()[:16]}"


PROVENANCE_PUBLIC = "BINANCE_PUBLIC"  # real public Binance market data over the network
PROVENANCE_MOCKED = "MOCKED"  # public-format test data (fake servers/transports): never real market observations


def forward_meta(cfg: Config, evidence: str = "PUBLIC", provenance: str = PROVENANCE_PUBLIC) -> dict[str, str]:
    fwd = cfg.forward
    return {
        "data_provenance": provenance,
        "mode": "PAPER",
        "evidence": evidence,
        "account_id": cfg.account_id,
        "symbol": cfg.symbol,
        "strategy": cfg.strategy,
        "config_canonical": cfg.canonical,
        "config_sha256": cfg.sha256,
        "metadata_json": "",
        "metadata_sha256": "versioned (see metadata_versions)",
        "metadata_label": "PUBLIC (fetched, versioned)",
        "metadata_retrieved_at": "per version",
        "input_path": "forward session (normalized inputs in input_payloads)" if evidence == "PUBLIC"
        else "recorded public-data session",
        "input_sha256": "n/a",
        "input_events": "0",
        "input_header": "",
        "starting_usdt": str(cfg.starting_usdt),
        "starting_btc": str(cfg.starting_btc),
        "data_sources": json.dumps({"rest_host": fwd.rest_host, "ws_host": fwd.ws_host} if fwd else {}),
    }


def manifest_items(cfg: Config) -> dict[str, str]:
    fwd = cfg.forward
    return {
        "code_revision": code_revision(),
        "schema_version": SCHEMA_VERSION,
        "configuration": cfg.canonical,
        "configuration_sha256": cfg.sha256,
        "data_sources": json.dumps({
            "rest": f"https://{fwd.rest_host} GET /api/v3/time, exchangeInfo?symbol=BTCUSDT, "
                    "executionRules?symbol=BTCUSDT, klines (1m), avgPrice, referencePrice",
            "websocket": f"wss://{fwd.ws_host} combined streams btcusdt@kline_1m, btcusdt@bookTicker, "
                         "btcusdt@avgPrice, btcusdt@referencePrice",
            "credentials": "none (public market data only)",
        }) if fwd else "{}",
        "reporting_timezone": cfg.day_timezone,
        "evaluation_rules": EVALUATION_RULES,
        "strategy_version": cfg.strategy,
    }


def open_forward(lock: StateLock, cfg: Config, provenance: str = PROVENANCE_PUBLIC) -> tuple[Engine, bool]:
    """Create a new forward account, or open and reconcile an existing one. Returns (engine, restart)."""
    path = lock.canonical
    if not Path(path).exists() or Path(path).stat().st_size == 0:
        store = Storage.create(lock, forward_meta(cfg, provenance=provenance), initial_state(cfg))
        engine = Engine(cfg, None, store, store.load_state())
        engine.notify = True
        engine.notify_label = "PAPER | PUBLIC DATA" + ("" if provenance == PROVENANCE_PUBLIC else " | MOCKED")
        return engine, False
    store = Storage.open_existing(lock)
    try:
        meta = store.meta()
        if meta.get("evidence") != "PUBLIC" or meta.get("mode") != "PAPER":
            raise StateError(f"state {path} is not a PAPER forward account (evidence {meta.get('evidence')!r})")
        if meta.get("data_provenance", PROVENANCE_PUBLIC) != provenance:
            raise StateError(f"state {path} holds {meta.get('data_provenance')} data; refusing to mix it with "
                             f"{provenance} data")
        if meta.get("config_sha256") != cfg.sha256:
            raise StateError("configuration changed since this forward account was created; a configuration "
                             "change is a new version and needs a new account/state path")
        state = store.load_state()
        require_reconciled(store, state, config_sha=cfg.sha256)
        engine = Engine(cfg, None, store, state)
        engine.notify = True
        engine.notify_label = "PAPER | PUBLIC DATA" + ("" if provenance == PROVENANCE_PUBLIC else " | MOCKED")
        return engine, True
    except BaseException:
        store.close()
        raise


def export_recording(state_path: str, out_path: str) -> dict:
    store = Storage.open_readonly(state_path)
    try:
        store.conn.execute("BEGIN")
        meta = store.meta()
        if meta.get("evidence") != "PUBLIC":
            raise StateError("only forward public-data accounts can be exported as recordings")
        controls = [dict(r) for r in store.conn.execute(
            "SELECT at_cursor, kind, latch, reason, wall_utc FROM control_events ORDER BY id")]
        sessions = [dict(r) for r in store.conn.execute("SELECT * FROM sessions ORDER BY started_wall")]
        payloads = store.payloads()
        header = {
            "type": "header", "evidence": "PUBLIC", "symbol": meta["symbol"], "account_id": meta["account_id"],
            "data_provenance": meta.get("data_provenance", PROVENANCE_PUBLIC),
            "config_sha256": meta["config_sha256"], "config_canonical": meta["config_canonical"],
            "description": "PAPER forward session on public Binance spot market data (normalized engine inputs)",
            "data_sources": json.loads(meta.get("data_sources") or "{}"), "manifest": store.manifest(),
            "sessions": sessions, "controls": controls, "events": len(payloads),
        }
        store.conn.execute("COMMIT")
    finally:
        store.close()
    lines = [json.dumps(header, sort_keys=True)] + [line for _, line in payloads]
    Path(out_path).write_text("\n".join(lines) + "\n", encoding="utf-8")
    return {"events": len(payloads), "controls": len(controls), "sha256": hashlib.sha256(
        Path(out_path).read_bytes()).hexdigest()}


def replay_recording(config_path: str, input_path: str, state_path: str) -> dict:
    cfg = load_config(config_path)
    inp = load_input(input_path, allowed_evidence=("PUBLIC",))
    if inp.header.get("config_sha256") != cfg.sha256:
        raise StateError("configuration differs from the recorded session's configuration; replay with the "
                         "recorded configuration")
    controls = inp.header.get("controls", [])
    with StateLock(state_path) as lock:
        meta = forward_meta(cfg, evidence="PUBLIC_RECORDED_REPLAY",
                            provenance=str(inp.header.get("data_provenance", PROVENANCE_PUBLIC)))
        meta.update({"input_path": inp.path, "input_sha256": inp.sha256, "input_events": str(len(inp.events)),
                     "recorded_account_id": str(inp.header.get("account_id"))})
        store = Storage.create(lock, meta, initial_state(cfg))
        try:
            store.put_manifest({**manifest_items(cfg), "recording_sha256": inp.sha256,
                                "recorded_manifest": json.dumps(inp.header.get("manifest", {}), sort_keys=True)})
            engine = Engine(cfg, None, store, store.load_state())
            by_cursor: dict[int, list[dict]] = {}
            for c in controls:
                by_cursor.setdefault(int(c["at_cursor"]), []).append(c)

            def apply(cursor: int) -> None:
                for c in by_cursor.get(cursor, []):
                    engine.apply_control(c["kind"], c["latch"], c["reason"], c["wall_utc"])

            apply(0)
            for raw in inp.events:
                engine.process(raw)
                apply(raw.seq)
            return {"events": len(inp.events), "controls": len(controls), "cursor": engine.state.cursor,
                    "provenance": meta["data_provenance"]}
        finally:
            store.close()
