"""Command-line interface. PAPER and SYNTHETIC only; there is no live mode or --live switch.

Exit codes: 0 ok, 2 configuration/usage error (including any live attempt), 3 state locked by another
process, 4 state/reconciliation error, 5 processing failure (engine halted; resume after investigation).
"""

from __future__ import annotations

import argparse
import sys
from datetime import datetime, timezone

from .config import ConfigError, LiveModeRejected, load_config
from .constraints import MetadataError, load_metadata
from .events import InputError, load_input
from .reconcile import ReconciliationError
from .replay import control, new_run, resume_run
from .report import build_report, render_positions, render_report, render_status, to_json
from .risk import LATCHES
from .storage import StateError, StateLocked, Storage

LIVE_FLAGS = ("--live", "--testnet", "--real", "--mode")


def _reject_live(argv: list[str]) -> None:
    for a in argv:
        if any(a == f or a.startswith(f + "=") for f in LIVE_FLAGS):
            raise LiveModeRejected(
                f"{a!r} rejected: this tool is PAPER-only. Live/testnet execution is not implemented and cannot "
                "be enabled from the command line."
            )


def _parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(prog="paperbot", description="PAPER-only BTCUSDT paper bot: synthetic replay "
                                 "(Phase 1) and a public-data forward runner (Phase 2). No live trading exists.")
    sub = ap.add_subparsers(dest="cmd", required=True)

    v = sub.add_parser("validate-config", help="validate configuration and metadata fixture")
    v.add_argument("config")

    for name, helptext in (("replay", "start a NEW synthetic replay account"),
                           ("resume", "resume an existing account from its committed cursor")):
        r = sub.add_parser(name, help=helptext)
        r.add_argument("--config", required=True)
        r.add_argument("--input", required=True)
        r.add_argument("--state", required=True)
        r.add_argument("--stop-after", type=int, default=None, help="process at most N more input events")
        r.add_argument("--quiet", action="store_true")

    f = sub.add_parser("run", help="start/restart the PAPER forward runner on public market data (foreground)")
    f.add_argument("--config", required=True)
    f.add_argument("--state", required=True)
    f.add_argument("--run-seconds", type=float, default=None, help="stop gracefully after N seconds")

    st = sub.add_parser("stop", help="ask the active forward runner to stop gracefully (local control socket)")
    st.add_argument("--state", required=True)
    st.add_argument("--reason", default="local stop command")

    ex = sub.add_parser("export-recording", help="write a forward account's normalized inputs as a recording")
    ex.add_argument("--state", required=True)
    ex.add_argument("--out", required=True)

    rr = sub.add_parser("replay-recording", help="replay a recorded public-data session into a NEW state")
    rr.add_argument("--config", required=True)
    rr.add_argument("--input", required=True)
    rr.add_argument("--state", required=True)

    for name in ("status", "report", "positions"):
        s = sub.add_parser(name, help=f"read-only {name}")
        s.add_argument("--state", required=True)
        s.add_argument("--json", action="store_true")

    k = sub.add_parser("kill", help="latch the persistent manual kill (audited)")
    k.add_argument("--state", required=True)
    k.add_argument("--reason", required=True)

    z = sub.add_parser("reset", help="reset one latch (audited; requires reconciled state)")
    z.add_argument("--state", required=True)
    z.add_argument("--latch", required=True, choices=LATCHES)
    z.add_argument("--reason", required=True)
    z.add_argument("--confirm", action="store_true", required=True,
                   help="required: acknowledges that the reset is recorded permanently")
    return ap


def _print_report(state: str, full: bool, as_json: bool, positions: bool = False) -> None:
    store = Storage.open_readonly(state)
    try:
        r = build_report(store)
    finally:
        store.close()
    if as_json:
        print(to_json(r))
    elif positions:
        print(render_positions(r))
    else:
        print(render_report(r) if full else render_status(r))


def _is_forward(path: str) -> bool:
    import tomllib

    try:
        with open(path, "rb") as fh:
            return "forward" in tomllib.load(fh)
    except (OSError, tomllib.TOMLDecodeError):
        return False


def _route_control(state: str, request: dict, direct) -> str:
    """Apply a control directly when no process owns the state; otherwise route it through the owner."""
    try:
        direct()
        return "direct"
    except StateLocked:
        from paperbot_net.runner import send_control

        try:
            reply = send_control(state, request)
        except (FileNotFoundError, ConnectionError, OSError):
            raise StateLocked(f"state {state} is locked by another process that has no control socket") from None
        if not reply.get("ok"):
            raise ValueError(f"owner rejected the control: {reply.get('error')}") from None
        return "routed to the active forward runner: " + str(reply.get("result"))


def main(argv: list[str] | None = None, *, wall_clock=None) -> int:
    argv = list(sys.argv[1:] if argv is None else argv)
    wall_clock = wall_clock or (lambda: datetime.now(timezone.utc).isoformat(timespec="milliseconds"))
    try:
        _reject_live(argv)
        try:
            args = _parser().parse_args(argv)
        except SystemExit as exc:  # usage errors and --help
            return int(exc.code or 0)
        if args.cmd == "validate-config" and _is_forward(args.config):
            cfg = load_config(args.config)
            print(f"PAPER | PUBLIC DATA | configuration OK | account {cfg.account_id} | {cfg.symbol} | {cfg.strategy}")
            print(f"config_sha256 {cfg.sha256}")
            print(f"public market data: REST https://{cfg.forward.rest_host} (GET only), "
                  f"WebSocket wss://{cfg.forward.ws_host}; no credentials")
            print(f"telegram: {'enabled' if cfg.telegram and cfg.telegram.enabled else 'disabled'}")
            print("live mode: not implemented (rejected)")
            return 0
        if args.cmd == "validate-config":
            cfg = load_config(args.config)
            rules = load_metadata(cfg.metadata_path)
            print(f"PAPER | SYNTHETIC | configuration OK | account {cfg.account_id} | {cfg.symbol} | {cfg.strategy}")
            print(f"config_sha256 {cfg.sha256}")
            print(f"metadata {rules.label} retrieved {rules.retrieved_at} sha256 {rules.sha256}")
            print("live mode: not implemented (rejected)")
            return 0
        if args.cmd in ("replay", "resume"):
            run = new_run if args.cmd == "replay" else resume_run
            res = run(args.config, args.input, args.state, stop_after=args.stop_after)
            inp = load_input(args.input)
            print(f"PAPER | SYNTHETIC | {args.cmd}: processed {res.processed} events, cursor {res.cursor}/{res.total}"
                  f" | input sha256 {inp.sha256} | dispositions {res.dispositions}")
            if not args.quiet:
                _print_report(args.state, full=False, as_json=False)
            return 0
        if args.cmd in ("status", "report", "positions"):
            _print_report(args.state, full=args.cmd == "report", as_json=args.json, positions=args.cmd == "positions")
            return 0
        if args.cmd == "kill":
            how = _route_control(args.state, {"cmd": "kill", "reason": args.reason},
                                 lambda: control(args.state, "kill", "manual_kill", args.reason, wall_clock()))
            print(f"PAPER | manual_kill latched (audited; {how}). Pending entries are canceled and tradable "
                  "inventory exits at the next trustworthy observation.")
            return 0
        if args.cmd == "reset":
            how = _route_control(args.state, {"cmd": "reset", "latch": args.latch, "reason": args.reason},
                                 lambda: control(args.state, "reset", args.latch, args.reason, wall_clock()))
            print(f"PAPER | {args.latch} reset (audited; {how}). History is preserved; no implicit resume of "
                  "other latches.")
            return 0
        if args.cmd == "run":
            from paperbot_net.runner import run_forward
            from paperbot_net.telegram import notifier_factory

            return run_forward(args.config, args.state, run_seconds=args.run_seconds,
                               notifier_factory=notifier_factory)
        if args.cmd == "stop":
            from paperbot_net.runner import send_control

            try:
                reply = send_control(args.state, {"cmd": "stop", "reason": args.reason})
            except (FileNotFoundError, ConnectionError, OSError) as exc:
                raise StateError(f"no active forward runner for {args.state}: {exc}") from None
            print(f"PAPER | stop: {reply}")
            return 0 if reply.get("ok") else 4
        if args.cmd == "export-recording":
            from .recorded import export_recording

            res = export_recording(args.state, args.out)
            print(f"PAPER | PUBLIC DATA | exported {res['events']} inputs and {res['controls']} controls to "
                  f"{args.out} (sha256 {res['sha256']})")
            return 0
        if args.cmd == "replay-recording":
            from .recorded import replay_recording

            res = replay_recording(args.config, args.input, args.state)
            print(f"PAPER | PUBLIC DATA | RECORDED REPLAY ({res['provenance']}): processed {res['events']} inputs "
                  f"and {res['controls']} controls; cursor {res['cursor']}")
            _print_report(args.state, full=False, as_json=False)
            return 0
    except LiveModeRejected as exc:
        print(f"PAPER | REJECTED: {exc}", file=sys.stderr)
        return 2
    except (ConfigError, MetadataError, InputError) as exc:
        print(f"PAPER | configuration/input error: {exc}", file=sys.stderr)
        return 2
    except StateLocked as exc:
        print(f"PAPER | LOCKED: {exc}", file=sys.stderr)
        return 3
    except (StateError, ReconciliationError, ValueError) as exc:
        print(f"PAPER | STATE ERROR (halted, nothing changed after the last commit): {exc}", file=sys.stderr)
        return 4
    except Exception as exc:  # noqa: BLE001 - surface any processing failure with a clear halt
        print(f"PAPER | PROCESSING FAILURE (halted; last committed state retained): {type(exc).__name__}: {exc}",
              file=sys.stderr)
        return 5
    return 2


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
