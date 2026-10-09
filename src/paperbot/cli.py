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
from .report import build_report, render_report, render_status, to_json
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
    ap = argparse.ArgumentParser(prog="paperbot", description="PAPER | SYNTHETIC offline core (Phase 1).")
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

    for name in ("status", "report"):
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


def _print_report(state: str, full: bool, as_json: bool) -> None:
    store = Storage.open_readonly(state)
    try:
        r = build_report(store)
    finally:
        store.close()
    if as_json:
        print(to_json(r))
    else:
        print(render_report(r) if full else render_status(r))


def main(argv: list[str] | None = None, *, wall_clock=None) -> int:
    argv = list(sys.argv[1:] if argv is None else argv)
    wall_clock = wall_clock or (lambda: datetime.now(timezone.utc).isoformat(timespec="milliseconds"))
    try:
        _reject_live(argv)
        try:
            args = _parser().parse_args(argv)
        except SystemExit as exc:  # usage errors and --help
            return int(exc.code or 0)
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
        if args.cmd in ("status", "report"):
            _print_report(args.state, full=args.cmd == "report", as_json=args.json)
            return 0
        if args.cmd == "kill":
            control(args.state, "kill", "manual_kill", args.reason, wall_clock())
            print("PAPER | SYNTHETIC | manual_kill latched (audited). Pending entries are canceled and tradable "
                  "inventory exits at the next trustworthy observation when replay resumes.")
            return 0
        if args.cmd == "reset":
            control(args.state, "reset", args.latch, args.reason, wall_clock())
            print(f"PAPER | SYNTHETIC | {args.latch} reset (audited). History is preserved; no implicit resume of "
                  "other latches.")
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
