"""A forward-runner owner process for the ownership tests (tests/test_ownership.py).

Runs the real ``run_forward`` (both OS locks, the authenticated control channel, the owner thread and the signal
handlers) on mocked public data: a fake REST transport in this process and the test's local WebSocket server.
Exit codes follow the CLI: 0 graceful stop, 3 locked, 5 halted.

Usage: python tests/owner_process.py CONFIG STATE WS_URL
"""

from __future__ import annotations

import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from fake_binance import FakeMarket, FakeRestTransport  # noqa: E402

from paperbot.storage import StateLocked  # noqa: E402
from paperbot.synthetic import staircase  # noqa: E402
from paperbot.timeutil import FIVE_MINUTES_US  # noqa: E402
from paperbot_net.runner import run_forward  # noqa: E402


def market_now() -> FakeMarket:
    """Warm-up bars ending at the current minute, so the owner reaches warm-up like the e2e tests."""
    now = time.time_ns() // 1000
    start = (now // FIVE_MINUTES_US) * FIVE_MINUTES_US - 300 * 60_000_000
    return FakeMarket(staircase(start, 13))


def main(config: str, state: str, ws_url: str) -> int:
    try:
        return run_forward(config, state, transport=FakeRestTransport(market_now()), ws_url=ws_url,
                           install_signals=True, log=lambda msg: print(msg, flush=True))
    except StateLocked as exc:
        print(f"PAPER | LOCKED: {exc}", file=sys.stderr, flush=True)
        return 3


if __name__ == "__main__":
    sys.exit(main(*sys.argv[1:4]))
