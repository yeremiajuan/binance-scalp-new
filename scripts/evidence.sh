#!/usr/bin/env bash
# Reproduce every Phase 1 evidence artifact from a clean virtual environment.
# PAPER | SYNTHETIC only. Needs no credentials; the replay itself needs no network
# (pip needs package-index access once, to install pytest).
#
# Usage: scripts/evidence.sh [EVIDENCE_DIR]    (default: ./evidence)
set -uo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
EV="${1:-$ROOT/evidence}"
WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT
mkdir -p "$EV"
cd "$ROOT"

step() { echo; echo "### $*"; }
run() { echo "\$ $*"; "$@"; local rc=$?; echo "[exit $rc]"; return $rc; }

{
  step "environment"
  run uname -srm
  run python3 --version
  run git rev-parse HEAD
  run git status --porcelain
} > "$EV/environment.txt" 2>&1

# Clean install
python3 -m venv "$WORK/venv" && "$WORK/venv/bin/pip" install -q --upgrade pip >/dev/null \
  && "$WORK/venv/bin/pip" install -q -e "$ROOT[test]" ruff >/dev/null
export PATH="$WORK/venv/bin:$PATH"
{
  echo "clean venv: $WORK/venv"
  run python --version
  run python -m pytest --version
  run pip freeze --exclude-editable
} >> "$EV/environment.txt" 2>&1

{ run python -m pytest -v -p no:cacheprovider; } > "$EV/pytest.txt" 2>&1
{ run ruff check src tests scripts; } > "$EV/ruff.txt" 2>&1
{ run paperbot validate-config config/paper.toml; } > "$EV/validate_config.txt" 2>&1

# Uninterrupted replay of the committed SYNTHETIC demo fixture.
rm -f "$EV/demo_state.sqlite" "$EV/demo_state.sqlite.lock"
{
  run python scripts/make_demo_fixture.py "$WORK/regenerated.jsonl"
  run cmp "$WORK/regenerated.jsonl" fixtures/synthetic_demo.jsonl && echo "fixture regenerates byte-for-byte"
  run sha256sum config/paper.toml fixtures/synthetic_demo.jsonl fixtures/exchange_info_btcusdt_synthetic.json
  run paperbot replay --config config/paper.toml --input fixtures/synthetic_demo.jsonl --state "$EV/demo_state.sqlite"
} > "$EV/replay.txt" 2>&1
rm -f "$EV/demo_state.sqlite.lock"
paperbot report --state "$EV/demo_state.sqlite" > "$EV/demo_report.txt" 2>&1
paperbot report --state "$EV/demo_state.sqlite" --json > "$EV/demo_report.json" 2>&1
python scripts/ledger_reconciliation.py "$EV/demo_state.sqlite" > "$EV/ledger_reconciliation.md" 2>&1

# Interrupted replay (crashes before/after commit, mid-order crash, partial runs) vs uninterrupted.
{
  run python scripts/crash_resume_demo.py config/paper.toml fixtures/synthetic_demo.jsonl "$EV/demo_state.sqlite" "$WORK"
  run python scripts/compare_states.py "$EV/demo_state.sqlite" "$WORK/restarted.sqlite"
} > "$EV/restart_equivalence.txt" 2>&1

# Real two-process exclusion on one canonical state path (and aliases).
{
  T="$WORK/lock"; mkdir -p "$T"; ln -s "$T" "$WORK/lock-alias"
  echo "process A: paperbot replay (runs ~30 s, holds the lock for its lifetime)"
  paperbot replay --config config/paper.toml --input fixtures/synthetic_demo.jsonl --state "$T/state.sqlite" --quiet \
    > "$T/a.out" 2>&1 &
  A=$!
  for _ in $(seq 1 200); do [ -s "$T/state.sqlite" ] && break; sleep 0.05; done
  sleep 1
  echo "process A pid $A running: $(kill -0 $A 2>/dev/null && echo yes || echo no)"
  run paperbot resume --config config/paper.toml --input fixtures/synthetic_demo.jsonl --state "$T/state.sqlite"
  run paperbot resume --config config/paper.toml --input fixtures/synthetic_demo.jsonl --state "$WORK/lock-alias/state.sqlite"
  run paperbot kill --state "$WORK/lock-alias/../lock/state.sqlite" --reason "second process"
  run paperbot replay --config config/paper.toml --input fixtures/synthetic_demo.jsonl --state "$T/state.sqlite"
  echo "read-only status while A owns the state:"
  run paperbot status --state "$T/state.sqlite"
  wait $A; echo "process A exit $?"; cat "$T/a.out" | head -1
  echo "A's final state vs the uninterrupted reference (proves the losers changed nothing):"
  run python scripts/compare_states.py "$EV/demo_state.sqlite" "$T/state.sqlite"
} > "$EV/two_process_lock.txt" 2>&1

# Audited kill/reset with an open position, across restarts and 00:00 Asia/Jakarta.
{
  K="$WORK/kill.sqlite"
  run paperbot replay --config config/paper.toml --input fixtures/synthetic_demo.jsonl --state "$K" --stop-after 11250 --quiet
  run paperbot status --state "$K"
  run paperbot kill --state "$K" --reason "evidence: manual kill with an open position"
  run paperbot resume --config config/paper.toml --input fixtures/synthetic_demo.jsonl --state "$K" --stop-after 60 --quiet
  run paperbot status --state "$K"
  echo "reset without --confirm must fail:"
  run paperbot reset --state "$K" --latch manual_kill --reason "x"
  run paperbot reset --state "$K" --latch manual_kill --reason "evidence: reviewed, resume paper" --confirm
  run paperbot resume --config config/paper.toml --input fixtures/synthetic_demo.jsonl --state "$K" --quiet
  paperbot report --state "$K" | grep -E "^PAPER|CONTROL|latch|exit=|halt|UNRESOLVED" | head -40
} > "$EV/kill_reset.txt" 2>&1

{
  echo "live-mode rejection:"
  run paperbot --live status --state x
  run paperbot replay --mode=live --config c --input i --state s
  sed 's/^mode = "paper".*/mode = "live"/' config/paper.toml > "$WORK/live.toml"
  sed -i "s#\.\./fixtures#$ROOT/fixtures#" "$WORK/live.toml"
  run paperbot validate-config "$WORK/live.toml"
} > "$EV/live_rejection.txt" 2>&1

sha256sum "$EV"/demo_state.sqlite "$EV"/*.txt "$EV"/*.json "$EV"/*.md > "$EV/SHA256SUMS" 2>/dev/null
echo "evidence written to $EV"
grep -E "passed|failed" "$EV/pytest.txt" | tail -1
