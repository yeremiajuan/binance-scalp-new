#!/usr/bin/env bash
# Reproduce the Phase 2 evidence artifacts from a clean virtual environment.
# All of it is offline: mocked transports, local fake WebSocket servers and step-mode sessions (labeled MOCKED).
# It does NOT start a public-data session. Phase 1 artifacts in evidence/ are left untouched.
#
# Usage: scripts/evidence_phase2.sh [EVIDENCE_DIR]    (default: ./evidence/phase2)
set -uo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
EV="${1:-$ROOT/evidence/phase2}"
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

# Connectivity facts of this environment (read-only probes of public hosts; no data is used).
{
  step "public Binance hosts reachable from this environment?"
  for u in https://api.binance.com/api/v3/time https://data-api.binance.vision/api/v3/time \
           https://data-stream.binance.vision/; do
    echo "\$ curl -sS -o /dev/null -w '%{http_code}' --max-time 10 $u"
    curl -sS -o /dev/null -w 'http_code=%{http_code}\n' --max-time 10 "$u" 2>&1; echo "[exit $?]"
  done
  echo
  echo "http_code=000 / non-zero exit means the host was not reachable: no public-data session was possible here."
} > "$EV/connectivity.txt" 2>&1

python3 -m venv "$WORK/venv" && "$WORK/venv/bin/pip" install -q --upgrade pip >/dev/null \
  && "$WORK/venv/bin/pip" install -q -e "$ROOT[test]" >/dev/null
export PATH="$WORK/venv/bin:$PATH"
{
  echo "clean venv: $WORK/venv"
  run python --version
  run python -m pytest --version
  run pip freeze --exclude-editable
} >> "$EV/environment.txt" 2>&1

{ run python -m pytest -v -p no:cacheprovider; } > "$EV/pytest.txt" 2>&1
{ run ruff check src tests scripts; } > "$EV/ruff.txt" 2>&1
{ run python -m build --outdir "$WORK/dist"; run ls -l "$WORK/dist"; \
  run python -c "import zipfile,glob; z=zipfile.ZipFile(glob.glob('$WORK/dist/*.whl')[0]); \
print('\n'.join(n for n in z.namelist() if n.endswith('.py')))"; } > "$EV/build.txt" 2>&1
{
  run paperbot validate-config config/forward.toml
  run paperbot validate-config config/paper.toml
  run sha256sum config/forward.toml config/paper.toml
} > "$EV/validate_config.txt" 2>&1

# Mocked step-mode session -> export -> recorded replay -> table comparison; status/positions samples.
{ run python scripts/phase2_mocked_demo.py "$WORK/demo"; } > "$EV/recorded_replay.txt" 2>&1
sed -n '/^### paperbot status/,$p' "$EV/recorded_replay.txt" > "$EV/status_samples.txt"

# Static inspection: no signer, credential, account or order path in either package.
{
  step "grep for signing/credential/order/account/testnet code (expected: only the PAPER-only rejection text in cli.py/config.py; none in paperbot_net)"
  run grep -rniE "hmac|X-MBX-APIKEY|/api/v3/order|/api/v3/account|userDataStream|listenKey|/sapi/|testnet" \
    --include=*.py src/paperbot_net src/paperbot
  step "REST endpoints the network package can call"
  run python -c "from paperbot_net.rest import ENDPOINTS; [print(k, v) for k, v in sorted(ENDPOINTS.items())]"
  step "HTTP methods used by the REST client"
  run grep -n "method=" src/paperbot_net/rest.py
} > "$EV/static_scan.txt" 2>&1

(cd "$EV" && sha256sum ./*.txt > SHA256SUMS)
echo "Phase 2 evidence written to $EV"
