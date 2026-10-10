# Verification summary: LINUX RUN (all checks passed) - NOT a Windows validation

- date (UTC): 2026-10-10T13:14:39+00:00
- platform: Linux-6.18.44-fc-v114-x86_64-with-glibc2.39
- python: 3.13.16

| step | file | result |
|---|---|---|
| clean install (pip install -e .[test]) | `environment.txt` | PASS |
| environment facts | `environment.txt` | INFO |
| connectivity probe | `connectivity.txt` | INFO |
| test suite | `pytest.txt` | PASS |
| lint | `ruff.txt` | PASS |
| package build | `build.txt` | PASS |
| validate configurations | `validate_config.txt` | PASS |
| synthetic replay determinism | `synthetic_replay.txt` | PASS |
| mocked recorded-replay comparison | `recorded_replay.txt` | PASS |
| ownership, controls and shutdown drill | `ownership_drill.txt` | PASS |
