# Windows verification evidence: NOT RUN

This folder is filled by running, on the Windows PC that will host the trial (PowerShell, repository root):

    py -3.13 scripts\verify_windows.py

The script writes `summary.md`, `SHA256SUMS` and one text file per step here (see `docs/WINDOWS.md`, section 3).
Until those files exist, native Windows validation has **not** been performed. The implementation was built and
tested on Linux; `evidence/platform-linux/` holds the same script's Linux run, which is not Windows evidence.
