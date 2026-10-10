"""OS-backed exclusive process locks for Windows and Linux (macOS uses the same flock path, unverified).

The lock primitive comes from ``filelock`` (tox-dev/py-filelock, MIT, pure Python, no dependencies):

* Linux/macOS: ``fcntl.flock(LOCK_EX | LOCK_NB)`` on the lock file;
* Windows: ``LockFileEx(LOCKFILE_EXCLUSIVE_LOCK | LOCKFILE_FAIL_IMMEDIATELY)`` on one byte of the lock file.

Both are kernel locks tied to an open handle: they are released when the process exits for any reason (including a
crash or a kill), and a second handle, in this or another process, cannot take them while held. This module never
falls back to lock-file existence: ``fallback_to_soft=False`` makes filelock fail closed when a filesystem lacks
flock, and anything other than the two native lock classes is refused. The lock file is kept after release
(``preserve_lock_file=True``) so its identity is stable. On Linux its text (pid, purpose) is informational only and
is never consulted to decide ownership; on Windows it stays empty.
"""

from __future__ import annotations

import os

from filelock import FileLock, Timeout, UnixFileLock, WindowsFileLock

NATIVE_LOCKS = (UnixFileLock, WindowsFileLock)


class LockUnavailable(RuntimeError):
    """No OS-backed lock primitive is available for this path or platform."""


class OsLock:
    """One exclusive, non-blocking OS lock on ``path``; held until ``release()`` or process exit."""

    def __init__(self, path: str, holder_text: str):
        self.path = path
        self.holder_text = holder_text
        self._lock: FileLock | None = None

    @property
    def held(self) -> bool:
        return self._lock is not None and self._lock.is_locked

    def try_acquire(self) -> bool:
        """True when acquired; False when another handle (any process) holds it. Never waits."""
        try:
            lock = FileLock(self.path, timeout=0, mode=0o600, thread_local=False, blocking=False,
                            fallback_to_soft=False, preserve_lock_file=True, on_acquired=self._write_holder)
        except (TypeError, ValueError) as exc:  # e.g. a soft (existence) lock rejects these options
            raise LockUnavailable(f"no OS-backed lock primitive for {self.path}: {exc}") from exc
        if not isinstance(lock, NATIVE_LOCKS):
            raise LockUnavailable(f"no OS-backed lock primitive on this platform for {self.path}")
        try:
            lock.acquire()
        except Timeout:
            return False
        except OSError as exc:  # e.g. a filesystem without flock (ENOSYS) or a reparse point on Windows
            raise LockUnavailable(f"cannot take an OS lock on {self.path}: {exc}") from exc
        if not isinstance(lock, NATIVE_LOCKS):  # pragma: no cover - fallback_to_soft=False forbids the switch
            lock.release()
            raise LockUnavailable(f"refusing a non-OS lock on {self.path}")
        self._lock = lock
        return True

    def _write_holder(self, fd: int) -> None:
        if os.name == "nt":
            return  # the locked byte range is at offset 0; leave the Windows lock file untouched
        os.lseek(fd, 0, os.SEEK_SET)
        os.ftruncate(fd, 0)
        os.write(fd, self.holder_text.encode("utf-8"))

    def release(self) -> None:
        if self._lock is not None:
            lock, self._lock = self._lock, None
            lock.release(force=True)
