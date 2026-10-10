"""Per-user directories for local runtime files (profile locks, control endpoints).

Windows: ``%LOCALAPPDATA%\\paperbot`` (its default ACL grants only the user, SYSTEM and Administrators).
Linux/macOS: ``~/.local/state/paperbot`` (created mode 0700).
"""

from __future__ import annotations

import os


def user_state_dir() -> str:
    if os.name == "nt":
        base = os.environ.get("LOCALAPPDATA") or os.path.join(os.path.expanduser("~"), "AppData", "Local")
        return os.path.join(base, "paperbot")
    return os.path.join(os.path.expanduser("~"), ".local", "state", "paperbot")


def default_profile_lock_dir() -> str:
    return os.path.join(user_state_dir(), "locks")


def default_control_dir() -> str:
    """Overridable with PAPERBOT_CONTROL_DIR (tests, or several isolated users of one account)."""
    return os.environ.get("PAPERBOT_CONTROL_DIR") or os.path.join(user_state_dir(), "control")
