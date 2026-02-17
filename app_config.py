from __future__ import annotations

import os
import tempfile
from pathlib import Path


def _path_from_env(name: str, default: str) -> str:
    value = os.getenv(name)
    if value:
        return os.path.expandvars(os.path.expanduser(value))
    return default


REMOTE_DB_FULL = _path_from_env(
    "HANDLER_REMOTE_DB_FULL",
    r"I:\6_EQUITIES\Database\Eiere-Database\topchanges.db",
)

REMOTE_DB_RECENT = _path_from_env(
    "HANDLER_REMOTE_DB_RECENT",
    r"I:\6_EQUITIES\Database\Eiere-Database\topchanges_recent_60d.db",
)

LIST_DIR = _path_from_env(
    "HANDLER_LIST_DIR",
    r"I:\6_EQUITIES\Database\Eiere-Styring",
)

LOCAL_WORKDIR = _path_from_env(
    "HANDLER_LOCAL_WORKDIR",
    os.path.join(tempfile.gettempdir(), "topchanges_sqlite_work"),
)

LOCAL_DB_NAME = _path_from_env("HANDLER_LOCAL_DB_NAME", "topchanges.db")
LOCAL_DB_PATH = str(Path(LOCAL_WORKDIR) / LOCAL_DB_NAME)
