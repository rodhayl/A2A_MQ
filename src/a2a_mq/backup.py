"""Offline backup restore helpers. Production PostgreSQL backup remains deployment-owned."""
from __future__ import annotations

import os
from pathlib import Path
import sqlite3

from .domain import PermanentError, RetryableError, require
from .operations import OperationsManager


def restore_sqlite_backup(source: str | Path, destination: str | Path) -> dict:
    """Restore a verified SQLite backup to a new path and fsync it; never overwrite."""
    source, destination = Path(source).absolute(), Path(destination).absolute()
    OperationsManager.verify_sqlite_backup(source)
    require(not destination.exists() and destination.parent.is_dir(), "INVALID_BACKUP_PATH")
    try:
        src = sqlite3.connect(f"file:{source.as_posix()}?mode=ro", uri=True)
        dst = sqlite3.connect(destination)
        try:
            src.backup(dst)
            dst.commit()
        finally:
            dst.close()
            src.close()
        result = OperationsManager.verify_sqlite_backup(destination)
        fd = os.open(destination, os.O_RDONLY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)
        return result
    except PermanentError:
        destination.unlink(missing_ok=True)
        raise
    except (OSError, sqlite3.Error):
        destination.unlink(missing_ok=True)
        raise RetryableError("RESTORE_UNAVAILABLE") from None
