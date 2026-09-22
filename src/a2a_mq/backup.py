"""Offline backup restore helpers. Production PostgreSQL backup remains deployment-owned."""
from __future__ import annotations

from pathlib import Path
import sqlite3

from .domain import PermanentError, RetryableError
from .operations import OperationsManager


def restore_sqlite_backup(source: str | Path, destination: str | Path) -> dict:
    """Restore a verified SQLite backup to a new path; never follow/overwrite destination."""
    source, destination = Path(source).absolute(), Path(destination).absolute()
    expected = OperationsManager.verify_sqlite_backup(source)
    try:
        from .sqlite_archive import restore_sqlite
        restored = restore_sqlite(source, destination, expected_sha256=expected["sha256"])
        verified = OperationsManager.verify_sqlite_backup(destination)
        require(restored["sha256"] == verified["sha256"]
                and restored["bytes"] == verified["bytes"], "BACKUP_INTEGRITY_ERROR")
        return verified
    except PermanentError:
        raise
    except (OSError, sqlite3.Error):
        raise RetryableError("RESTORE_UNAVAILABLE") from None
