"""Offline administrator backup/restore for complete SQLite files, including WAL.

This operates across all scopes in a file and is intentionally not an HTTP API.
Use PostgreSQL's native pg_dump/pg_restore for the PostgreSQL backend.
"""
from __future__ import annotations

from contextlib import closing, contextmanager
import hashlib
import os
from pathlib import Path
import sqlite3
import stat
import tempfile
import time

from .domain import PermanentError, require


def sha256(path: Path) -> str:
    hashed = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1048576), b""):
            hashed.update(block)
    return hashed.hexdigest()


@contextmanager
def verified_archive(source, expected_sha256, *, timeout=300):
    """Hash the exact private bytes the restore tools will consume, once."""
    require(isinstance(expected_sha256, str) and len(expected_sha256) == 64
            and all(c in "0123456789abcdef" for c in expected_sha256), "INVALID_BACKUP_DIGEST")
    source = Path(source).absolute()
    require(not source.is_symlink(), "INVALID_BACKUP_SOURCE")
    deadline = time.monotonic() + timeout
    with tempfile.TemporaryDirectory(prefix="a2amq-restore-") as temporary:
        private = Path(temporary) / "archive"
        fd = os.open(source, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
                     | getattr(os, "O_NONBLOCK", 0))
        with os.fdopen(fd, "rb") as original:
            require(stat.S_ISREG(os.fstat(original.fileno()).st_mode), "INVALID_BACKUP_SOURCE")
            out = os.open(private, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
            hashed = hashlib.sha256()
            with os.fdopen(out, "wb") as target:
                for block in iter(lambda: original.read(1048576), b""):
                    require(time.monotonic() <= deadline, "BACKUP_TIMEOUT")
                    hashed.update(block)
                    target.write(block)
                target.flush()
                os.fsync(target.fileno())
        require(hashed.hexdigest() == expected_sha256, "BACKUP_DIGEST_MISMATCH")
        yield private

def backup_sqlite(source: str | Path, destination: str | Path, *, timeout: float = 60) -> dict:
    source, destination = Path(source).absolute(), Path(destination).absolute()
    require(type(timeout) in {int,float} and 0 < timeout <= 3600, "INVALID_TIMEOUT")
    require(source.is_file() and not source.is_symlink(), "INVALID_BACKUP_SOURCE")
    require(not os.path.lexists(destination) and destination.parent.is_dir(), "BACKUP_DESTINATION_EXISTS")
    deadline = time.monotonic()+timeout
    def progress(status, remaining, total):
        if time.monotonic() > deadline:
            raise PermanentError("BACKUP_TIMEOUT")
    fd, name = tempfile.mkstemp(prefix=".a2amq-backup-", dir=destination.parent)
    os.close(fd)
    temporary = Path(name)
    try:
        with closing(sqlite3.connect(source.as_uri()+"?mode=ro", uri=True, timeout=5)) as original:
            with closing(sqlite3.connect(temporary, timeout=5)) as target:
                original.backup(target, pages=256, progress=progress, sleep=.01)
                require(target.execute("PRAGMA quick_check").fetchall() == [("ok",)], "BACKUP_INTEGRITY_ERROR")
                target.execute("PRAGMA journal_mode=DELETE")
        with temporary.open("rb") as stream:
            os.fsync(stream.fileno())
        digest = sha256(temporary)
        os.link(temporary, destination)
        parent = os.open(destination.parent, os.O_RDONLY)
        try:
            os.fsync(parent)
        finally:
            os.close(parent)
        return {"file": destination.name, "sha256": digest, "bytes": destination.stat().st_size,
                "scope": "entire-sqlite-file", "method": "sqlite-online-backup"}
    except sqlite3.Error:
        raise PermanentError("BACKUP_FAILED") from None
    finally:
        temporary.unlink(missing_ok=True)


def restore_sqlite(backup: str | Path, destination: str | Path, *, expected_sha256: str) -> dict:
    with verified_archive(backup, expected_sha256) as verified:
        return backup_sqlite(verified, destination)
