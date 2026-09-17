"""Administrator-only PostgreSQL native archive backup and empty-target restore.

Uses libpq service files, not credentials in command arguments. Restore requires
an explicitly named, precreated empty database and an exact archive digest. The
operator must keep that target offline and exclusive while restoring.
"""
from __future__ import annotations

from contextlib import contextmanager
import configparser
import os
from pathlib import Path
import subprocess
import tempfile

from .sqlite_archive import sha256, verified_archive
from .configuration import read_file
from .domain import PermanentError, require, token


@contextmanager
def service_environment(service_file, name):
    token(name)
    raw = read_file(str(service_file), secret=True)
    parsed = configparser.ConfigParser(interpolation=None)
    try:
        parsed.read_string(raw)
        section = parsed[name]
        require(bool(section.get("dbname")) and bool(section.get("host"))
                and bool(section.get("user")), "INCOMPLETE_PG_SERVICE")
        require(section.get("sslmode") == "verify-full" and bool(section.get("sslrootcert")),
                "POSTGRES_TLS_REQUIRED")
    except (configparser.Error, KeyError):
        raise PermanentError("INVALID_PG_SERVICE") from None
    with tempfile.TemporaryDirectory(prefix="a2amq-pg-") as temporary:
        path = Path(temporary) / "service.conf"
        fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(fd, "w") as stream:
            stream.write(raw)
        env = {"PATH": os.environ.get("PATH", "/usr/bin:/bin"), "HOME": temporary,
               "LANG": "C", "PGSERVICEFILE": str(path), "PGSERVICE": name,
               "PGCONNECT_TIMEOUT": "10", "PGAPPNAME": "a2amq-backup"}
        yield env, section["dbname"]


def _run(args, env, timeout, *, output=False):
    require(type(timeout) in {int, float} and 0 < timeout <= 86400, "INVALID_TIMEOUT")
    try:
        result = subprocess.run(args, env=env, stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE if output else subprocess.DEVNULL,
            stderr=subprocess.DEVNULL, timeout=timeout, check=False)
    except (OSError, subprocess.TimeoutExpired):
        raise PermanentError("PG_NATIVE_TOOL_UNAVAILABLE_OR_TIMEOUT") from None
    require(result.returncode == 0, "PG_NATIVE_OPERATION_FAILED")
    if output:
        require(len(result.stdout) <= 4096, "PG_NATIVE_RESPONSE_TOO_LARGE")
        return result.stdout.decode("utf-8").strip()
    return ""


def backup_postgres(service_file, destination, *, service="a2amq-backup", timeout=3600):
    destination = Path(destination).absolute()
    require(not os.path.lexists(destination) and destination.parent.is_dir(), "BACKUP_DESTINATION_EXISTS")
    with service_environment(service_file, service) as (env, _):
        fd, name = tempfile.mkstemp(prefix=".a2amq-pg-", dir=destination.parent)
        os.close(fd)
        temporary = Path(name)
        try:
            _run(["pg_dump", "--format=custom", "--no-password", "--no-owner", "--no-privileges",
                  "--file", str(temporary)], env, timeout)
            require(temporary.stat().st_size > 5, "EMPTY_PG_ARCHIVE")
            with temporary.open("rb") as stream:
                require(stream.read(5) == b"PGDMP", "INVALID_PG_ARCHIVE")
                os.fsync(stream.fileno())
            _run(["pg_restore", "--list", str(temporary)], env, min(timeout, 60))
            hashed = sha256(temporary)
            os.link(temporary, destination)
            parent = os.open(destination.parent, os.O_RDONLY)
            try:
                os.fsync(parent)
            finally:
                os.close(parent)
            return {"file": destination.name, "sha256": hashed, "bytes": destination.stat().st_size,
                    "scope": "entire-postgres-database", "method": "pg_dump-custom"}
        finally:
            temporary.unlink(missing_ok=True)


def restore_postgres(archive, service_file, *, expected_sha256, confirm_database,
                     service="a2amq-restore", timeout=3600):
    with verified_archive(archive, expected_sha256) as private:
        return _restore_verified(private, service_file, expected_sha256=expected_sha256,
            confirm_database=confirm_database, service=service, timeout=timeout)


def _restore_verified(archive, service_file, *, expected_sha256, confirm_database, service, timeout):
    with service_environment(service_file, service) as (env, database):
        require(database == confirm_database and database not in {"postgres", "template0", "template1"},
                "RESTORE_TARGET_NOT_CONFIRMED")
        query = """SELECT (SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
            WHERE n.nspname NOT IN ('pg_catalog','information_schema') AND n.nspname NOT LIKE 'pg_toast%')
          + (SELECT count(*) FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace
            WHERE n.nspname NOT IN ('pg_catalog','information_schema'))
          + (SELECT count(*) FROM pg_type t JOIN pg_namespace n ON n.oid=t.typnamespace
            WHERE n.nspname NOT IN ('pg_catalog','information_schema') AND n.nspname NOT LIKE 'pg_toast%')
          + (SELECT count(*) FROM pg_extension WHERE extname<>'plpgsql');"""
        count = _run(["psql", "-X", "--no-password", "--tuples-only", "--no-align",
                      "--set", "ON_ERROR_STOP=1", "--command", query], env, 30, output=True)
        require(count == "0", "RESTORE_TARGET_NOT_EMPTY")
        _run(["pg_restore", "--list", str(archive)], env, min(timeout, 60))
        _run(["pg_restore", "--no-password", "--exit-on-error", "--single-transaction",
              "--no-owner", "--no-privileges", "--dbname", "service=" + service, str(archive)], env, timeout)
        return {"restored": True, "sha256": expected_sha256, "targetDatabase": database,
                "method": "pg_restore-single-transaction", "requiresSeparateReconnection": True}
