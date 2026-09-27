#!/usr/bin/env python3
"""Backup/verify/restore the a2amq PostgreSQL schema using libpq service files.

Passwords never appear in argv. Configure PGSERVICEFILE/PGPASSFILE (or platform
secret mounts) and pass only a validated service name. Restore refuses a target
that already contains a2amq tables.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import tempfile

SERVICE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$")
TIMEOUT = 1800


class ArchiveError(RuntimeError):
    pass


def service(value: str) -> str:
    if not isinstance(value, str) or SERVICE.fullmatch(value) is None:
        raise ArchiveError("INVALID_SERVICE")
    return value


def binary(name: str) -> str:
    path = shutil.which(name)
    if path is None:
        raise ArchiveError(f"MISSING_{name.upper().replace('-', '_')}")
    return path


def run(argv: list[str], *, capture: bool = True) -> subprocess.CompletedProcess[str]:
    try:
        return subprocess.run(argv, check=True, text=True,
                              capture_output=capture, timeout=TIMEOUT)
    except (subprocess.SubprocessError, OSError) as exc:
        raise ArchiveError("POSTGRES_ARCHIVE_COMMAND_FAILED") from exc


def verify(path: Path) -> dict[str, object]:
    path = path.absolute()
    if not path.is_file() or path.is_symlink() or path.stat().st_size <= 0:
        raise ArchiveError("INVALID_ARCHIVE")
    listing = run([binary("pg_restore"), "--list", str(path)]).stdout
    if "SCHEMA" not in listing or "a2amq" not in listing:
        raise ArchiveError("ARCHIVE_SCHEMA_MISSING")
    return {"path": str(path), "bytes": path.stat().st_size,
            "sha256": hashlib.sha256(path.read_bytes()).hexdigest()}


def backup(service_name: str, destination: Path) -> dict[str, object]:
    service_name = service(service_name)
    destination = destination.absolute()
    if destination.exists() or not destination.parent.is_dir():
        raise ArchiveError("INVALID_ARCHIVE_PATH")
    fd, raw_temp = tempfile.mkstemp(prefix=".a2amq-pgdump-", dir=destination.parent)
    os.close(fd)
    temp = Path(raw_temp)
    temp.unlink()
    try:
        run([binary("pg_dump"), "--format=custom", "--no-owner", "--no-privileges",
             "--schema=a2amq", "--file", str(temp), "--dbname", f"service={service_name}"],
            capture=True)
        result = verify(temp)
        file_fd = os.open(temp, os.O_RDONLY)
        try:
            os.fsync(file_fd)
        finally:
            os.close(file_fd)
        os.replace(temp, destination)
        dir_fd = os.open(destination.parent, os.O_RDONLY)
        try:
            os.fsync(dir_fd)
        finally:
            os.close(dir_fd)
        result = verify(destination)
        result["service"] = service_name
        return result
    finally:
        temp.unlink(missing_ok=True)


def assert_empty_target(service_name: str) -> None:
    service_name = service(service_name)
    result = run([binary("psql"), "--no-psqlrc", "--tuples-only", "--no-align",
                  "--dbname", f"service={service_name}", "--command",
                  "SELECT count(*) FROM information_schema.tables WHERE table_schema='a2amq';"])
    try:
        count = int(result.stdout.strip())
    except ValueError as exc:
        raise ArchiveError("INVALID_TARGET_PROBE") from exc
    if count != 0:
        raise ArchiveError("RESTORE_TARGET_NOT_EMPTY")


def restore(service_name: str, source: Path, *, confirm_empty: bool) -> dict[str, object]:
    service_name = service(service_name)
    if confirm_empty is not True:
        raise ArchiveError("RESTORE_CONFIRMATION_REQUIRED")
    result = verify(source)
    assert_empty_target(service_name)
    run([binary("pg_restore"), "--exit-on-error", "--no-owner", "--no-privileges",
         "--dbname", f"service={service_name}", str(source.absolute())], capture=True)
    result["service"] = service_name
    result["restored"] = True
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    create = sub.add_parser("backup")
    create.add_argument("--service", required=True)
    create.add_argument("--output", required=True, type=Path)
    check = sub.add_parser("verify")
    check.add_argument("archive", type=Path)
    load = sub.add_parser("restore")
    load.add_argument("--service", required=True)
    load.add_argument("--confirm-empty", action="store_true")
    load.add_argument("archive", type=Path)
    args = parser.parse_args()
    try:
        if args.command == "backup":
            result = backup(args.service, args.output)
        elif args.command == "verify":
            result = verify(args.archive)
        else:
            result = restore(args.service, args.archive, confirm_empty=args.confirm_empty)
        print(json.dumps(result, sort_keys=True))
        return 0
    except ArchiveError as exc:
        print(json.dumps({"error": str(exc)}))
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
