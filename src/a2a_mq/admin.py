"""Explicit administrator commands; destructive operations require named inputs."""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import ssl

import httpx

from .sqlite_archive import backup_sqlite, restore_sqlite
from .pg_backup import backup_postgres, restore_postgres
from .configuration import https_origin
from .domain import PermanentError, load_json, require
from .vault import FileToken


def remote(url, token_file, *, method, path, body=None, ca_file=None):
    origin = https_origin(url)
    require(path.startswith("/operations/") and ".." not in path, "INVALID_ADMIN_PATH")
    context = ssl.create_default_context(cafile=ca_file)
    with httpx.Client(verify=context, timeout=35, trust_env=False, follow_redirects=False) as client:
        with client.stream(method, origin + path, json=body,
                           headers={"Authorization": "Bearer " + FileToken(token_file)()}) as response:
            require(200 <= response.status_code < 300, "ADMIN_REQUEST_REJECTED")
            raw = bytearray()
            for chunk in response.iter_bytes():
                require(len(raw) + len(chunk) <= 1048576, "ADMIN_RESPONSE_TOO_LARGE")
                raw.extend(chunk)
    return load_json(bytes(raw), 1048576)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    for operation in ("backup", "restore"):
        cmd = sub.add_parser(operation)
        cmd.add_argument("source", type=Path)
        cmd.add_argument("destination", type=Path)
        if operation == "restore":
            cmd.add_argument("--sha256", required=True)
    for operation in ("pg-backup", "pg-restore"):
        cmd = sub.add_parser(operation)
        cmd.add_argument("archive", type=Path)
        cmd.add_argument("--service-file", required=True)
        if operation == "pg-restore":
            cmd.add_argument("--sha256", required=True)
            cmd.add_argument("--confirm-database", required=True)
    for operation in ("status", "retire", "deadletters", "replay"):
        cmd = sub.add_parser(operation)
        cmd.add_argument("--url", required=True)
        cmd.add_argument("--token-file", required=True)
        cmd.add_argument("--ca-file")
        if operation == "retire":
            cmd.add_argument("--retention-seconds", type=int, default=604800)
            cmd.add_argument("--limit", type=int, default=50)
            cmd.add_argument("--execute", action="store_true")
        if operation == "replay":
            cmd.add_argument("--record-id", required=True)
            cmd.add_argument("--request-id", required=True)
    cmd = sub.add_parser("projection-retire")
    cmd.add_argument("--retention-seconds", type=int, default=604800)
    cmd.add_argument("--limit", type=int, default=50)
    cmd.add_argument("--execute", action="store_true")
    args = parser.parse_args()
    try:
        if args.command == "projection-retire":
            from .configuration import Settings, store
            from .projections import ProjectionPolicy
            settings = Settings.from_env()
            require(settings.role == "observer", "OBSERVER_SCOPE_REQUIRED")
            backend = store(settings)
            try:
                result = ProjectionPolicy(backend, max_records=settings.max_records).retire(
                    retention_seconds=args.retention_seconds, limit=args.limit, dry_run=not args.execute)
            finally:
                if hasattr(backend, "close"):
                    backend.close()
        elif args.command == "pg-backup":
            result = backup_postgres(args.service_file, args.archive)
        elif args.command == "pg-restore":
            result = restore_postgres(args.archive, args.service_file, expected_sha256=args.sha256,
                                      confirm_database=args.confirm_database)
        elif args.command == "backup":
            result = backup_sqlite(args.source, args.destination)
        elif args.command == "restore":
            result = restore_sqlite(args.source, args.destination, expected_sha256=args.sha256)
        else:
            method, path, body = "GET", "/operations/" + args.command, None
            if args.command == "retire":
                method, path = "POST", "/operations/retention"
                body = {"retentionSeconds": args.retention_seconds, "limit": args.limit, "dryRun": not args.execute}
            elif args.command == "replay":
                from urllib.parse import quote
                method = "POST"
                path = "/operations/deadletters/" + quote(args.record_id, safe="") + "/replay"
                body = {"requestId": args.request_id}
            result = remote(args.url, args.token_file, method=method, path=path, body=body, ca_file=args.ca_file)
        print(json.dumps(result, indent=2))
    except (PermanentError, httpx.HTTPError, OSError):
        print('{"error":"ADMIN_OPERATION_FAILED"}')
        raise SystemExit(1) from None


if __name__ == "__main__":
    main()
