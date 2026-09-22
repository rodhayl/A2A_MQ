#!/usr/bin/env python3
"""Verify exact native local-lite binary versions before resource acceptance."""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import re
import subprocess

EXPECTED_DAPRD = "1.18.3"
EXPECTED_NATS = "2.15.0"
SEMVER = re.compile(r"(?<![0-9])v?(\d+\.\d+\.\d+)(?![0-9])")


def _binary(path: str) -> str:
    resolved = Path(path).expanduser().resolve()
    if not resolved.is_file() or not os.access(resolved, os.X_OK):
        raise ValueError("native binary unavailable")
    return str(resolved)


def _version(path: str, arguments: tuple[str, ...]) -> tuple[str, str]:
    binary = _binary(path)
    try:
        completed = subprocess.run(
            [binary, *arguments],
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=5,
            check=False,
            env={"PATH": os.environ.get("PATH", ""), "HOME": os.environ.get("HOME", "")},
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        raise ValueError("native version probe failed") from exc
    output = completed.stdout[:4096].strip()
    if completed.returncode != 0:
        raise ValueError("native version probe returned nonzero")
    match = SEMVER.search(output)
    if match is None:
        raise ValueError("native version unavailable")
    return match.group(1), output


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--daprd", required=True)
    parser.add_argument("--nats-server", required=True)
    parser.add_argument("--json", dest="json_path", type=Path, required=True)
    args = parser.parse_args()

    report = args.json_path.absolute()
    report.parent.mkdir(parents=True, exist_ok=True)
    if report.exists() or report.is_symlink():
        raise SystemExit("evidence output already exists or is unsafe")

    try:
        daprd, daprd_raw = _version(args.daprd, ("--version",))
        nats, nats_raw = _version(args.nats_server, ("-v",))
        exact = daprd == EXPECTED_DAPRD and nats == EXPECTED_NATS
        result = {
            "verdict": "PASS_NATIVE_VERSION" if exact else "INCOMPLETE_NATIVE_VERSION",
            "expected": {"daprd": EXPECTED_DAPRD, "nats-server": EXPECTED_NATS},
            "observed": {"daprd": daprd, "nats-server": nats},
            "versionOutput": {"daprd": daprd_raw, "nats-server": nats_raw},
        }
        code = 0 if exact else 2
    except ValueError as exc:
        result = {
            "verdict": "INCOMPLETE_NATIVE_VERSION",
            "error": str(exc),
            "expected": {"daprd": EXPECTED_DAPRD, "nats-server": EXPECTED_NATS},
        }
        code = 2

    fd = os.open(report, os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0), 0o600)
    with os.fdopen(fd, "w", encoding="utf-8") as stream:
        stream.write(json.dumps(result, indent=2, sort_keys=True) + "\n")
        stream.flush()
        os.fsync(stream.fileno())
    print(json.dumps(result, sort_keys=True))
    return code


if __name__ == "__main__":
    raise SystemExit(main())
