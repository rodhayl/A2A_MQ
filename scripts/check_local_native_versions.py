#!/usr/bin/env python3
"""Verify exact native local-lite binary versions before resource acceptance."""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import platform
import re
import subprocess
import stat

EXPECTED_DAPRD = "1.18.3"
EXPECTED_NATS = "2.15.0"
EXPECTED_DAPR_FLAVOR = "allcomponents"
EXPECTED_ARCHIVES = {
    "amd64": {
        "daprd": "f5d1683b672d83bd88b8e79624d695779b72f856f7eb365ae1ea06354f2cb96c",
        "nats-server": "5d2c51caca950333aba84911df7d377f826f3a59ec36061c6539105084f65c92",
    },
    "arm64": {
        "daprd": "7fbffc5ffd18bb78cd7685c129d4f7ec337c8f92a0855790a6570571b2e62953",
        "nats-server": "cdc208f5a3f42963a52b6ab06ef65626bb870315dc936e26ba571780c6351112",
    },
}
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


def _bootstrap_manifest(path: Path) -> dict:
    """Read bounded private bootstrap provenance without following symlinks."""
    if path.is_symlink():
        raise ValueError("bootstrap manifest unavailable or unsafe")
    try:
        flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_NONBLOCK", 0)
        fd = os.open(path, flags)
        with os.fdopen(fd, "rb") as stream:
            info = os.fstat(stream.fileno())
            if not stat.S_ISREG(info.st_mode) or info.st_size <= 0 or info.st_size > 16384:
                raise ValueError("bootstrap manifest unavailable or unsafe")
            raw = stream.read(16385)
    except OSError as exc:
        raise ValueError("bootstrap manifest unavailable or unsafe") from exc
    if len(raw) > 16384:
        raise ValueError("bootstrap manifest unavailable or unsafe")
    try:
        value = json.loads(raw.decode("utf-8", errors="strict"))
    except (UnicodeError, ValueError):
        raise ValueError("bootstrap manifest invalid") from None
    if not isinstance(value, dict):
        raise ValueError("bootstrap manifest invalid")
    return value


def _arch() -> str:
    if platform.system() != "Linux":
        raise ValueError("bootstrap provenance supports Linux only")
    machine = platform.machine().lower()
    mapping = {"x86_64": "amd64", "amd64": "amd64", "aarch64": "arm64", "arm64": "arm64"}
    try:
        return mapping[machine]
    except KeyError:
        raise ValueError("unsupported bootstrap architecture") from None


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    try:
        with path.open("rb") as stream:
            for chunk in iter(lambda: stream.read(1024 * 1024), b""):
                digest.update(chunk)
    except OSError as exc:
        raise ValueError("native binary hash unavailable") from exc
    return digest.hexdigest()


def _verified_bootstrap(path: Path, daprd: str, nats_server: str) -> dict:
    manifest = _bootstrap_manifest(path)
    arch = _arch()
    root = path.absolute().parent
    bin_dir = root / "bin"
    if root.is_symlink() or bin_dir.is_symlink() or not bin_dir.is_dir():
        raise ValueError("bootstrap root unavailable or unsafe")
    try:
        if root.stat().st_mode & 0o077 or bin_dir.stat().st_mode & 0o077:
            raise ValueError("bootstrap root permissions are not private")
    except OSError as exc:
        raise ValueError("bootstrap root unavailable or unsafe") from exc

    expected_paths = {
        "daprd": bin_dir / "daprd",
        "nats-server": bin_dir / "nats-server",
    }
    provided = {"daprd": Path(daprd).expanduser().absolute(),
                "nats-server": Path(nats_server).expanduser().absolute()}
    binaries = manifest.get("binaries")
    if (manifest.get("platform") != "linux" or manifest.get("arch") != arch
            or not isinstance(binaries, dict)):
        raise ValueError("bootstrap provenance mismatch")

    for name, expected_path in expected_paths.items():
        actual_path = provided[name]
        if actual_path != expected_path or actual_path.is_symlink():
            raise ValueError("native binary is not the bootstrap-owned executable")
        record = binaries.get(name)
        if not isinstance(record, dict):
            raise ValueError("bootstrap binary provenance missing")
        if record.get("archiveSha256") != EXPECTED_ARCHIVES[arch][name]:
            raise ValueError("bootstrap archive provenance mismatch")
        binary_sha = record.get("binarySha256")
        if not isinstance(binary_sha, str) or not re.fullmatch(r"[0-9a-f]{64}", binary_sha):
            raise ValueError("bootstrap binary digest invalid")
        if _sha256(actual_path) != binary_sha:
            raise ValueError("bootstrap binary digest mismatch")
    return manifest


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--daprd", required=True)
    parser.add_argument("--nats-server", required=True)
    parser.add_argument("--bootstrap-manifest", type=Path, required=True)
    parser.add_argument("--json", dest="json_path", type=Path, required=True)
    args = parser.parse_args()

    report = args.json_path.absolute()
    report.parent.mkdir(parents=True, exist_ok=True)
    if report.exists() or report.is_symlink():
        raise SystemExit("evidence output already exists or is unsafe")

    try:
        daprd, daprd_raw = _version(args.daprd, ("--version",))
        nats, nats_raw = _version(args.nats_server, ("-v",))
        manifest = _verified_bootstrap(args.bootstrap_manifest, args.daprd, args.nats_server)
        exact = (daprd == EXPECTED_DAPRD and nats == EXPECTED_NATS
                 and manifest.get("daprVersion") == EXPECTED_DAPRD
                 and manifest.get("natsVersion") == EXPECTED_NATS
                 and manifest.get("daprFlavor") == EXPECTED_DAPR_FLAVOR)
        result = {
            "verdict": "PASS_NATIVE_VERSION" if exact else "INCOMPLETE_NATIVE_VERSION",
            "expected": {"daprd": EXPECTED_DAPRD, "daprFlavor": EXPECTED_DAPR_FLAVOR,
                         "nats-server": EXPECTED_NATS},
            "observed": {"daprd": daprd, "daprFlavor": manifest.get("daprFlavor"),
                         "nats-server": nats},
            "versionOutput": {"daprd": daprd_raw, "nats-server": nats_raw},
        }
        code = 0 if exact else 2
    except ValueError as exc:
        result = {
            "verdict": "INCOMPLETE_NATIVE_VERSION",
            "error": str(exc),
            "expected": {"daprd": EXPECTED_DAPRD, "daprFlavor": EXPECTED_DAPR_FLAVOR,
                         "nats-server": EXPECTED_NATS},
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
