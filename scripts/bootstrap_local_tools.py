#!/usr/bin/env python3
"""Download pinned local native binaries into a private validation directory."""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import platform
import shutil
import stat
import tarfile
import tempfile
from urllib.request import Request, urlopen

DAPR_VERSION = "1.18.3"
NATS_VERSION = "2.15.0"

ASSETS = {
    "amd64": {
        "daprd": {
            "url": "https://github.com/dapr/dapr/releases/download/v1.18.3/daprd_linux_amd64.tar.gz",
            "sha256": "f5d1683b672d83bd88b8e79624d695779b72f856f7eb365ae1ea06354f2cb96c",
            "bytes": 63716263,
            "max_bytes": 80 * 1024 * 1024,
        },
        "nats-server": {
            "url": "https://github.com/nats-io/nats-server/releases/download/v2.15.0/nats-server-v2.15.0-linux-amd64.tar.gz",
            "sha256": "5d2c51caca950333aba84911df7d377f826f3a59ec36061c6539105084f65c92",
            "bytes": 7236657,
            "max_bytes": 20 * 1024 * 1024,
        },
        "placement": {
            "url": "https://github.com/dapr/dapr/releases/download/v1.18.3/placement_linux_amd64.tar.gz",
            "sha256": "1a697e769d9bb4b7eab883ee7d8776a243e2f2580bae6a001e8ff7fca12604ad",
            "bytes": 14382449,
            "max_bytes": 32 * 1024 * 1024,
        },
        "scheduler": {
            "url": "https://github.com/dapr/dapr/releases/download/v1.18.3/scheduler_linux_amd64.tar.gz",
            "sha256": "aa55eae2a8c38ef70e6d3f418a9d88e6b106d7fa56f8f4950c10e79ba7577e6f",
            "bytes": 18464040,
            "max_bytes": 32 * 1024 * 1024,
        },
    },
    "arm64": {
        "daprd": {
            "url": "https://github.com/dapr/dapr/releases/download/v1.18.3/daprd_linux_arm64.tar.gz",
            "sha256": "7fbffc5ffd18bb78cd7685c129d4f7ec337c8f92a0855790a6570571b2e62953",
            "bytes": 56814528,
            "max_bytes": 80 * 1024 * 1024,
        },
        "nats-server": {
            "url": "https://github.com/nats-io/nats-server/releases/download/v2.15.0/nats-server-v2.15.0-linux-arm64.tar.gz",
            "sha256": "cdc208f5a3f42963a52b6ab06ef65626bb870315dc936e26ba571780c6351112",
            "bytes": 6528127,
            "max_bytes": 20 * 1024 * 1024,
        },
        "placement": {
            "url": "https://github.com/dapr/dapr/releases/download/v1.18.3/placement_linux_arm64.tar.gz",
            "sha256": "be5fb143920f188109907e8930dffd504e808b3d40a1ab74e710a28cd5ec194f",
            "bytes": 12651936,
            "max_bytes": 32 * 1024 * 1024,
        },
        "scheduler": {
            "url": "https://github.com/dapr/dapr/releases/download/v1.18.3/scheduler_linux_arm64.tar.gz",
            "sha256": "49384218a253edeb9a95a551c886e25ea13011aec47ca2b8c4eedf747638492a",
            "bytes": 16329382,
            "max_bytes": 32 * 1024 * 1024,
        },
    },
}


def _arch() -> str:
    if platform.system() != "Linux":
        raise SystemExit("bootstrap supports Linux only")
    machine = platform.machine().lower()
    mapping = {"x86_64": "amd64", "amd64": "amd64", "aarch64": "arm64", "arm64": "arm64"}
    if machine not in mapping:
        raise SystemExit(f"unsupported architecture: {machine}")
    return mapping[machine]


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _download(url: str, destination: Path, expected: str,
              expected_bytes: int, max_bytes: int) -> int:
    request = Request(url, headers={"User-Agent": "a2a-mq-local-bootstrap/1"})
    digest = hashlib.sha256()
    written = 0
    with urlopen(request, timeout=60) as response, destination.open("xb") as out:
        length = response.headers.get("Content-Length")
        if length:
            declared = int(length)
            if declared != expected_bytes:
                raise RuntimeError("archive size mismatch")
            if declared > max_bytes:
                raise RuntimeError("download exceeds configured size limit")
        while True:
            chunk = response.read(1024 * 1024)
            if not chunk:
                break
            written += len(chunk)
            if written > max_bytes:
                raise RuntimeError("download exceeds configured size limit")
            digest.update(chunk)
            out.write(chunk)
        out.flush()
        os.fsync(out.fileno())
    if written != expected_bytes:
        destination.unlink(missing_ok=True)
        raise RuntimeError("archive size mismatch")
    if digest.hexdigest() != expected:
        destination.unlink(missing_ok=True)
        raise RuntimeError("archive checksum mismatch")
    return written


def _extract_binary(archive: Path, destination: Path, basename: str,
                    *, max_bytes: int = 256 * 1024 * 1024) -> None:
    matches = []
    with tarfile.open(archive, "r:gz") as bundle:
        for member in bundle.getmembers():
            if Path(member.name).name == basename:
                matches.append(member)
        if len(matches) != 1:
            raise RuntimeError(f"archive must contain exactly one {basename}")
        member = matches[0]
        if not member.isfile() or member.size <= 0 or member.size > max_bytes:
            raise RuntimeError("invalid binary archive member")
        source = bundle.extractfile(member)
        if source is None:
            raise RuntimeError("binary archive member unavailable")
        fd = os.open(destination, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o700)
        try:
            with os.fdopen(fd, "wb") as out:
                shutil.copyfileobj(source, out, length=1024 * 1024)
                out.flush()
                os.fsync(out.fileno())
        finally:
            source.close()
    destination.chmod(stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--workflow", action="store_true",
                        help="also bootstrap pinned Placement and Scheduler for the optional Workflow tier")
    args = parser.parse_args()
    root = args.output.resolve()
    if root.exists():
        raise SystemExit("output already exists; refusing to overwrite")
    # Validate the host before creating any persistent output. Unsupported hosts
    # must not leave an empty tools directory that blocks a corrected retry.
    arch = _arch()
    root.mkdir(parents=True, mode=0o700)
    try:
        bin_dir = root / "bin"
        bin_dir.mkdir(mode=0o700)
        selected = ["daprd", "nats-server"]
        if args.workflow:
            selected.extend(["placement", "scheduler"])
        manifest = {
            "platform": "linux", "arch": arch, "daprVersion": DAPR_VERSION,
            "daprFlavor": "allcomponents", "natsVersion": NATS_VERSION,
            "runtimeProfile": "local-workflow" if args.workflow else "local-lite",
            "binaries": {},
        }
        with tempfile.TemporaryDirectory(prefix="archives-", dir=root) as temp:
            temp_dir = Path(temp)
            for name in selected:
                spec = ASSETS[arch][name]
                archive = temp_dir / f"{name}.tar.gz"
                archive_bytes = _download(
                    spec["url"], archive, spec["sha256"], spec["bytes"], spec["max_bytes"]
                )
                binary = bin_dir / name
                _extract_binary(archive, binary, name)
                binary_bytes = binary.stat().st_size
                manifest["binaries"][name] = {
                    "archiveSha256": spec["sha256"],
                    "archiveBytes": archive_bytes,
                    "binarySha256": _sha256(binary),
                    "binaryBytes": binary_bytes,
                    "url": spec["url"],
                }
        manifest["archiveBytesTotal"] = sum(item["archiveBytes"] for item in manifest["binaries"].values())
        manifest["binaryBytesTotal"] = sum(item["binaryBytes"] for item in manifest["binaries"].values())
        manifest_path = root / "manifest.json"
        manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        manifest_path.chmod(0o600)
    except BaseException:
        shutil.rmtree(root, ignore_errors=True)
        raise
    print(json.dumps({
        "status": "ready", "bin": str(bin_dir), "manifest": str(root / "manifest.json")
    }, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
