import hashlib
import importlib.util
from io import BytesIO
from pathlib import Path
import tarfile

import pytest


def _module():
    path = Path(__file__).resolve().parents[1] / "scripts/bootstrap_local_tools.py"
    spec = importlib.util.spec_from_file_location("bootstrap_local_tools", path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _tar(path, members):
    with tarfile.open(path, "w:gz") as bundle:
        for name, data in members:
            info = tarfile.TarInfo(name)
            info.size = len(data)
            info.mode = 0o755
            bundle.addfile(info, BytesIO(data))


def test_extracts_exact_regular_binary_privately(tmp_path):
    bootstrap = _module()
    archive = tmp_path / "archive.tar.gz"
    _tar(archive, [("release/daprd", b"binary")])
    output = tmp_path / "daprd"
    bootstrap._extract_binary(archive, output, "daprd")
    assert output.read_bytes() == b"binary"
    assert output.stat().st_mode & 0o077 == 0


def test_rejects_duplicate_binary_members(tmp_path):
    bootstrap = _module()
    archive = tmp_path / "archive.tar.gz"
    _tar(archive, [("x/daprd", b"x"), ("y/daprd", b"y")])
    with pytest.raises(RuntimeError, match="exactly one"):
        bootstrap._extract_binary(archive, tmp_path / "daprd", "daprd")


def test_sha256_helper(tmp_path):
    bootstrap = _module()
    value = tmp_path / "value"
    value.write_bytes(b"abc")
    assert bootstrap._sha256(value) == hashlib.sha256(b"abc").hexdigest()


def test_bootstrap_uses_allcomponents_daprd_assets_for_jetstream():
    bootstrap = _module()
    expected = {
        "amd64": ("f5d1683b672d83bd88b8e79624d695779b72f856f7eb365ae1ea06354f2cb96c", 63716263),
        "arm64": ("7fbffc5ffd18bb78cd7685c129d4f7ec337c8f92a0855790a6570571b2e62953", 56814528),
    }
    for arch, (digest, size) in expected.items():
        asset = bootstrap.ASSETS[arch]["daprd"]
        assert asset["sha256"] == digest
        assert asset["bytes"] == size
        assert asset["url"].endswith(f"daprd_linux_{arch}.tar.gz")
        assert "stablecomponents" not in asset["url"]


def test_bootstrap_pins_exact_nats_archive_sizes():
    bootstrap = _module()
    assert bootstrap.ASSETS["amd64"]["nats-server"]["bytes"] == 7236657
    assert bootstrap.ASSETS["arm64"]["nats-server"]["bytes"] == 6528127
