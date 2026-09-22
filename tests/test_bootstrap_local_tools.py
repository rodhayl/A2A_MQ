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
