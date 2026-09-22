import importlib.util
from pathlib import Path
import subprocess


def _module():
    path = Path(__file__).resolve().parents[1] / "scripts/check_local_native_versions.py"
    spec = importlib.util.spec_from_file_location("check_local_native_versions", path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_version_parser_accepts_daprd_and_nats_shapes(tmp_path, monkeypatch):
    check = _module()
    binary = tmp_path / "tool"
    binary.write_text("#!/bin/sh\n")
    binary.chmod(0o700)

    outputs = iter(("1.18.3\n", "nats-server: v2.15.0\n"))
    monkeypatch.setattr(
        check.subprocess,
        "run",
        lambda *args, **kwargs: subprocess.CompletedProcess(args[0], 0, next(outputs)),
    )
    assert check._version(str(binary), ("--version",))[0] == "1.18.3"
    assert check._version(str(binary), ("-v",))[0] == "2.15.0"


def test_version_parser_rejects_missing_semver(tmp_path, monkeypatch):
    check = _module()
    binary = tmp_path / "tool"
    binary.write_text("#!/bin/sh\n")
    binary.chmod(0o700)
    monkeypatch.setattr(
        check.subprocess,
        "run",
        lambda *args, **kwargs: subprocess.CompletedProcess(args[0], 0, "unknown"),
    )
    try:
        check._version(str(binary), ("--version",))
    except ValueError as exc:
        assert "version unavailable" in str(exc)
    else:
        raise AssertionError("missing semantic version accepted")


def test_version_parser_rejects_nonzero_command(tmp_path, monkeypatch):
    check = _module()
    binary = tmp_path / "tool"
    binary.write_text("#!/bin/sh\n")
    binary.chmod(0o700)
    monkeypatch.setattr(
        check.subprocess,
        "run",
        lambda *args, **kwargs: subprocess.CompletedProcess(args[0], 1, "1.18.3"),
    )
    try:
        check._version(str(binary), ("--version",))
    except ValueError as exc:
        assert "nonzero" in str(exc)
    else:
        raise AssertionError("nonzero version command accepted")
