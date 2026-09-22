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


def test_bootstrap_manifest_requires_allcomponents_provenance(tmp_path):
    import json

    check = _module()
    manifest = tmp_path / "manifest.json"
    manifest.write_text(json.dumps({
        "daprVersion": check.EXPECTED_DAPRD,
        "daprFlavor": "allcomponents",
        "natsVersion": check.EXPECTED_NATS,
    }))
    assert check._bootstrap_manifest(manifest)["daprFlavor"] == "allcomponents"

    manifest.write_text(json.dumps({
        "daprVersion": check.EXPECTED_DAPRD,
        "daprFlavor": "stablecomponents",
        "natsVersion": check.EXPECTED_NATS,
    }))
    assert check._bootstrap_manifest(manifest)["daprFlavor"] == "stablecomponents"


def test_bootstrap_manifest_rejects_symlink_and_oversize(tmp_path):
    check = _module()
    target = tmp_path / "target.json"
    target.write_text("{}")
    link = tmp_path / "manifest.json"
    link.symlink_to(target)
    try:
        check._bootstrap_manifest(link)
    except ValueError as exc:
        assert "unsafe" in str(exc)
    else:
        raise AssertionError("symlink manifest accepted")

    link.unlink()
    link.write_text("x" * 16385)
    try:
        check._bootstrap_manifest(link)
    except ValueError as exc:
        assert "unsafe" in str(exc)
    else:
        raise AssertionError("oversized manifest accepted")
