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


def test_verified_bootstrap_binds_manifest_to_private_run_binaries(tmp_path, monkeypatch):
    import hashlib
    import json

    check = _module()
    root = tmp_path / "tools"
    root.mkdir(mode=0o700)
    bin_dir = root / "bin"
    bin_dir.mkdir(mode=0o700)
    daprd = bin_dir / "daprd"
    nats = bin_dir / "nats-server"
    daprd.write_bytes(b"daprd fixture")
    nats.write_bytes(b"nats fixture")
    daprd.chmod(0o700)
    nats.chmod(0o700)
    monkeypatch.setattr(check, "_arch", lambda: "amd64")
    manifest = {
        "platform": "linux",
        "arch": "amd64",
        "daprVersion": check.EXPECTED_DAPRD,
        "daprFlavor": check.EXPECTED_DAPR_FLAVOR,
        "natsVersion": check.EXPECTED_NATS,
        "binaries": {
            "daprd": {
                "archiveSha256": check.EXPECTED_ARCHIVES["amd64"]["daprd"],
                "archiveBytes": check.EXPECTED_ARCHIVE_BYTES["amd64"]["daprd"],
                "binarySha256": hashlib.sha256(daprd.read_bytes()).hexdigest(),
                "binaryBytes": daprd.stat().st_size,
            },
            "nats-server": {
                "archiveSha256": check.EXPECTED_ARCHIVES["amd64"]["nats-server"],
                "archiveBytes": check.EXPECTED_ARCHIVE_BYTES["amd64"]["nats-server"],
                "binarySha256": hashlib.sha256(nats.read_bytes()).hexdigest(),
                "binaryBytes": nats.stat().st_size,
            },
        },
        "archiveBytesTotal": sum(check.EXPECTED_ARCHIVE_BYTES["amd64"].values()),
        "binaryBytesTotal": daprd.stat().st_size + nats.stat().st_size,
    }
    path = root / "manifest.json"
    path.write_text(json.dumps(manifest))
    path.chmod(0o600)
    assert check._verified_bootstrap(path, str(daprd), str(nats))["daprFlavor"] == "allcomponents"

    other = tmp_path / "other-daprd"
    other.write_bytes(daprd.read_bytes())
    other.chmod(0o700)
    try:
        check._verified_bootstrap(path, str(other), str(nats))
    except ValueError as exc:
        assert "bootstrap-owned" in str(exc)
    else:
        raise AssertionError("unrelated same-content daprd accepted")


def test_verified_bootstrap_rejects_tampered_binary_or_archive_provenance(tmp_path, monkeypatch):
    import hashlib
    import json

    check = _module()
    root = tmp_path / "tools"
    root.mkdir(mode=0o700)
    bin_dir = root / "bin"
    bin_dir.mkdir(mode=0o700)
    daprd = bin_dir / "daprd"
    nats = bin_dir / "nats-server"
    daprd.write_bytes(b"daprd")
    nats.write_bytes(b"nats")
    daprd.chmod(0o700)
    nats.chmod(0o700)
    monkeypatch.setattr(check, "_arch", lambda: "amd64")
    manifest = {
        "platform": "linux", "arch": "amd64",
        "archiveBytesTotal": sum(check.EXPECTED_ARCHIVE_BYTES["amd64"].values()),
        "binaryBytesTotal": daprd.stat().st_size + nats.stat().st_size,
        "binaries": {
            "daprd": {
                "archiveSha256": check.EXPECTED_ARCHIVES["amd64"]["daprd"],
                "archiveBytes": check.EXPECTED_ARCHIVE_BYTES["amd64"]["daprd"],
                "binarySha256": hashlib.sha256(daprd.read_bytes()).hexdigest(),
                "binaryBytes": daprd.stat().st_size,
            },
            "nats-server": {
                "archiveSha256": check.EXPECTED_ARCHIVES["amd64"]["nats-server"],
                "archiveBytes": check.EXPECTED_ARCHIVE_BYTES["amd64"]["nats-server"],
                "binarySha256": hashlib.sha256(nats.read_bytes()).hexdigest(),
                "binaryBytes": nats.stat().st_size,
            },
        },
    }
    path = root / "manifest.json"
    path.write_text(json.dumps(manifest))
    path.chmod(0o600)

    daprd.write_bytes(b"tampr")
    try:
        check._verified_bootstrap(path, str(daprd), str(nats))
    except ValueError as exc:
        assert "digest mismatch" in str(exc)
    else:
        raise AssertionError("tampered binary accepted")

    daprd.write_bytes(b"tampered")
    try:
        check._verified_bootstrap(path, str(daprd), str(nats))
    except ValueError as exc:
        assert "binary size provenance mismatch" in str(exc)
    else:
        raise AssertionError("tampered binary size accepted")

    daprd.write_bytes(b"daprd")
    manifest["binaries"]["daprd"]["archiveSha256"] = "0" * 64
    path.write_text(json.dumps(manifest))
    try:
        check._verified_bootstrap(path, str(daprd), str(nats))
    except ValueError as exc:
        assert "archive provenance mismatch" in str(exc)
    else:
        raise AssertionError("wrong archive provenance accepted")


def test_verified_bootstrap_rejects_wrong_archive_size_provenance(tmp_path, monkeypatch):
    import hashlib
    import json

    check = _module()
    root = tmp_path / "tools"
    root.mkdir(mode=0o700)
    bin_dir = root / "bin"
    bin_dir.mkdir(mode=0o700)
    daprd = bin_dir / "daprd"
    nats = bin_dir / "nats-server"
    daprd.write_bytes(b"daprd")
    nats.write_bytes(b"nats")
    daprd.chmod(0o700)
    nats.chmod(0o700)
    monkeypatch.setattr(check, "_arch", lambda: "amd64")
    manifest = {
        "platform": "linux", "arch": "amd64",
        "archiveBytesTotal": sum(check.EXPECTED_ARCHIVE_BYTES["amd64"].values()),
        "binaryBytesTotal": daprd.stat().st_size + nats.stat().st_size,
        "binaries": {
            "daprd": {
                "archiveSha256": check.EXPECTED_ARCHIVES["amd64"]["daprd"],
                "archiveBytes": check.EXPECTED_ARCHIVE_BYTES["amd64"]["daprd"] + 1,
                "binarySha256": hashlib.sha256(daprd.read_bytes()).hexdigest(),
                "binaryBytes": daprd.stat().st_size,
            },
            "nats-server": {
                "archiveSha256": check.EXPECTED_ARCHIVES["amd64"]["nats-server"],
                "archiveBytes": check.EXPECTED_ARCHIVE_BYTES["amd64"]["nats-server"],
                "binarySha256": hashlib.sha256(nats.read_bytes()).hexdigest(),
                "binaryBytes": nats.stat().st_size,
            },
        },
    }
    path = root / "manifest.json"
    path.write_text(json.dumps(manifest))
    path.chmod(0o600)
    with __import__("pytest").raises(ValueError, match="archive size provenance mismatch"):
        check._verified_bootstrap(path, str(daprd), str(nats))


def test_verified_workflow_bootstrap_binds_control_plane_binaries(tmp_path, monkeypatch):
    import hashlib
    import json

    check = _module()
    root = tmp_path / "tools"
    root.mkdir(mode=0o700)
    bin_dir = root / "bin"
    bin_dir.mkdir(mode=0o700)
    names = ("daprd", "nats-server", "placement", "scheduler")
    paths = {}
    for name in names:
        path = bin_dir / name
        path.write_bytes(name.encode())
        path.chmod(0o700)
        paths[name] = path
    monkeypatch.setattr(check, "_arch", lambda: "amd64")
    archive_sha = {**check.EXPECTED_ARCHIVES["amd64"], **check.EXPECTED_CONTROL_ARCHIVES["amd64"]}
    archive_bytes = {**check.EXPECTED_ARCHIVE_BYTES["amd64"],
                     **check.EXPECTED_CONTROL_ARCHIVE_BYTES["amd64"]}
    manifest = {
        "platform": "linux", "arch": "amd64", "runtimeProfile": "local-workflow",
        "daprVersion": check.EXPECTED_DAPRD, "daprFlavor": check.EXPECTED_DAPR_FLAVOR,
        "natsVersion": check.EXPECTED_NATS,
        "binaries": {
            name: {
                "archiveSha256": archive_sha[name],
                "archiveBytes": archive_bytes[name],
                "binarySha256": hashlib.sha256(paths[name].read_bytes()).hexdigest(),
                "binaryBytes": paths[name].stat().st_size,
            } for name in names
        },
        "archiveBytesTotal": sum(archive_bytes.values()),
        "binaryBytesTotal": sum(path.stat().st_size for path in paths.values()),
    }
    manifest_path = root / "manifest.json"
    manifest_path.write_text(json.dumps(manifest))
    manifest_path.chmod(0o600)

    verified = check._verified_bootstrap(
        manifest_path, str(paths["daprd"]), str(paths["nats-server"]),
        str(paths["placement"]), str(paths["scheduler"])
    )
    assert verified["runtimeProfile"] == "local-workflow"

    with __import__("pytest").raises(ValueError, match="supplied together"):
        check._verified_bootstrap(
            manifest_path, str(paths["daprd"]), str(paths["nats-server"]),
            str(paths["placement"]), None
        )
