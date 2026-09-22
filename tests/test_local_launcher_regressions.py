"""Sequential launcher boundary tests: real private files, no native services."""
import json
import os
from pathlib import Path
import subprocess

import pytest

from a2a_mq import local_runtime as lite
from a2a_mq import local_workflow_runtime as workflow
from a2a_mq.domain import PermanentError


@pytest.fixture
def profile(tmp_path):
    root = tmp_path / "profile with spaces"
    root.mkdir(mode=0o700)
    data = {"mode": "local-lite", "appId": lite.APP_ID, "appPort": 18000,
            "daprHttpPort": 3500, "daprGrpcPort": 50001, "tenant": "demo",
            "source": "local-worker", "issuer": "https://local-a2amq.invalid",
            "publicUrl": "http://127.0.0.1:18000", "mcp": False}
    (root / "profile.json").write_text(json.dumps(data))
    for name in ("app-api-token", "dapr-api-token", "nats-token", "nats.conf", "broker-plan.json"):
        (root / name).write_text("fixture-token-01234567890123456789")
        (root / name).chmod(0o600)
    return root, data


class BoundaryReached(RuntimeError):
    pass


@pytest.mark.parametrize("module,mode", [(lite, "local-lite"), (workflow, "local-workflow")])
def test_relative_profile_resolves_files_before_changing_child_cwd(profile, monkeypatch, module, mode):
    root, data = profile
    data["mode"] = mode
    (root / "profile.json").write_text(json.dumps(data))
    monkeypatch.chdir(root.parent)
    monkeypatch.setattr(os, "environ", os.environ.copy())
    monkeypatch.setattr(module, "_refresh_client_tokens", lambda *a: None)
    monkeypatch.setattr(module, "_native_binary", lambda name: "/fixture/" + name)
    def start(command, *, cwd, env):
        config = Path(command[command.index("-c") + 1])
        # This is exactly how the child would resolve its config argument.
        resolved = config if config.is_absolute() else Path(cwd) / config
        assert resolved.is_file(), str(resolved)
        assert config.is_absolute()
        raise BoundaryReached("config is accessible")
    monkeypatch.setattr(module.subprocess, "Popen", start)
    with pytest.raises(BoundaryReached, match="config is accessible"):
        module.run(Path(root.name))


def test_fixed_sidecar_endpoint_is_not_inherited_from_other_project(profile, monkeypatch):
    root, data = profile
    monkeypatch.setenv("DAPR_HTTP_ENDPOINT", "http://127.0.0.1:3999")
    app, _, _ = lite._runtime_environments(root, data)
    assert app["DAPR_HTTP_ENDPOINT"] == "http://127.0.0.1:3500"


@pytest.mark.parametrize("module,mode", [(lite, "local-lite"), (workflow, "local-workflow")])
def test_provisioning_is_time_bounded_and_nats_is_cleaned_up(profile, monkeypatch, module, mode):
    root, data = profile
    data["mode"] = mode
    (root / "profile.json").write_text(json.dumps(data))
    monkeypatch.setattr(os, "environ", os.environ.copy())
    monkeypatch.setattr(module, "_refresh_client_tokens", lambda *a: None)
    monkeypatch.setattr(module, "_native_binary", lambda name: "/fixture/" + name)
    child = object()
    stopped = []
    monkeypatch.setattr(module.subprocess, "Popen", lambda *a, **k: child)
    monkeypatch.setattr(module, "_wait_port", lambda *a, **k: None)
    monkeypatch.setattr(module, "_terminate", lambda p: stopped.append(p))
    def provision(command, **kwargs):
        assert 0 < kwargs.get("timeout", 0) <= 30
        raise subprocess.TimeoutExpired(command, kwargs["timeout"])
    monkeypatch.setattr(module.subprocess, "run", provision)
    with pytest.raises(subprocess.TimeoutExpired):
        module.run(root)
    assert child in stopped


@pytest.mark.parametrize("module", [lite, workflow])
def test_symlink_profile_is_rejected_before_starting_children(tmp_path, monkeypatch, module):
    target = tmp_path / "target"
    target.mkdir(mode=0o700)
    link = tmp_path / "link"
    link.symlink_to(target, target_is_directory=True)
    monkeypatch.setattr(module.subprocess, "Popen", lambda *a, **k: pytest.fail("child was started"))
    with pytest.raises(PermanentError, match="PROFILE_NOT_FOUND"):
        module.run(link)


@pytest.mark.parametrize("mode", ["local-lite", "local-workflow"])
def test_prepare_generates_readable_profile_and_final_paths(tmp_path, mode):
    root = tmp_path / "new profile"
    data = lite.prepare(root, mode=mode)
    assert lite._load_profile(root, mode) == data
    assert str(root / "nats") in (root / "nats.conf").read_text()
    assert not list(tmp_path.glob("*.tmp"))
    lite._refresh_client_tokens(root, data)
    assert (root / "client.jwt").read_text()
    assert (root / "client.jwt").stat().st_mode & 0o077 == 0


def test_native_binary_in_relative_path_becomes_absolute(tmp_path, monkeypatch):
    binary = tmp_path / "bin" / "daprd"
    binary.parent.mkdir()
    binary.write_text("fixture, not executed")
    binary.chmod(0o700)
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("PATH", "bin")
    assert lite._native_binary("daprd") == str(binary)


@pytest.mark.parametrize("module,mode", [(lite, "local-lite"), (workflow, "local-workflow")])
def test_port_preflight_happens_before_token_refresh_or_child_start(profile, monkeypatch, module, mode):
    root, data = profile
    data["mode"] = mode
    (root / "profile.json").write_text(json.dumps(data))
    monkeypatch.setattr(module, "_require_ports_free",
                        lambda ports: (_ for _ in ()).throw(BoundaryReached("ports checked")))
    monkeypatch.setattr(module, "_refresh_client_tokens",
                        lambda *a: pytest.fail("credentials were refreshed before port preflight"))
    monkeypatch.setattr(module.subprocess, "Popen",
                        lambda *a, **k: pytest.fail("child was started before port preflight"))
    with pytest.raises(BoundaryReached, match="ports checked"):
        module.run(root)
