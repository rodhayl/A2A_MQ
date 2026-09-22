import importlib.util
import os
from pathlib import Path


def _module():
    path = Path(__file__).resolve().parents[1] / "scripts/local_stack_probe.py"
    spec = importlib.util.spec_from_file_location("local_stack_probe", path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_stack_probe_proc_helpers_and_tree_size(tmp_path):
    probe = _module()
    assert os.getpid() in probe._descendants(os.getpid())
    assert probe._rss(os.getpid()) > 0
    assert probe._cpu_ticks(os.getpid()) >= 0
    assert probe._cmdline(os.getpid())
    (tmp_path / "a").write_bytes(b"x" * 100)
    nested = tmp_path / "nested"
    nested.mkdir()
    (nested / "b").write_bytes(b"y" * 50)
    assert probe._tree_bytes(tmp_path) == 150


def test_stack_probe_has_distinct_lite_and_workflow_budgets():
    probe = _module()
    lite = probe._profile_policy({"mode": "local-lite"})
    workflow = probe._profile_policy({"mode": "local-workflow"})
    assert lite["expectedRoles"] == ("app", "daprd", "nats")
    assert workflow["expectedRoles"] == ("app", "daprd", "nats", "placement", "scheduler")
    assert workflow["maxTotalRssMiB"] > lite["maxTotalRssMiB"]
    assert workflow["maxStartupSeconds"] > lite["maxStartupSeconds"]


def test_stack_probe_profile_reader_rejects_symlink_and_oversize(tmp_path):
    probe = _module()
    root = tmp_path / "profile"
    root.mkdir()
    outside = tmp_path / "outside.json"
    outside.write_text('{"mode":"local-lite"}')
    (root / "profile.json").symlink_to(outside)
    try:
        probe._read_profile(root)
    except ValueError as exc:
        assert "unavailable" in str(exc)
    else:
        raise AssertionError("symlink profile accepted")

    (root / "profile.json").unlink()
    (root / "profile.json").write_bytes(b"x" * 16385)
    try:
        probe._read_profile(root)
    except ValueError as exc:
        assert "size limit" in str(exc)
    else:
        raise AssertionError("oversized profile accepted")


def test_stack_probe_profile_reader_accepts_bounded_object(tmp_path):
    probe = _module()
    root = tmp_path / "profile"
    root.mkdir()
    (root / "profile.json").write_text('{"mode":"local-lite"}')
    assert probe._read_profile(root) == {"mode": "local-lite"}


def test_stack_probe_process_identity_rejects_pid_reuse(monkeypatch):
    probe = _module()
    observed = {123: 10}
    monkeypatch.setattr(probe, "_start_ticks", lambda pid: observed.get(pid))
    assert probe._same_process(123, 10) is True
    observed[123] = 11
    assert probe._same_process(123, 10) is False


def test_stack_probe_collects_only_live_process_identities(monkeypatch):
    probe = _module()
    starts = {1: 100, 2: None, 3: 300}
    monkeypatch.setattr(probe, "_start_ticks", lambda pid: starts[pid])
    assert probe._identities([1, 2, 3]) == {1: 100, 3: 300}


def test_stack_probe_all_alive_requires_root_and_original_children(monkeypatch):
    probe = _module()

    class Process:
        def __init__(self, code=None):
            self.code = code
        def poll(self):
            return self.code

    monkeypatch.setattr(probe, "_same_process", lambda pid, started: pid != 2)
    assert probe._all_alive(Process(), {1: 10, 2: 20}) is False
    assert probe._all_alive(Process(code=1), {1: 10}) is False

    monkeypatch.setattr(probe, "_same_process", lambda pid, started: True)
    assert probe._all_alive(Process(), {1: 10, 2: 20}) is True


def test_stack_probe_samples_aggregate_descendant_rss(monkeypatch):
    probe = _module()
    monkeypatch.setattr(probe, "_descendants", lambda root: [root, 2, 3])
    rss = {1: 100, 2: 200, 3: 300}
    monkeypatch.setattr(probe, "_rss", lambda pid: rss[pid])
    assert probe._sample_total_rss(1) == (600, 3)


def test_stack_probe_policy_requires_stable_idle_process_count():
    probe = _module()
    for mode in ("local-lite", "local-workflow"):
        policy = probe._profile_policy({"mode": mode})
        assert len(policy["expectedRoles"]) in {3, 5}


def test_stack_probe_post_shutdown_state_can_fail_budget():
    probe = _module()
    result = {
        "verdict": "PASS_LOCAL_LITE",
        "observed": {"runtimeStateMiB": 1.0},
        "failedChecks": [],
    }
    probe._apply_post_shutdown_state(result, 2 * 1024 * 1024, 4.0, "LOCAL_LITE")
    assert result["verdict"] == "PASS_LOCAL_LITE"
    assert result["observed"]["postShutdownStateMiB"] == 2.0

    probe._apply_post_shutdown_state(result, 5 * 1024 * 1024, 4.0, "LOCAL_LITE")
    assert result["verdict"] == "FAIL_LOCAL_LITE"
    assert "postShutdownState" in result["failedChecks"]


def test_stack_probe_writes_failure_when_root_process_cannot_start(tmp_path, monkeypatch):
    import json
    import sys

    probe = _module()
    profile = tmp_path / "profile"
    profile.mkdir()
    (profile / "profile.json").write_text('{"mode":"local-lite"}')
    output = tmp_path / "result.json"
    log = tmp_path / "result.log"

    def fail_start(*args, **kwargs):
        raise OSError("injected start failure")

    monkeypatch.setattr(probe.subprocess, "Popen", fail_start)
    monkeypatch.setattr(sys, "argv", [
        "local_stack_probe.py",
        "--profile", str(profile),
        "--json", str(output),
        "--log", str(log),
    ])
    assert probe.main() == 3
    result = json.loads(output.read_text())
    assert result["verdict"] == "FAIL_LOCAL_LITE"
    assert result["failedChecks"] == ["processStart"]
    assert result["error"] == "OSError"


def test_role_aggregation_preserves_per_role_costs():
    probe = _module()
    roles = {"app": [1], "daprd": [2], "nats": [3], "placement": [], "scheduler": []}
    assert probe._by_role(roles, {1: 10, 2: 20, 3: 30}) == {
        "app": 10, "daprd": 20, "nats": 30, "placement": 0, "scheduler": 0,
    }
