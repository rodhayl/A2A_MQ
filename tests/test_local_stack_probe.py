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
