import importlib.util
from pathlib import Path


def _module():
    path = Path(__file__).resolve().parents[1] / "scripts/local_resource_probe.py"
    spec = importlib.util.spec_from_file_location("local_resource_probe", path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_resource_helpers_are_bounded_and_non_destructive(tmp_path):
    probe = _module()
    db = tmp_path / "probe.sqlite"
    db.write_bytes(b"x" * 1024)
    assert probe._mib(1024 * 1024) == 1.0
    assert probe._state_bytes(db) == 1024
    assert probe._disk_free(tmp_path) > 0
    assert probe._rss_bytes() >= 0


def test_resource_probe_smoke_verifies_task_projection(tmp_path, monkeypatch):
    import json
    import sys

    probe = _module()
    monkeypatch.setattr(probe.time, "sleep", lambda _: None)
    report = tmp_path / "report.json"
    state = tmp_path / "state"
    monkeypatch.setattr(sys, "argv", [
        "local_resource_probe.py",
        "--state-dir", str(state),
        "--json", str(report),
        "--tasks", "3",
        "--min-available-mib", "0",
        "--max-rss-mib", "1000000",
        "--max-idle-cpu-percent", "1000000000",
        "--max-import-seconds", "1000",
        "--max-smoke-seconds", "1000",
        "--max-state-mib", "1000",
    ])
    assert probe.main() == 0
    result = json.loads(report.read_text())
    assert result["verdict"] == "PASS_CORE_LIGHT"
    assert result["observed"]["tasksExecuted"] == 3
