"""Adversarial probe tests use process/metric fixtures, never Dapr or NATS."""
import importlib.util
import json
from pathlib import Path
import shlex
import sys

import pytest


def load_probe():
    path = Path(__file__).resolve().parents[1] / "scripts/local_stack_probe.py"
    spec = importlib.util.spec_from_file_location("stack_probe_under_test", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_roles_use_executable_not_directory_or_argument_substrings():
    probe = load_probe()
    commands = {1: "python -m launcher --profile /tmp/daprd/scheduler",
                2: shlex.join(["/tmp/nats-server dir/daprd", "--resources-path", "/tmp/scheduler"]),
                3: shlex.join(["/tmp/daprd dir/nats-server", "-c", "/tmp/placement.conf"])}
    roles = probe._roles(1, commands)
    assert roles == {"app": [1], "daprd": [2], "nats": [3], "placement": [], "scheduler": []}


@pytest.fixture
def measured_stack(tmp_path, monkeypatch):
    probe = load_probe()
    root = tmp_path / "profile"
    root.mkdir()
    (root / "profile.json").write_text('{"mode":"local-lite"}')
    report = tmp_path / "report.json"
    monkeypatch.setattr(sys, "argv", ["probe", "--profile", str(root), "--json", str(report),
                                     "--log", str(tmp_path / "log.txt")])
    state = {"finished": False, "exited": None, "missing_rss": None, "missing_cpu": None, "added": False}
    class Process:
        pid = 1001
        def poll(self):
            return 1 if state["finished"] and state["exited"] == self.pid else None
    monkeypatch.setattr(probe.subprocess, "Popen", lambda *a, **kw: Process())
    monkeypatch.setattr(probe, "_ready", lambda *a: (3 * 1024**2, 3))
    def descendants(pid):
        pids = [1001, 1002, 1003] + ([1004] if state["finished"] and state["added"] else [])
        return [p for p in pids if not state["finished"] or p != state["exited"]]
    monkeypatch.setattr(probe, "_descendants", descendants)
    monkeypatch.setattr(probe, "_start_ticks", lambda pid: None if state["finished"] and state["exited"] == pid else pid)
    monkeypatch.setattr(probe, "_cmdline", lambda pid: {1001: "python -m launcher", 1002: "/bin/daprd",
                                                       1003: "/bin/nats-server", 1004: "/bin/extra"}[pid])
    monkeypatch.setattr(probe, "_rss", lambda pid: 0 if pid == state["missing_rss"] else 1024**2)
    monkeypatch.setattr(probe, "_cpu_ticks", lambda pid: None if pid == state["missing_cpu"] else 0)
    clock = [0.0]
    monkeypatch.setattr(probe.time, "monotonic", lambda: clock[0])
    def sleep(seconds):
        clock[0] += seconds
        state.update(finished=True)
    monkeypatch.setattr(probe.time, "sleep", sleep)
    monkeypatch.setattr(probe, "_tree_bytes", lambda *_: 1024)
    cleaned = []
    monkeypatch.setattr(probe, "_terminate", lambda proc, children: cleaned.append(proc.pid))
    return probe, report, state, cleaned


@pytest.mark.parametrize("pid", [1001, 1002, 1003])
def test_a_dead_process_cannot_make_the_stack_look_cheaper(measured_stack, pid):
    probe, report, state, cleaned = measured_stack
    state["exited"] = pid
    assert probe.main() == 3
    assert not json.loads(report.read_text())["verdict"].startswith("PASS_")
    assert cleaned == [1001]


def test_zero_rss_is_missing_evidence_not_free_memory(measured_stack):
    probe, report, state, _ = measured_stack
    state["missing_rss"] = 1002
    assert probe.main() == 2
    assert json.loads(report.read_text())["verdict"] == "INCOMPLETE_MEASUREMENT"


def test_missing_cpu_data_cannot_pass_idle_budget(measured_stack):
    probe, report, state, _ = measured_stack
    state["missing_cpu"] = 1002
    assert probe.main() == 2
    assert json.loads(report.read_text())["verdict"] == "INCOMPLETE_MEASUREMENT"


def test_new_descendant_during_measurement_is_not_silently_ignored(measured_stack):
    probe, report, state, _ = measured_stack
    state["added"] = True
    assert probe.main() == 3
    assert not json.loads(report.read_text())["verdict"].startswith("PASS_")


def test_stable_complete_measurements_pass(measured_stack):
    probe, report, _, cleaned = measured_stack
    assert probe.main() == 0
    assert json.loads(report.read_text())["verdict"] == "PASS_LOCAL_LITE"
    assert cleaned == [1001]


@pytest.mark.parametrize("flag,value", [("--idle-seconds", "0"), ("--idle-seconds", "nan"),
                                       ("--startup-timeout", "inf"), ("--max-total-rss-mib", "-1")])
def test_invalid_probe_budget_stops_before_process_creation(measured_stack, monkeypatch, flag, value):
    probe, report, _, _ = measured_stack
    sys.argv.append(flag + "=" + value)
    monkeypatch.setattr(probe.subprocess, "Popen", lambda *a, **k: pytest.fail("started a process"))
    with pytest.raises(SystemExit) as exc:
        probe.main()
    assert exc.value.code == 2
    assert not report.exists()


def test_json_and_log_paths_must_be_distinct(measured_stack, monkeypatch):
    probe, report, _, _ = measured_stack
    sys.argv[sys.argv.index("--log") + 1] = str(report)
    monkeypatch.setattr(probe.subprocess, "Popen", lambda *a, **k: pytest.fail("started a process"))
    with pytest.raises(SystemExit) as exc:
        probe.main()
    assert exc.value.code == 2


def test_missing_state_is_unknown_not_zero_bytes(tmp_path):
    probe = load_probe()
    with pytest.raises(probe.MeasurementUnavailable):
        probe._tree_bytes(tmp_path / "missing")


def test_symlink_state_cannot_hide_unaccounted_bytes(tmp_path):
    probe = load_probe()
    state = tmp_path / "state"
    state.mkdir()
    other = tmp_path / "data"
    other.write_bytes(b"x" * 100)
    (state / "link").symlink_to(other)
    with pytest.raises(probe.MeasurementUnavailable):
        probe._tree_bytes(state)


def test_startup_peak_is_not_lost_when_idle_usage_is_low(measured_stack, monkeypatch):
    probe, report, _, _ = measured_stack
    monkeypatch.setattr(probe, "_ready", lambda *a: (400 * 1024**2, 4))
    assert probe.main() == 3
    result = json.loads(report.read_text())
    assert "rss" in result["failedChecks"]
    assert result["observed"]["startupPeakRssMiB"] == 400


def test_flushed_state_is_checked_after_cleanup(measured_stack, monkeypatch):
    probe, report, _, _ = measured_stack
    shutdown = [False]
    monkeypatch.setattr(probe, "_terminate", lambda *a: shutdown.__setitem__(0, True))
    monkeypatch.setattr(probe, "_tree_bytes", lambda *a: 20 * 1024**2 if shutdown[0] else 1024)
    assert probe.main() == 3
    assert "postShutdownState" in json.loads(report.read_text())["failedChecks"]


def test_missing_startup_measurement_still_captures_children_for_cleanup(measured_stack, monkeypatch):
    probe, report, _, _ = measured_stack
    def missing(*args):
        raise probe.MeasurementUnavailable("unavailable startup metric")
    monkeypatch.setattr(probe, "_ready", missing)
    children_seen = []
    monkeypatch.setattr(probe, "_terminate", lambda p, children: children_seen.append(children))
    assert probe.main() == 2
    assert children_seen == [{1001: 1001, 1002: 1002, 1003: 1003}]
    assert json.loads(report.read_text())["verdict"] == "INCOMPLETE_MEASUREMENT"
