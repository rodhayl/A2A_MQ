"""The resource gate must preserve evidence and reject unmeasured budgets."""
import importlib.util
import json
from pathlib import Path
import sys

import pytest


def load_probe():
    path = Path(__file__).resolve().parents[1] / "scripts/local_resource_probe.py"
    spec = importlib.util.spec_from_file_location("probe_under_test", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture
def probe_run(tmp_path, monkeypatch):
    probe = load_probe()
    monkeypatch.setattr(probe.time, "sleep", lambda *_: None)
    monkeypatch.setattr(probe, "_meminfo", lambda: {"MemAvailable": 2**30, "SwapTotal": 0, "SwapFree": 0})
    monkeypatch.setattr(probe, "_rss_bytes", lambda: 40 * 1024**2)
    monkeypatch.setattr(probe, "_peak_rss_bytes", lambda: 42 * 1024**2)
    report = tmp_path / "report.json"
    state = tmp_path / "state"
    args = ["probe", "--state-dir", str(state), "--json", str(report), "--tasks", "1",
            "--max-idle-cpu-percent", "1000000", "--max-import-seconds", "1000",
            "--max-smoke-seconds", "1000"]
    monkeypatch.setattr(sys, "argv", args)
    return probe, report, state, args


def test_missing_host_metrics_never_pass_or_create_a_database(probe_run, monkeypatch):
    probe, report, state, _ = probe_run
    monkeypatch.setattr(probe, "_meminfo", lambda: {})
    assert probe.main() == 2
    assert json.loads(report.read_text())["verdict"] == "INCOMPLETE_MEASUREMENT"
    assert not (state / "probe.sqlite").exists()


def test_zero_available_memory_is_not_treated_as_unknown_or_sufficient(probe_run, monkeypatch):
    probe, report, state, _ = probe_run
    monkeypatch.setattr(probe, "_meminfo", lambda: {"MemAvailable": 0, "SwapTotal": 0, "SwapFree": 0})
    assert probe.main() == 2
    assert json.loads(report.read_text())["verdict"] == "INCOMPLETE_HOST_HEADROOM"
    assert not (state / "probe.sqlite").exists()


def test_existing_report_is_preserved_and_work_does_not_start(probe_run):
    probe, report, state, _ = probe_run
    report.write_text('prior-evidence')
    with pytest.raises(SystemExit) as error:
        probe.main()
    assert error.value.code == 2
    assert report.read_text() == 'prior-evidence'
    assert not (state / "probe.sqlite").exists()


def test_report_cannot_replace_the_probe_database(probe_run):
    probe, _, state, args = probe_run
    args[args.index("--json") + 1] = str(state / "probe.sqlite")
    with pytest.raises(SystemExit) as error:
        probe.main()
    assert error.value.code == 2
    assert not (state / "probe.sqlite").exists()


@pytest.mark.parametrize("bad", ["nan", "inf", "-1"])
def test_invalid_budget_is_rejected_before_work(probe_run, bad):
    probe, report, state, args = probe_run
    args.append("--max-rss-mib=" + bad)
    with pytest.raises(SystemExit) as error:
        probe.main()
    assert error.value.code == 2
    assert not report.exists()
    assert not (state / "probe.sqlite").exists()


def test_peak_rss_in_excess_of_budget_fails(probe_run, monkeypatch):
    probe, report, _, _ = probe_run
    monkeypatch.setattr(probe, "_peak_rss_bytes", lambda: 193 * 1024**2)
    assert probe.main() == 3
    assert "rss" in json.loads(report.read_text())["failedChecks"]


def test_missing_final_metrics_are_incomplete_not_zero_usage(probe_run, monkeypatch):
    probe, report, _, _ = probe_run
    memory = iter([{"MemAvailable": 2**30, "SwapTotal": 0, "SwapFree": 0}, {}])
    monkeypatch.setattr(probe, "_meminfo", lambda: next(memory))
    assert probe.main() == 2
    assert json.loads(report.read_text())["verdict"] == "INCOMPLETE_MEASUREMENT"


def test_execution_failure_leaves_structured_failure_evidence(probe_run, monkeypatch):
    from a2a_mq.storage import SQLiteStore
    probe, report, _, _ = probe_run
    def fail(*a, **kw):
        raise RuntimeError("fixture error; do not expose exception details")
    monkeypatch.setattr(SQLiteStore, "claim", fail)
    assert probe.main() == 3
    evidence = json.loads(report.read_text())
    assert evidence["verdict"] == "FAIL_CORE_LIGHT"
    assert evidence["failedChecks"] == ["execution"]
    assert "fixture error" not in report.read_text()


def test_projection_verification_follows_all_pages():
    probe = load_probe()
    class Store:
        def __init__(self):
            self.calls = []
        def list_tasks(self, *, limit, after):
            self.calls.append((limit, after))
            if after is None:
                return [{"id": str(i)} for i in range(200)], "199"
            return [{"id": "200"}], None
    store = Store()
    probe._verify_task_count(store, 201)
    assert store.calls == [(200, None), (200, "199")]


@pytest.mark.parametrize("kind", ["missing", "duplicate", "stuck"])
def test_bad_projection_pages_never_satisfy_smoke(kind):
    probe = load_probe()
    class Store:
        def list_tasks(self, *, limit, after):
            if kind == "missing":
                return [], None
            if kind == "duplicate":
                return [{"id": "x"}, {"id": "x"}], None
            return [{"id": "x"}], "x"
    with pytest.raises(RuntimeError):
        probe._verify_task_count(Store(), 3)
