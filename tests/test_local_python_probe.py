import importlib.util
from pathlib import Path


def _module():
    path = Path(__file__).resolve().parents[1] / "scripts/local_python_probe.py"
    spec = importlib.util.spec_from_file_location("local_python_probe", path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_python_probe_helpers_are_bounded(tmp_path):
    probe = _module()
    assert probe._mib(1024 * 1024) == 1.0
    assert probe._rss_bytes() >= 0
    assert probe._peak_rss_bytes() >= 0
    (tmp_path / "a").write_bytes(b"x" * 7)
    nested = tmp_path / "nested"
    nested.mkdir()
    (nested / "b").write_bytes(b"y" * 5)
    (nested / "c.tmp").write_bytes(b"z" * 10)
    assert probe._tree_bytes(tmp_path) == 22


def test_python_probe_rejects_missing_or_symlinked_state(tmp_path):
    import pytest

    probe = _module()
    with pytest.raises(probe.MeasurementUnavailable):
        probe._tree_bytes(tmp_path / "missing")

    state = tmp_path / "state"
    state.mkdir()
    target = tmp_path / "outside"
    target.write_bytes(b"x")
    (state / "link").symlink_to(target)
    with pytest.raises(probe.MeasurementUnavailable):
        probe._tree_bytes(state)


def test_python_probe_rejects_invalid_budget_before_running(tmp_path, monkeypatch):
    import pytest
    import sys

    probe = _module()
    report = tmp_path / "report.json"
    state = tmp_path / "state"
    monkeypatch.setattr(sys, "argv", [
        "probe", "--state-dir", str(state), "--json", str(report), "--max-rss-mib=inf",
    ])
    monkeypatch.setattr(probe, "_run", lambda *a: pytest.fail("probe ran"))
    with pytest.raises(SystemExit) as exc:
        probe.main()
    assert exc.value.code == 2
    assert not report.exists()
    assert not state.exists()


def test_python_probe_preserves_existing_report(tmp_path, monkeypatch):
    import pytest
    import sys

    probe = _module()
    report = tmp_path / "report.json"
    report.write_text("prior")
    state = tmp_path / "state"
    monkeypatch.setattr(sys, "argv", ["probe", "--state-dir", str(state), "--json", str(report)])
    monkeypatch.setattr(probe, "_run", lambda *a: pytest.fail("probe ran"))
    with pytest.raises(SystemExit) as exc:
        probe.main()
    assert exc.value.code == 2
    assert report.read_text() == "prior"
    assert not state.exists()


def test_python_probe_writes_sanitized_failure_evidence(tmp_path, monkeypatch):
    import json
    import sys

    probe = _module()
    report = tmp_path / "report.json"
    state = tmp_path / "state"
    monkeypatch.setattr(sys, "argv", ["probe", "--state-dir", str(state), "--json", str(report)])

    def fail(*_):
        raise RuntimeError("secret fixture detail")

    monkeypatch.setattr(probe, "_run", fail)
    assert probe.main() == 3
    evidence = json.loads(report.read_text())
    assert evidence["verdict"] == "FAIL_LOCAL_PYTHON"
    assert evidence["failedChecks"] == ["execution"]
    assert evidence["errorType"] == "RuntimeError"
    assert "secret fixture detail" not in report.read_text()
    assert report.stat().st_mode & 0o077 == 0


def test_python_probe_missing_measurement_is_incomplete(tmp_path, monkeypatch):
    import json
    import sys

    probe = _module()
    report = tmp_path / "report.json"
    state = tmp_path / "state"
    monkeypatch.setattr(sys, "argv", ["probe", "--state-dir", str(state), "--json", str(report)])

    def incomplete(*_):
        raise probe.MeasurementUnavailable("fixture details")

    monkeypatch.setattr(probe, "_run", incomplete)
    assert probe.main() == 2
    evidence = json.loads(report.read_text())
    assert evidence["verdict"] == "INCOMPLETE_LOCAL_PYTHON"
    assert evidence["failedChecks"] == ["measurement"]
    assert "fixture details" not in report.read_text()


async def test_python_probe_warms_owned_dapr_client_before_idle():
    from contextlib import asynccontextmanager
    from types import SimpleNamespace

    probe = _module()
    events = []

    class Bus:
        _owns_client = True

        def _http_client(self):
            events.append("warm")
            return object()

    @asynccontextmanager
    async def lifespan(app):
        events.append("start")
        yield
        events.append("stop")

    app = SimpleNamespace(
        state=SimpleNamespace(a2amq_bus=Bus()),
        router=SimpleNamespace(lifespan_context=lifespan),
    )
    cpu, rss = await probe._measure(app, 0)
    assert cpu >= 0 and rss >= 0
    assert events == ["start", "warm", "stop"]


async def test_python_probe_requires_owned_publisher_for_warm_measurement():
    from contextlib import asynccontextmanager
    from types import SimpleNamespace
    import pytest

    probe = _module()

    @asynccontextmanager
    async def lifespan(app):
        yield

    app = SimpleNamespace(
        state=SimpleNamespace(),
        router=SimpleNamespace(lifespan_context=lifespan),
    )
    with pytest.raises(probe.MeasurementUnavailable, match="owned Dapr publisher unavailable"):
        await probe._measure(app, 0)
