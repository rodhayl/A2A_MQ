"""Harness contracts; the actual native demo is a separate, recorded live check."""
import importlib.util
from pathlib import Path
import signal
from types import SimpleNamespace

import pytest


@pytest.fixture
def demo():
    path = Path(__file__).resolve().parents[1] / "scripts/portfolio_demo.py"
    spec = importlib.util.spec_from_file_location("portfolio_demo_test", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_measurement_nearest_rank(demo):
    assert demo.percentile(list(range(1, 21)), .95) == 19
    assert demo.percentile([5], .95) == 5


def test_synthetic_request_uses_stable_message_identity(demo):
    assert demo.request("same") == demo.request("same")
    assert demo.request("same") != demo.request("same", "changed")
    assert demo.request("same")["metadata"]["a2amq"]["transform"] == "upper"


def test_cleanup_targets_only_owned_group_even_after_app_crash(demo, tmp_path, monkeypatch):
    calls = []
    monkeypatch.setattr(demo.os, "killpg", lambda pid, sig: calls.append((pid, sig)))
    monkeypatch.setattr(demo.time, "sleep", lambda _: None)
    stack = demo.OwnedStack(tmp_path, tmp_path)
    stack.process = SimpleNamespace(pid=7654321, wait=lambda timeout: -signal.SIGKILL)
    stack.stop()
    assert calls == [(7654321, signal.SIGTERM), (7654321, signal.SIGKILL)]
    assert stack.process is None
    stack.stop()
    assert len(calls) == 2


def test_existing_profile_is_never_overwritten(demo, tmp_path, monkeypatch):
    profile = tmp_path / "profile"
    profile.mkdir()
    marker = profile / "existing"
    marker.write_text("preserve")
    monkeypatch.setattr(demo.sys, "argv", ["demo", "--work-dir", str(profile), "--tools-bin", str(tmp_path),
                                         "--output", str(tmp_path / "report.json")])
    with pytest.raises(SystemExit) as error:
        demo.main()
    assert error.value.code == 2
    assert marker.read_text() == "preserve"
    assert not (tmp_path / "report.json").exists()


def test_source_digest_excludes_private_logs_and_changes_with_code(demo, tmp_path):
    (tmp_path / "src").mkdir()
    (tmp_path / "scripts").mkdir()
    source = tmp_path / "src/code.py"
    source.write_text("one")
    (tmp_path / "pyproject.toml").write_text("metadata")
    (tmp_path / "scripts/portfolio_demo.py").write_text("demo")
    before = demo.source_digest(tmp_path)
    (tmp_path / "private.log").write_text("should not enter digest")
    assert before == demo.source_digest(tmp_path)
    source.write_text("two")
    assert before != demo.source_digest(tmp_path)


def test_sigterm_enters_normal_cleanup_path(demo):
    with pytest.raises(KeyboardInterrupt):
        demo.stop_signal(signal.SIGTERM, None)
