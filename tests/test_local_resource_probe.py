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
