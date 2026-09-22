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
    assert probe._tree_bytes(tmp_path) == 12
