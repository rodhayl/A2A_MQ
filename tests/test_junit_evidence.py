"""Required gates fail closed on missing, inconsistent and skipped evidence."""
from __future__ import annotations

import importlib.util
from pathlib import Path
import subprocess
import sys

import pytest

SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "assert_junit_complete.py"
spec = importlib.util.spec_from_file_location("junit_evidence", SCRIPT)
assert spec is not None and spec.loader is not None
junit = importlib.util.module_from_spec(spec)
spec.loader.exec_module(junit)


def xml(tmp_path: Path, text: str) -> Path:
    path = tmp_path / "result.xml"
    path.write_text(text, encoding="utf-8")
    return path


@pytest.mark.parametrize("document", [
    '<testsuite tests="1"><testcase name="ok"/></testsuite>',
    '<testsuites tests="1"><testsuite tests="1" failures="0" errors="0" skipped="0">'
    '<testcase name="ok" classname="suite"/></testsuite></testsuites>',
    '<testsuites><testsuite name="outer"><testsuite name="inner">'
    '<testcase name="ok"/></testsuite></testsuite></testsuites>',
    '<testsuite xmlns="urn:junit" tests="1"><testcase name="ok"/></testsuite>',
    '\ufeff<?xml version="1.0" encoding="UTF-8"?><testsuite><testcase name="ok"/></testsuite>',
])
def test_complete_documents(tmp_path, document):
    assert junit.require_complete(xml(tmp_path, document)) == (1, 0, 0, 0)


@pytest.mark.parametrize("outcome,index", [("failure", 1), ("error", 2), ("skipped", 3)])
def test_all_nonpasses_rejected(tmp_path, outcome, index):
    path = xml(tmp_path, f'<testsuite><testcase name="bad"><{outcome}/></testcase></testsuite>')
    assert junit.counts(path)[index] == 1
    with pytest.raises(junit.EvidenceError):
        junit.require_complete(path)


@pytest.mark.parametrize("document", [
    '<testsuite/>',
    '<testsuites/>',
    '<testsuite tests="1"/>',
    '<testsuite failures="1"><testcase name="ok"/></testsuite>',
    '<testsuite errors="1"><testcase name="ok"/></testsuite>',
    '<testsuite skipped="1"><testcase name="ok"/></testsuite>',
    '<testsuite tests="-1"><testcase name="ok"/></testsuite>',
    '<testsuite tests="1.0"><testcase name="ok"/></testsuite>',
    '<testsuite tests="nan"><testcase name="ok"/></testsuite>',
    '<testsuite><error message="collection failed"/><testcase name="ok"/></testsuite>',
    '<testsuite><failure/><testcase name="ok"/></testsuite>',
    '<testsuite><skipped/><testcase name="ok"/></testsuite>',
    '<testsuites><testcase name="ok"/></testsuites>',
    '<other><testcase name="ok"/></other>',
    '<testsuite><testcase/></testsuite>',
    '<testsuite><testcase name="same"/><testcase name="same"/></testsuite>',
    '<testsuite><testcase name="x"><error/><failure/></testcase></testsuite>',
    '<testsuite><testcase name="outer"><testcase name="inner"/></testcase></testsuite>',
    '<testsuite><testcase name="ok"></testsuite>',
    '<!DOCTYPE testsuite><testsuite><testcase name="ok"/></testsuite>',
    '<?xml version="1.0" encoding="UTF-16"?><testsuite><testcase name="ok"/></testsuite>',
    '<testsuite>\x00<testcase name="ok"/></testsuite>',
])
def test_invalid_evidence_rejected(tmp_path, document):
    with pytest.raises(junit.EvidenceError):
        junit.require_complete(xml(tmp_path, document))


def test_missing_file(tmp_path):
    with pytest.raises(junit.EvidenceError):
        junit.require_complete(tmp_path / "absent.xml")


def test_read_is_bounded(tmp_path, monkeypatch):
    path = xml(tmp_path, "x" * 65)
    monkeypatch.setattr(junit, "MAX_BYTES", 64)
    with pytest.raises(junit.EvidenceError, match="byte limit"):
        junit.require_complete(path)


def test_symlink_rejected(tmp_path):
    target = xml(tmp_path, '<testsuite><testcase name="ok"/></testsuite>')
    link = tmp_path / "link.xml"
    link.symlink_to(target)
    with pytest.raises(junit.EvidenceError, match="non-symlink"):
        junit.require_complete(link)


def test_distinct_classes_allowed(tmp_path):
    path = xml(tmp_path, '<testsuite><testcase classname="a" name="same"/>'
                        '<testcase classname="b" name="same"/></testsuite>')
    assert junit.require_complete(path)[0] == 2


def test_cli_reports_each_input_and_returns_failure(tmp_path):
    good = xml(tmp_path, '<testsuite><testcase name="ok"/></testsuite>')
    bad = tmp_path / "absent.xml"
    result = subprocess.run([sys.executable, str(SCRIPT), str(bad), str(good)],
                            capture_output=True, text=True, timeout=10)
    assert result.returncode == 1
    assert "INCOMPLETE" in result.stdout and "tests=1" in result.stdout


def test_actual_pytest_junit_is_accepted(tmp_path):
    test = tmp_path / "test_sample.py"
    test.write_text("def test_true():\n    assert True\n", encoding="utf-8")
    report = tmp_path / "pytest.xml"
    result = subprocess.run([sys.executable, "-m", "pytest", str(test), "-q",
                             f"--junitxml={report}"], cwd=tmp_path,
                            capture_output=True, text=True, timeout=20)
    assert result.returncode == 0, result.stdout + result.stderr
    assert junit.require_complete(report) == (1, 0, 0, 0)
