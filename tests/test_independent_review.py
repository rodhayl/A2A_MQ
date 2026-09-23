"""Reviewer integration tests use tiny disposable Git repos, not an MVP mock pass."""
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys

import pytest

ROOT = Path(__file__).resolve().parents[1]


def git(root, *args):
    return subprocess.run(["git", "-C", str(root), *args], check=True,
                          text=True, capture_output=True, timeout=15).stdout.strip()


@pytest.fixture
def repository(tmp_path):
    root = tmp_path / "repo"
    (root / "scripts").mkdir(parents=True)
    (root / "tests").mkdir()
    (root / "tools").mkdir()
    for name in ("independent_mvp.py", "assert_junit_complete.py"):
        shutil.copyfile(ROOT / "scripts" / name, root / "scripts" / name)
    (root / "tools/check.py").write_text('print("synthetic fixture check")\n')
    (root / "tests/test_sample.py").write_text('def test_ok():\n    assert True\n')
    (root / "tracked.txt").write_text("original\n")
    (root / ".gitignore").write_text("__pycache__/\n.pytest_cache/\n")
    git(root, "init", "-q")
    git(root, "config", "user.name", "Review fixture")
    git(root, "config", "user.email", "fixture@example.invalid")
    git(root, "add", ".")
    git(root, "commit", "-qm", "synthetic fixture")
    return root


def invoke(root, output, *args, env_updates=None):
    env = {k: v for k, v in os.environ.items()
           if not any(n in k.upper() for n in ("KEY", "TOKEN", "CREDENTIAL"))
           and not k.startswith("GIT_")}
    env.update(PYTHONDONTWRITEBYTECODE="1", PYTEST_DISABLE_PLUGIN_AUTOLOAD="1")
    env.update(env_updates or {})
    return subprocess.run([sys.executable, str(root / "scripts/independent_mvp.py"),
                           "--sha", git(root, "rev-parse", "HEAD"), "--output", str(output), *args],
                          text=True, capture_output=True, timeout=30, env=env)


def commit(root):
    git(root, "add", ".")
    git(root, "commit", "-qm", "fixture variant")


def test_review_environment_disables_external_pytest_plugins(repository, monkeypatch):
    import importlib.util

    scripts = repository / "scripts"
    monkeypatch.syspath_prepend(str(scripts))
    path = scripts / "independent_mvp.py"
    spec = importlib.util.spec_from_file_location("independent_mvp_fixture", path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    monkeypatch.setenv("PYTEST_ADDOPTS", "-p arbitrary")
    monkeypatch.setenv("PYTEST_PLUGINS", "arbitrary")
    env = module.review_env()
    assert env["PYTEST_DISABLE_PLUGIN_AUTOLOAD"] == "1"
    assert "PYTEST_ADDOPTS" not in env
    assert "PYTEST_PLUGINS" not in env
    assert env["A2AMQ_NO_LLM_TOKENS"] == "1"


def test_local_pass_has_exact_binding_and_hashed_evidence(repository, tmp_path):
    output = tmp_path / "evidence"
    result = invoke(repository, output, "--scope", "local")
    assert result.returncode == 0, result.stdout + result.stderr
    report = json.loads((output / "independent-verdict.json").read_text())
    assert report["status"] == "LOCAL_PASS"
    assert report["mvpStatus"] == "INCOMPLETE" and report["productionCertified"] is False
    assert report["sha"] == git(repository, "rev-parse", "HEAD")
    assert report["tree"] == git(repository, "rev-parse", "HEAD^{tree}")
    assert report["testCounts"] == dict(tests=1, failures=0, errors=0, skipped=0)
    assert report["excludedMarkers"] == ["live", "postgres", "upstream"]
    for name, digest in report["evidenceSha256"].items():
        assert hashlib.sha256((output / name).read_bytes()).hexdigest() == digest
    assert (output / "independent-verdict.sha256").read_text().split()[0] == hashlib.sha256(
        (output / "independent-verdict.json").read_bytes()).hexdigest()
    assert not git(repository, "status", "--porcelain")


def test_default_scope_cannot_certify_missing_mvp(repository, tmp_path):
    output = tmp_path / "evidence"
    result = invoke(repository, output)
    assert result.returncode == 2
    report = json.loads((output / "independent-verdict.json").read_text())
    assert report["status"] == "INCOMPLETE" and not report["productionCertified"]
    assert report["runs"] == [] and report["unverifiedGates"]


@pytest.mark.parametrize("sha", ["main", "a" * 39, "A" * 40, "0" * 40])
def test_exact_sha_required(repository, tmp_path, sha):
    result = invoke(repository, tmp_path / "evidence", "--sha", sha, "--scope", "local")
    assert result.returncode == 2 and "INCOMPLETE" in result.stderr


@pytest.mark.parametrize("untracked", [False, True])
def test_dirty_checkout_rejected(repository, tmp_path, untracked):
    (repository / ("new.txt" if untracked else "tracked.txt")).write_text("changed")
    result = invoke(repository, tmp_path / "evidence", "--scope", "local")
    assert result.returncode == 2 and "clean" in result.stderr


def test_evidence_inside_checkout_rejected(repository):
    result = invoke(repository, repository / "evidence", "--scope", "local")
    assert result.returncode == 2 and "outside" in result.stderr
    assert not (repository / "evidence").exists()


def test_existing_evidence_preserved(repository, tmp_path):
    output = tmp_path / "evidence"
    output.mkdir()
    (output / "keep.txt").write_text("preserve")
    result = invoke(repository, output, "--scope", "local")
    assert result.returncode == 2 and (output / "keep.txt").read_text() == "preserve"


@pytest.mark.parametrize("name", ["OPENAI_API_KEY", "ANTHROPIC_API_KEY", "GEMINI_API_KEY", "HF_TOKEN"])
def test_model_credentials_rejected_without_disclosing_value(repository, tmp_path, name):
    result = invoke(repository, tmp_path / "evidence", "--scope", "local",
                    env_updates={name: "synthetic-value-must-not-print"})
    assert result.returncode == 2 and "credentials" in result.stderr
    assert "synthetic-value-must-not-print" not in result.stdout + result.stderr


@pytest.mark.parametrize("name", [".bundle/part-00", ".sync/staged", ".publish/01.patch", ".runtime/state", "private.key"])
def test_tracked_recovery_and_secret_files_rejected(repository, tmp_path, name):
    path = repository / name
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("synthetic")
    commit(repository)
    result = invoke(repository, tmp_path / "evidence", "--scope", "local")
    assert result.returncode == 2 and "artifact" in result.stderr


@pytest.mark.parametrize("body", [
    "def test_bad():\n    assert False\n",
    "import pytest\ndef test_skip():\n    pytest.skip('synthetic')\n",
    "# no tests\n",
])
def test_failed_skipped_or_absent_tests_are_incomplete(repository, tmp_path, body):
    (repository / "tests/test_sample.py").write_text(body)
    commit(repository)
    output = tmp_path / "evidence"
    result = invoke(repository, output, "--scope", "local")
    assert result.returncode != 0
    assert json.loads((output / "independent-verdict.json").read_text())["status"] == "INCOMPLETE"


def test_worktree_mutation_during_tests_invalidates_acceptance(repository, tmp_path):
    (repository / "tests/test_sample.py").write_text(
        "from pathlib import Path\ndef test_mutate():\n    Path('tracked.txt').write_text('changed')\n")
    commit(repository)
    output = tmp_path / "evidence"
    result = invoke(repository, output, "--scope", "local")
    assert result.returncode == 2
    report = json.loads((output / "independent-verdict.json").read_text())
    assert report["status"] == "INCOMPLETE" and "clean" in report["reason"]


def test_invalid_timeout_rejected(repository, tmp_path):
    result = invoke(repository, tmp_path / "evidence", "--timeout", "nan")
    assert result.returncode == 2 and "finite" in result.stderr


def test_missing_repository_check_rejected(repository, tmp_path):
    (repository / "tools/check.py").unlink()
    commit(repository)
    result = invoke(repository, tmp_path / "evidence", "--scope", "local")
    assert result.returncode == 2 and "missing" in result.stderr
