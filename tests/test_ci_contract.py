"""Static CI guardrails are not evidence that a hosted runner actually executed."""
from pathlib import Path
import re

import yaml

WORKFLOW = Path(__file__).resolve().parents[1] / ".github/workflows/ci.yml"


def workflow():
    return yaml.load(WORKFLOW.read_text(), Loader=yaml.BaseLoader)


def test_ci_preserves_read_only_pr_and_python_matrix():
    value = workflow()
    assert set(value["on"]) == {"push", "pull_request", "workflow_dispatch"}
    assert value["permissions"] == {"contents": "read"}
    job = value["jobs"]["local"]
    assert job["strategy"]["matrix"]["python"] == ["3.12", "3.13"]
    assert job["strategy"]["fail-fast"] == "false"
    assert 0 < int(job["timeout-minutes"]) <= 30
    assert all(step.get("continue-on-error", "false") == "false" for step in job["steps"])


def test_all_actions_are_sha_pinned_without_persisted_checkout_credentials():
    steps = workflow()["jobs"]["local"]["steps"]
    actions = [step for step in steps if "uses" in step]
    assert len(actions) == 3
    assert all(re.fullmatch(r"actions/[a-z-]+@[0-9a-f]{40}", step["uses"]) for step in actions)
    checkout = next(step for step in actions if step["uses"].startswith("actions/checkout@"))
    assert checkout["with"]["persist-credentials"] == "false"


def test_required_sdk_is_installed_and_selected_without_hidden_skips():
    steps = workflow()["jobs"]["local"]["steps"]
    commands = "\n".join(step.get("run", "") for step in steps)
    assert ".[dev,upstream,postgres,mcp]" in commands and "python -m pip check" in commands
    assert "import a2a.types" in commands
    test = next(step for step in steps if step.get("id") == "tests")
    assert "-W error" in test["run"] and "not live and not postgres" in test["run"]
    assert "not upstream" not in test["run"]
    assert "-p pytest_asyncio.plugin" in test["run"]
    assert workflow()["env"]["A2AMQ_NO_LLM_TOKENS"] == "1"
    assert workflow()["env"]["PYTEST_DISABLE_PLUGIN_AUTOLOAD"] == "1"
    postgres = next(step for step in workflow()["jobs"]["postgres"]["steps"]
                    if step.get("name") == "Real PostgreSQL transactional gate")
    assert "-p pytest_asyncio.plugin" in postgres["run"]


def test_junit_gate_runs_even_when_the_report_is_missing():
    steps = workflow()["jobs"]["local"]["steps"]
    junit = next(step for step in steps if step.get("id") == "junit")
    assert junit["if"] == "always()"
    assert "scripts/assert_junit_complete.py" in junit["run"]
    assert "results.xml" in junit["run"]


def test_upload_is_restricted_to_junit_and_missing_evidence_is_an_error():
    steps = workflow()["jobs"]["local"]["steps"]
    upload = next(step for step in steps if step.get("id") == "evidence")
    assert upload["if"] == "always()"
    assert upload["with"]["path"] == "${{ runner.temp }}/a2amq-validation/results.xml"
    assert upload["with"]["if-no-files-found"] == "error"

def test_wheel_smoke_imports_the_installed_target_under_isolated_python():
    steps = workflow()["jobs"]["local"]["steps"]
    smoke = next(step for step in steps if step.get("name") == "Build and install wheel outside source tree")
    command = smoke["run"]
    assert 'A2AMQ_WHEEL_TARGET="$RUNNER_TEMP/installed" python -I' in command
    assert 'sys.path.insert(0,str(target))' in command
    assert 'Path(a2a_mq.__file__).resolve().is_relative_to(target)' in command
    assert 'PYTHONPATH="$RUNNER_TEMP/installed" python -I' not in command
    assert "pip wheel --no-deps --wheel-dir" in command
    assert "--no-build-isolation" not in command


def test_warning_suppression_policy_does_not_mask_broad_resource_or_unraisable_warnings():
    from pathlib import Path

    root = Path(__file__).parent.parent
    pyproject = (root / "pyproject.toml").read_text(encoding="utf-8")
    acceptance = (root / "src/a2a_mq/acceptance.py").read_text(encoding="utf-8")
    assert "ignore::ResourceWarning" not in pyproject
    assert "ignore::pytest.PytestUnraisableExceptionWarning" not in pyproject
    assert "ignore::ResourceWarning" not in acceptance
    assert "ignore::pytest.PytestUnraisableExceptionWarning" not in acceptance
    assert "ignore:<socket.socket:pytest.PytestUnraisableExceptionWarning" in pyproject
    assert "ignore:<function BaseEventLoop.__del__:pytest.PytestUnraisableExceptionWarning" in pyproject
    assert "ignore:<socket.socket:pytest.PytestUnraisableExceptionWarning" in acceptance
    assert "ignore:<function BaseEventLoop.__del__:pytest.PytestUnraisableExceptionWarning" in acceptance


