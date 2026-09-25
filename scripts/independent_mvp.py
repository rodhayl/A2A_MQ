"""Frozen-SHA review. Local acceptance is explicitly not full-MVP acceptance."""
from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
from pathlib import Path
import re
import stat
import subprocess
import sys

# Keep the reviewed checkout untouched when importing the local evidence parser.
sys.dont_write_bytecode = True

from assert_junit_complete import EvidenceError, require_complete  # noqa: E402

ROOT = Path(__file__).resolve().parents[1]
EXCLUDED = "not live and not postgres and not upstream"
CREDENTIAL = re.compile(
    r"^(?:OPENAI|ANTHROPIC|AZURE_OPENAI|GEMINI|GOOGLE|MISTRAL|COHERE|GROQ|"
    r"DEEPSEEK|TOGETHER|OPENROUTER|HUGGING_FACE|HF_|XAI|CEREBRAS|REPLICATE).*(?:KEY|TOKEN|CREDENTIAL)", re.I
)
FORBIDDEN_DIRS = {".bundle", ".sync", ".publish", ".runtime", ".venv", "__pycache__"}
GIT_OVERRIDES = {"GIT_DIR", "GIT_WORK_TREE", "GIT_INDEX_FILE", "GIT_OBJECT_DIRECTORY",
                 "GIT_ALTERNATE_OBJECT_DIRECTORIES"}
FULL_GATES = ["canonical_snapshot_recovery", "postgresql", "jetstream", "rabbitmq",
              "dapr_workflow", "official_a2a_sdk", "official_mcp_sdk", "vault",
              "full_system_e2e", "package_and_supply_chain", "production_deployment"]


def git(*args: str) -> str:
    result = subprocess.run(["git", "--no-optional-locks", "-C", str(ROOT), *args],
                            capture_output=True, text=True, timeout=20, check=False)
    if result.returncode:
        raise EvidenceError("Git verification failed")
    return result.stdout.strip()


def snapshot(sha: str) -> str:
    if Path(git("rev-parse", "--show-toplevel")).resolve() != ROOT:
        raise EvidenceError("Reviewer must reside at the checkout root")
    if git("rev-parse", "HEAD") != sha:
        raise EvidenceError("HEAD differs from the requested exact SHA")
    if git("status", "--porcelain=v1", "--untracked-files=all"):
        raise EvidenceError("Checkout must be clean, including untracked files")
    return git("rev-parse", "HEAD^{tree}")


def preflight(sha: str, output: Path) -> str:
    if not re.fullmatch(r"[0-9a-f]{40}", sha):
        raise EvidenceError("Supply a lowercase exact 40-character commit SHA")
    if any(os.environ.get(key) for key in GIT_OVERRIDES):
        raise EvidenceError("Unset Git repository/index overrides")
    if any(value and CREDENTIAL.match(key) for key, value in os.environ.items()):
        raise EvidenceError("Unset model-provider credentials before review")
    if output == ROOT or ROOT in output.parents:
        raise EvidenceError("Evidence must be outside the checkout")
    if output.exists():
        raise EvidenceError("Evidence directory already exists; use a fresh path")
    tree = snapshot(sha)
    common = Path(git("rev-parse", "--path-format=absolute", "--git-common-dir")).resolve()
    if output == common or common in output.parents:
        raise EvidenceError("Evidence must be outside Git metadata")
    for record in git("ls-files", "-s", "-z").split("\0"):
        if not record:
            continue
        metadata, name = record.split("\t", 1)
        mode = metadata.split(" ", 1)[0]
        path = Path(name)
        if mode == "120000" and (name == "src" or name.startswith("src/")):
            raise EvidenceError("Tracked source symlink present")
        if (FORBIDDEN_DIRS.intersection(path.parts) or path.name == ".env"
                or path.suffix.lower() in {".pem", ".key", ".p12", ".pfx"}):
            raise EvidenceError("Tracked recovery/runtime/secret artifact present")
    for name in ("tools/check.py", "tests", "scripts/assert_junit_complete.py"):
        if not (ROOT / name).exists():
            raise EvidenceError("Required local validation input is missing: " + name)
    return tree


def review_env() -> dict[str, str]:
    env = dict(os.environ, PYTHONDONTWRITEBYTECODE="1", A2AMQ_NO_LLM_TOKENS="1",
               PYTEST_DISABLE_PLUGIN_AUTOLOAD="1")
    env["PYTHONPATH"] = str(ROOT / "src")
    env.pop("PYTEST_ADDOPTS", None)
    env.pop("PYTEST_PLUGINS", None)
    return env


def evidence_sha256(path: Path) -> str:
    flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_NONBLOCK", 0)
    fd = os.open(path, flags)
    with os.fdopen(fd, "rb") as stream:
        info = os.fstat(stream.fileno())
        if not stat.S_ISREG(info.st_mode):
            raise EvidenceError("Evidence path is not a regular file")
        return hashlib.file_digest(stream, "sha256").hexdigest()


def run(command: list[str], log: Path, timeout: float) -> int:
    env = review_env()
    with log.open("xb") as stream:
        try:
            result = subprocess.run(command, cwd=ROOT, env=env, stdout=stream,
                                    stderr=subprocess.STDOUT, timeout=timeout, check=False)
            return result.returncode
        except subprocess.TimeoutExpired:
            return 124


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--sha", required=True)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--scope", choices=("local", "full"), default="full")
    parser.add_argument("--timeout", type=float, default=900.0)
    args = parser.parse_args()
    if not math.isfinite(args.timeout) or not 0 < args.timeout <= 7200:
        parser.error("timeout must be finite and in (0, 7200]")
    output = args.output.resolve()
    try:
        tree = preflight(args.sha, output)
        output.mkdir(parents=True, mode=0o700, exist_ok=False)
    except (EvidenceError, OSError, subprocess.SubprocessError) as exc:
        print("INCOMPLETE: " + str(exc), file=sys.stderr)
        return 2
    report = {"sha": args.sha, "tree": tree, "scope": args.scope, "status": "INCOMPLETE",
              "mvpStatus": "INCOMPLETE", "productionCertified": False,
              "unverifiedGates": FULL_GATES, "runs": [], "evidenceSha256": {},
              "zeroLlmPolicy": "credential guard and deterministic fixtures; not an egress sandbox"}
    code = 2
    try:
        if args.scope == "full":
            raise EvidenceError("Full MVP snapshot and full strict validator have not been recovered")
        report["excludedMarkers"] = ["live", "postgres", "upstream"]
        commands = [
            ("repository", [sys.executable, "tools/check.py"]),
            ("local", [sys.executable, "-m", "pytest", "-p", "pytest_asyncio.plugin",
                       "-p", "no:cacheprovider", "-q", "-W", "error",
                       "-m", EXCLUDED, "--junitxml=" + str(output / "local.junit.xml")]),
        ]
        for name, command in commands:
            rc = run(command, output / (name + ".log"), args.timeout)
            report["runs"].append({"name": name, "returncode": rc})
        counts = require_complete(output / "local.junit.xml")
        report["testCounts"] = dict(zip(("tests", "failures", "errors", "skipped"), counts, strict=True))
        if any(item["returncode"] for item in report["runs"]):
            raise EvidenceError("A required local command failed or timed out")
        report["status"] = "LOCAL_PASS"
        code = 0
    except (EvidenceError, OSError, subprocess.SubprocessError) as exc:
        report["reason"] = str(exc)
    finally:
        try:
            if snapshot(args.sha) != tree:
                raise EvidenceError("Tree changed during review")
        except (EvidenceError, subprocess.SubprocessError) as exc:
            report["status"] = "INCOMPLETE"
            report["reason"] = str(exc)
            code = 2
        for name in ("repository.log", "local.log", "local.junit.xml"):
            path = output / name
            if not path.exists() and not path.is_symlink():
                continue
            try:
                report["evidenceSha256"][name] = evidence_sha256(path)
            except (EvidenceError, OSError):
                report["status"] = "INCOMPLETE"
                report["reason"] = "Unsafe evidence path: " + name
                code = 2
        verdict = json.dumps(report, indent=2, sort_keys=True) + "\n"
        (output / "independent-verdict.json").write_text(verdict, encoding="utf-8")
        digest = hashlib.sha256(verdict.encode()).hexdigest()
        (output / "independent-verdict.sha256").write_text(digest + "  independent-verdict.json\n")
    print(report["status"] + ": " + str(output / "independent-verdict.json"))
    return code


if __name__ == "__main__":
    raise SystemExit(main())
