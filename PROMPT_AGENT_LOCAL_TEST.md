# /goal - Validate A2A-MQ on a low-resource server without changing the host

This is a self-contained task for a NEW agent conversation in `rodhayl/A2A_MQ`, branch `implementation/production-hardening-20260916`. Test the actual checkout, find reproducible defects and fix their root causes. The owner has explicitly required a lightweight, venv-only run. This replaces the previous instruction to install every extra and start Docker stacks.

## Non-negotiable host and resource boundaries

- Use an existing Python 3.12 or 3.13 interpreter and a fresh project-local venv WITHOUT `--system-site-packages`. Run every Python/pip command through that venv's absolute executable. A venv isolates Python packages, not CPU, RAM, network or filesystem access; obey the remaining boundaries separately.
- Keep the venv, temporary files, SQLite fixtures, caches and raw evidence under an ignored `.local-validation/` directory on a disk-backed filesystem. Preserve any pre-existing venv, files and worktree changes. Do not recreate or delete someone else's environment. Avoid `/tmp` when it is memory-backed.
- Do not use sudo, OS package managers, global/user pip installs, `--break-system-packages`, system services, cron, shell-profile changes, system Python changes or newly installed runtimes. Missing Python/venv support is `INCOMPLETE`; continue the static work possible without installing system components.
- Do not install or invoke Docker, Compose, Podman, Kubernetes, Dapr sidecars, brokers, PostgreSQL servers, Vault servers or telemetry collectors. Do not start an alternative native multi-service stack. These restrictions apply even when those tools are already installed. Existing unrelated services are not test fixtures.
- Start with ONLY `requirements/local-validation.txt`. Do not install `.[dev]`, all extras, SDK bundles, GPU/model libraries, source-build toolchains or dependency upgrades. Download binary wheels only. Record an unavailable pin/wheel as `INCOMPLETE` rather than silently changing versions.
- Run ONE test command at a time, one test file at a time. No pytest-xdist, parallel agents running tools, multiple benchmark jobs or coverage in this pass. Set numerical-library thread counts to one. Small concurrency regressions may use their existing bounded threads; inspect subprocess tests before running them and defer those requiring multiple services or material memory use.
- Inspect available RAM and disk before installing and between test groups. Use 512 MiB available RAM and 2 GiB free disk as conservative starting safety reserves, not a claim about actual hardware needs. Stop the affected workload if these reserves cannot be maintained, the host starts swapping heavily or the run's directory reaches 1 GiB. Continue source inspection instead. Do not create swap or kill other users' processes.
- Keep network use limited to the ordinary authorized Git remote and package downloads needed for the minimal environment. Tests use SQLite, in-process HTTP or short-lived loopback fixtures, never production endpoints or paid model APIs. Do not print credentials or dump the environment.
- Preserve architecture and all infrastructure tests/deployment files. This machine is not required to certify them. No reduced assertions, mocked native SDKs presented as real SDK validation, or skipped tests presented as passes.
- Commit small verified fixes locally, with regression evidence. Do not push, merge, force-push or release from this validation task unless the owner separately authorizes it.

## 1. Provenance and deliberate bug hunt

Read `AGENTS.md`, this file, `README.md`, `docs/IMPLEMENTED_STACK.md`, `docs/IMPLEMENTATION_COVERAGE.md`, relevant ADRs and `pyproject.toml`. Record the starting SHA and `git status --short` without discarding changes. The integrated entry point is `a2a_mq.application:from_env`; existing `production.py`, `production_env.py`, `service:from_env` and demo paths remain intentional separate profiles.

Review storage/inbox/outbox, leases and fencing, task admission/control, Workflow replay, identity, HTTP boundaries, recovery, retention, DLQ, backup, configuration and acceptance code together. Inspect SDK and deployment integration as source only when unavailable in the minimal environment. Concentrate on tenant/consumer/owner isolation, stable retry bytes, terminal guards, cancellation, failed-start cleanup, schema races, replay resurrection and secret leakage. Capture concrete file/line findings; do not invent a quota of defects or add speculative abstractions.

## 2. Create a private minimal environment

Use Linux bash commands below from the repository root after checking the resource reserves. If a compatible interpreter or these existing shell utilities are unavailable, adapt to existing equivalents or record the limitation; do not install host tools.

```bash
set -euo pipefail
umask 077
REPO="$(git rev-parse --show-toplevel)"
cd "$REPO"
# Inspect a pre-existing path first; reject symlinks or someone else's directory.
if [ -L "$REPO/.local-validation" ]; then exit 1; fi
mkdir -p "$REPO/.local-validation"
STATE="$(mktemp -d "$REPO/.local-validation/run.XXXXXXXX")"
mkdir -p "$STATE/tmp" "$STATE/cache" "$STATE/evidence"
EVIDENCE="$STATE/evidence"
export TMPDIR="$STATE/tmp" TMP="$STATE/tmp" TEMP="$STATE/tmp"
export XDG_CACHE_HOME="$STATE/cache" PIP_CACHE_DIR="$STATE/cache/pip"
export PYTHONPYCACHEPREFIX="$STATE/cache/pycache"
export MYPY_CACHE_DIR="$STATE/cache/mypy" RUFF_CACHE_DIR="$STATE/cache/ruff"
export PYTHONNOUSERSITE=1 PYTHONDONTWRITEBYTECODE=1
export PIP_REQUIRE_VIRTUALENV=true PIP_DISABLE_PIP_VERSION_CHECK=1
export PIP_NO_CACHE_DIR=1 PIP_NO_INPUT=1 PIP_CONFIG_FILE=/dev/null
export PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 A2AMQ_NO_LLM_TOKENS=1
export OMP_NUM_THREADS=1 OPENBLAS_NUM_THREADS=1 MKL_NUM_THREADS=1
export NUMEXPR_NUM_THREADS=1 VECLIB_MAXIMUM_THREADS=1
unset PYTHONHOME PYTHONPATH PYTHONUSERBASE PYTEST_ADDOPTS PYTEST_PLUGINS
unset PIP_TARGET PIP_PREFIX PIP_USER PIP_BREAK_SYSTEM_PACKAGES
# Process-local removal only: never inspect or change the user's stored secrets.
unset OPENAI_API_KEY ANTHROPIC_API_KEY GEMINI_API_KEY GOOGLE_API_KEY AZURE_OPENAI_API_KEY
PY_BASE="$(command -v python3.13 || command -v python3.12 || command -v python3)"
"$PY_BASE" -c 'import sys; assert sys.version_info[:2] in {(3,12),(3,13)}, sys.version'
"$PY_BASE" -m venv "$STATE/venv"
PY="$STATE/venv/bin/python"
"$PY" -c 'import sys; assert sys.prefix != sys.base_prefix; print(sys.executable)'
"$PY" -m pip --version
"$PY" -m pip install --no-cache-dir --only-binary=:all: \
  -r "$REPO/requirements/local-validation.txt" \
  > "$EVIDENCE/install.log" 2>&1
"$PY" -m pip check > "$EVIDENCE/pip-check.log" 2>&1
"$PY" -m pip list --format=json > "$EVIDENCE/dependencies.json"
git rev-parse HEAD > "$EVIDENCE/start-sha.txt"
git status --short > "$EVIDENCE/start-status.txt"
```

The venv is fresh for this run, not a reason to repeatedly reinstall dependencies. Retain it for reproducible retries. Never use a bare `pip`, `pip3` or system Python for subsequent work. Run source tests via the repository's pytest `pythonpath = ["src"]`; an editable install and a build backend are not needed for this initial pass.

## 3. Sequential, broker-free regression

Run `"$PY" tools/check.py` and save its result. Audit each candidate file's imports and fixture setup BEFORE execution. Marker filtering does not prevent module-level imports or collection side effects. Use explicit paths, not a repository-wide pytest collection.

Start with these core candidates, in this order, one at a time:

```text
tests/test_json_boundary_regressions.py
tests/test_domain.py
tests/test_reference_replay.py
tests/test_storage.py
tests/test_storage_recovery.py
tests/test_transport.py
tests/test_transport_lifecycle.py
tests/test_runtime.py
tests/test_service.py
tests/test_junit_evidence.py
tests/test_task_ledger.py
tests/test_task_api.py
tests/test_operations_new.py
tests/test_projection_retention_new.py
tests/test_pg_backup_new.py
tests/test_deployment_new.py
```

Only execute candidates whose inspected imports/fixtures fit the minimal dependency and host boundaries. `test_deployment_new.py` is for static manifest generation, not starting a stack; `test_pg_backup_new.py` uses subprocess fixtures, not a live database. Verify that remains true in this checkout. Defer any unsupported candidate explicitly and continue the supported ones. Additional deterministic modules are allowed after the same inspection, without adding dependencies.

For EACH eligible file, substitute its path and a unique log stem in this template:

```bash
TEST=tests/test_json_boundary_regressions.py
STEM="$(basename "$TEST" .py)"
# The pytest configuration adds src; explicitly enable only the async plugin.
# Use an already available timeout utility, otherwise a bounded agent tool call.
set +e
timeout --signal=TERM --kill-after=15s 300s \
  "$PY" -m pytest -p pytest_asyncio.plugin -p no:cacheprovider \
  -o addopts= -q -W error --maxfail=1 \
  -m 'not live and not postgres and not upstream and not mcp and not vault' \
  --basetemp="$STATE/tmp/$STEM" --junitxml="$EVIDENCE/$STEM.xml" "$TEST" \
  > "$EVIDENCE/$STEM.log" 2>&1
RC=$?
set -e
printf '%s\n' "$RC" > "$EVIDENCE/$STEM.exit-code"
"$PY" scripts/assert_junit_complete.py "$EVIDENCE/$STEM.xml"
# Interpret the exit code AND JUnit. A timeout/nonzero exit is not a pass.
test "$RC" -eq 0
```

A failed command is input to the fix loop, not permission to keep launching expensive retries. Preserve the first failure and its exit code. Run resource checks again before resuming. An unexpected skip, xfail, collection error, missing report, empty selection or timeout cannot satisfy this file's gate. Explicitly deselected infrastructure tests are outside this profile, not validated.

## 4. Optional tools and intentionally deferred gates

Use Ruff/mypy only if already available INSIDE this run's venv, sequentially with bounded commands. Do not use host executables or install all development extras to obtain them. If unavailable, record `NOT_RUN_DEPENDENCY_LIMIT`. Packaging can be tested only if its pinned build prerequisites are already present in the venv and resource reserves permit it; otherwise defer it explicitly.

Security/telemetry/native-SDK tests requiring extra packages are source-reviewed and marked `NOT_RUN_DEPENDENCY_LIMIT` in the default run. Do not install cryptography, OpenTelemetry exporters, A2A/MCP/Dapr SDK bundles or database drivers merely to make the total count larger. The owner can authorize a separately budgeted venv-only extension later.

Do not run `make up`, `make check`, bare repository-wide pytest, `a2a_mq.live_probe`, performance/load tests, or the acceptance driver's `full`, `sdk`, `postgres` or `vault` modes. Do not run its `local` mode blindly either: its current prerequisite/collection scope is broader than this lightweight profile. Keep that driver and its strict full criteria unchanged.

Record PostgreSQL, Vault, real Dapr Workflow, JetStream, RabbitMQ, native A2A/MCP interoperability and full-stack acceptance as `NOT_RUN_RESOURCE_LIMIT` or `NOT_RUN_DEPENDENCY_LIMIT`, with overall full acceptance `INCOMPLETE`. Unit fixtures do not replace those integration gates. No remote/cloud execution or external service connections are authorized as a workaround.

## 5. Fix loop and cleanup

For each confirmed defect: preserve a minimal reproducer, add a regression test, apply the smallest architecture-consistent fix, rerun that test and affected lightweight files sequentially, and make a focused local commit. Separate code defects, missing dependencies and host-resource limits. Preserve passing behavior and existing full infrastructure gates.

Stop and reap only subprocesses created by this run. Remove their temporary databases/files once evidence is captured; keep the isolated venv and compact evidence for reproducibility. Do not sweep unrelated `/tmp`, remove old worktrees, kill matching system processes, or run Docker cleanup commands. Never commit `.local-validation/`, venvs, caches, credentials, raw dumps or unsanitized logs. No scheduled jobs, background services or shell activation should remain after the task.

## 6. Deliverable

Write a concise, sanitized `LOCAL_VALIDATION_REPORT.md` at the repository root. Include starting/final SHA, preserved pre-existing changes, venv path/interpreter, exact command/exit-code/JUnit counts for executed files, resource observations, confirmed fixes and local commit SHAs, unexecuted files with reasons, and unresolved defects. Raw evidence stays in `$EVIDENCE` under the ignored run directory; do not duplicate or upload large archives.

Use `PASS_LOCAL_LIGHT` only when every selected supported file genuinely passes and no confirmed local defect remains. Report any unexecuted broader requirement separately and overall full acceptance as `INCOMPLETE`; this run is not whole-repository certification, infrastructure parity or production readiness. Finish with the report path, evidence path and a short summary of what actually ran. Do not push.
