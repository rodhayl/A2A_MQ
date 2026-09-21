# /goal - Prove local viability first, then validate A2A-MQ without overloading the host

This is a self-contained task for a NEW agent conversation in `rodhayl/A2A_MQ`,
branch `implementation/production-hardening-20260916`.

The first product question is **not whether every feature works**. It is whether our
local bridge design is economically viable on this low-resource machine. A technically
correct bridge that needs a heavy container stack or consumes too much idle RAM/CPU
fails the local-product goal.

## Non-negotiable host boundaries

- Use an existing Python 3.12/3.13 and a fresh project-local venv without
  `--system-site-packages`. Never use sudo, OS package managers, global/user pip,
  `--break-system-packages`, system services, cron or shell-profile changes.
- Keep venv, caches, SQLite fixtures and raw evidence under ignored
  `.local-validation/`. Preserve pre-existing worktree changes and environments.
- Do not install or invoke Docker, Compose, Podman, Kubernetes, Dapr, NATS,
  RabbitMQ, PostgreSQL, Redis, Vault or telemetry collectors in this task.
- Start with only `requirements/local-validation.txt`, binary wheels only.
- Run one command/test file at a time. No xdist, coverage, parallel agents or load
  tests. Keep numerical-library thread counts at one.
- Never turn a missing dependency/service or skipped infrastructure gate into PASS.
- Do not push or merge. Focused local commits for proven fixes are allowed.

## Phase 0 - FIRST TEST: local economic viability

Do this before the bug hunt and before broad tests.

Create the private venv/run directory:

```bash
set -euo pipefail
umask 077
REPO="$(git rev-parse --show-toplevel)"
cd "$REPO"
test ! -L "$REPO/.local-validation"
mkdir -p "$REPO/.local-validation"
STATE="$(mktemp -d "$REPO/.local-validation/run.XXXXXXXX")"
mkdir -p "$STATE/tmp" "$STATE/cache" "$STATE/evidence" "$STATE/probe"
EVIDENCE="$STATE/evidence"
export TMPDIR="$STATE/tmp" TMP="$STATE/tmp" TEMP="$STATE/tmp"
export XDG_CACHE_HOME="$STATE/cache" PIP_CACHE_DIR="$STATE/cache/pip"
export PYTHONPYCACHEPREFIX="$STATE/cache/pycache"
export PYTHONNOUSERSITE=1 PYTHONDONTWRITEBYTECODE=1
export PIP_REQUIRE_VIRTUALENV=true PIP_DISABLE_PIP_VERSION_CHECK=1
export PIP_NO_CACHE_DIR=1 PIP_NO_INPUT=1 PIP_CONFIG_FILE=/dev/null
export PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 A2AMQ_NO_LLM_TOKENS=1
export OMP_NUM_THREADS=1 OPENBLAS_NUM_THREADS=1 MKL_NUM_THREADS=1
export NUMEXPR_NUM_THREADS=1 VECLIB_MAXIMUM_THREADS=1
unset PYTHONHOME PYTHONPATH PYTHONUSERBASE PYTEST_ADDOPTS PYTEST_PLUGINS
unset PIP_TARGET PIP_PREFIX PIP_USER PIP_BREAK_SYSTEM_PACKAGES
PY_BASE="$(command -v python3.13 || command -v python3.12 || command -v python3)"
"$PY_BASE" -c 'import sys; assert sys.version_info[:2] in {(3,12),(3,13)}, sys.version'
"$PY_BASE" -m venv "$STATE/venv"
PY="$STATE/venv/bin/python"
"$PY" -m pip install --no-cache-dir --only-binary=:all:   -r "$REPO/requirements/local-validation.txt"   > "$EVIDENCE/install.log" 2>&1
"$PY" -m pip check > "$EVIDENCE/pip-check.log" 2>&1
```

Now run the repository's stdlib/core probe:

```bash
set +e
"$PY" "$REPO/scripts/local_resource_probe.py"   --state-dir "$STATE/probe"   --json "$EVIDENCE/resource-viability.json"   > "$EVIDENCE/resource-viability.log" 2>&1
PROBE_RC=$?
set -e
cat "$EVIDENCE/resource-viability.json"
```

The initial core budget is:

- maximum observed Python RSS: **192 MiB**;
- idle CPU: **<=3% of one core**;
- imports: **<=3 s**;
- 100-task SQLite smoke: **<=8 s**;
- SQLite DB/WAL/SHM: **<=16 MiB**;
- at least **512 MiB available host RAM** before the probe;
- no swap growth attributable to the probe.

These are product-budget assumptions for the first measurement, not production SLOs.

**Stop here** if the probe returns `FAIL_CORE_LIGHT` or
`INCOMPLETE_HOST_HEADROOM`. Do not compensate by increasing host requirements.
Write `LOCAL_VALIDATION_REPORT.md` with the measurements and classify local
economics as a blocker.

If it passes, perform a topology-cost review before running functional tests:

1. Read ADR 0011, `application.py`, `deployment.py`, `configuration.py`,
   `ARCHITECTURE.md` and `IMPLEMENTED_STACK.md`.
2. List every process/service required by the current integrated local profile.
3. Separate **product-required** components from **acceptance/production-only**
   components.
4. Explicitly evaluate PostgreSQL, Redis, Docker, multiple Dapr sidecars, Placement,
   Scheduler, Vault and OTLP. Do not assume they belong in the default local product.
5. Record one of:
   - `LOCAL_CORE_VIABLE`: core budget passes;
   - `LOCAL_TOPOLOGY_NEEDS_REDESIGN`: core is light but current integrated topology
     is unnecessarily heavy for a local bridge;
   - `LOCAL_PRODUCT_VIABLE_AS_IMPLEMENTED`: only if the actually required local
     topology is already lightweight and supported by evidence.

Do **not** award `LOCAL_PRODUCT_VIABLE_AS_IMPLEMENTED` merely because the core probe
passes. The current implementation still assumes Dapr in the integrated application;
that is a known architectural question to verify, not a hidden pass.

## Phase 1 - deliberate architecture and bug review

Only after Phase 0 passes, review the implementation as one system. Start with
`AGENTS.md`, ADR 0011, `README.md`, `ARCHITECTURE.md`, `REQUIREMENTS.md`,
`IMPLEMENTED_STACK.md`, `IMPLEMENTATION_COVERAGE.md`, `pyproject.toml`,
`application.py`, `configuration.py`, `deployment.py` and the storage/runtime
modules.

First challenge unnecessary infrastructure and process boundaries. For the default
local bridge, prefer the smallest architecture that preserves the advertised local
semantics. The full Compose/Kubernetes topology is an acceptance/distributed profile,
not the target local footprint.

Then hunt concrete correctness issues: tenant/consumer/owner isolation, immutable
IDs, leases/fences, outbox ambiguity, cancellation, terminal guards, startup cleanup,
replay resurrection, DLQ integrity, secret leakage and false readiness. Capture
specific file/line findings in `$EVIDENCE/bug-hunt.md`.

## Phase 2 - sequential broker-free regression

Run `"$PY" tools/check.py` and save the result. Inspect imports/fixtures before
executing each test. Execute one eligible file at a time, starting with:

```text
tests/test_local_resource_probe.py
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
```

Use this bounded pattern:

```bash
TEST=tests/test_local_resource_probe.py
STEM="$(basename "$TEST" .py)"
set +e
timeout --signal=TERM --kill-after=15s 300s   "$PY" -m pytest -p pytest_asyncio.plugin -p no:cacheprovider   -o addopts= -q -W error --maxfail=1   -m 'not live and not postgres and not upstream and not mcp and not vault'   --basetemp="$STATE/tmp/$STEM" --junitxml="$EVIDENCE/$STEM.xml" "$TEST"   > "$EVIDENCE/$STEM.log" 2>&1
RC=$?
set -e
printf '%s\n' "$RC" > "$EVIDENCE/$STEM.exit-code"
"$PY" scripts/assert_junit_complete.py "$EVIDENCE/$STEM.xml"
test "$RC" -eq 0
```

An unexpected skip, xfail, timeout, empty report or collection failure is not PASS.
Do not keep retrying expensive failures without diagnosis.

## Phase 3 - design recommendation after measurements

Based on Phase 0 and source review, recommend the smallest local tier that preserves
real value. Evaluate these candidates, without launching them on this host:

1. **Core diagnostic**: Python + SQLite only. Useful baseline, not MQ parity.
2. **Local MQ Lite target**: one app process + SQLite + one native Dapr sidecar +
   one native single-node JetStream. No Docker/PostgreSQL/Redis/Vault/OTLP by default.
   Workflow-only features may be disabled unless their tier is enabled.
3. **Local Workflow target**: Local MQ Lite plus only the minimum Dapr
   Placement/Scheduler services and a Dapr SQLite Workflow state store where
   supported. Measure each daemon incrementally in a separately authorized run.
4. **Distributed/production**: existing role-separated Dapr, shared PostgreSQL,
   production broker/state, HA/security/observability infrastructure.

Do not rewrite architecture speculatively. If the current implementation lacks the
chosen lightweight composition, identify the minimum code changes and tests needed.
The owner will decide whether to authorize that implementation.

## Phase 4 - fixes and report

For every reproducible defect: preserve the failing case, add a regression test,
make the smallest root-cause fix, rerun the focused and affected lightweight tests,
and commit locally. Keep resource/design issues separate from correctness bugs.

Write a concise sanitized `LOCAL_VALIDATION_REPORT.md` containing:

- starting/final SHA and clean/dirty status;
- exact venv path and interpreter;
- the full Phase-0 resource JSON and viability classification;
- current mandatory process topology and estimated unnecessary components;
- recommended local tier and why;
- exact test commands/counts;
- confirmed fixes/local commit SHAs;
- deferred infrastructure gates and reasons;
- remaining product blockers.

Raw evidence remains under `$EVIDENCE`. Do not push.

Finish with two independent verdicts:

- **LOCAL ECONOMICS:** PASS / FAIL / REDESIGN_REQUIRED
- **LIGHTWEIGHT CORRECTNESS:** PASS / FAIL / INCOMPLETE

Full distributed/production acceptance remains separately INCOMPLETE unless it was
executed elsewhere with its real prerequisites.
