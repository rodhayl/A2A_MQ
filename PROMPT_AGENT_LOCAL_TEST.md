# /goal - Validate the implemented lightweight local A2A-MQ on a low-resource host

Work in a NEW agent conversation in `rodhayl/A2A_MQ`, branch
`implementation/production-hardening-20260916`.

The local product is now implemented. Do not redesign it before measuring it.
The first gate is economic viability, then lightweight correctness. The target
steady state is exactly one Python A2A-MQ process + one native `daprd` + one native
`nats-server`, with SQLite application state.

## Hard host boundaries

- Use an existing Python 3.12/3.13 in a fresh project-local venv without
  `--system-site-packages`.
- Never use sudo, OS package managers, global/user pip, `--break-system-packages`,
  Docker/Compose/Podman/Kubernetes, system services, cron or shell-profile changes.
- Keep venv, caches, SQLite/NATS state and raw evidence under ignored
  `.local-validation/`.
- Start with only `requirements/core-probe.txt`. Install the broader
  `requirements/local-validation.txt` only after the pure-core resource gate passes.
- Do not install `daprd`, `nats-server`, PostgreSQL, Redis, Vault, RabbitMQ or
  telemetry services into the host. If `daprd` or `nats-server` is absent,
  complete the core/Python/regression gates first, then Phase 2 is explicitly
  authorized to use `scripts/bootstrap_local_tools.py` inside this run's
  `$STATE/tools` only. PostgreSQL/Redis/Vault/RabbitMQ/collectors remain deferred.
- Run one test command/file at a time. No xdist, coverage, load tests or parallel
  tool agents.
- Never convert missing prerequisites, skips, xfails, empty selections or timeouts
  into PASS.
- Do not push or merge. Focused local commits for proven defects are allowed.

## Phase 0 - FIRST: pure-core economic gate

Create the private run and venv before broad source analysis:

```bash
set -euo pipefail
umask 077
REPO="$(git rev-parse --show-toplevel)"
cd "$REPO"
test ! -L "$REPO/.local-validation"
mkdir -p "$REPO/.local-validation"
STATE="$(mktemp -d "$REPO/.local-validation/run.XXXXXXXX")"
mkdir -p "$STATE/tmp" "$STATE/cache" "$STATE/evidence" "$STATE/core-probe"
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
unset OPENAI_API_KEY ANTHROPIC_API_KEY GEMINI_API_KEY GOOGLE_API_KEY AZURE_OPENAI_API_KEY

PY_BASE="$(command -v python3.13 || command -v python3.12 || command -v python3)"
"$PY_BASE" -c 'import sys; assert sys.version_info[:2] in {(3,12),(3,13)}, sys.version'
"$PY_BASE" -m venv "$STATE/venv"
PY="$STATE/venv/bin/python"
"$PY" -m pip install --no-cache-dir --only-binary=:all: \
  -r "$REPO/requirements/core-probe.txt" \
  > "$EVIDENCE/install-core.log" 2>&1
"$PY" -m pip check > "$EVIDENCE/pip-check-core.log" 2>&1
git rev-parse HEAD > "$EVIDENCE/start-sha.txt"
git status --short > "$EVIDENCE/start-status.txt"
```

Run the pure Python+SQLite probe:

```bash
set +e
"$PY" "$REPO/scripts/local_resource_probe.py"   --state-dir "$STATE/core-probe"   --json "$EVIDENCE/core-resources.json"   > "$EVIDENCE/core-resources.log" 2>&1
CORE_RC=$?
set -e
cat "$EVIDENCE/core-resources.json"
```

Initial core budget:

- observed Python RSS <= 192 MiB;
- idle CPU <= 3% of one core;
- imports <= 3 seconds;
- 100-task SQLite smoke <= 8 seconds;
- SQLite DB/WAL/SHM <= 16 MiB;
- >= 512 MiB host memory headroom before the probe;
- no attributable swap growth.

If verdict is `FAIL_CORE_LIGHT` or `INCOMPLETE_HOST_HEADROOM`, STOP expensive
execution. Report **LOCAL ECONOMICS = FAIL**. Do not raise thresholds or machine
requirements to obtain a pass.

## Phase 1 - Python-product economics, then broker-free correctness

Only after the core gate passes, install the actual **local product** Python extra
in the same venv. It does not start native services:

```bash
du -sb "$STATE/venv" > "$EVIDENCE/venv-before-product.txt" || true
"$PY" -m pip install --no-cache-dir --only-binary=:all: -e '.[local]' \
  > "$EVIDENCE/install-local-product.log" 2>&1
"$PY" -m pip check > "$EVIDENCE/pip-check-local-product.log" 2>&1
du -sb "$STATE/venv" > "$EVIDENCE/venv-after-product.txt" || true

set +e
"$PY" "$REPO/scripts/local_python_probe.py" \
  --state-dir "$STATE/python-app" \
  --json "$EVIDENCE/python-app-resources.json" \
  > "$EVIDENCE/python-app-resources.log" 2>&1
PYAPP_RC=$?
set -e
cat "$EVIDENCE/python-app-resources.json"
test "$PYAPP_RC" -eq 0
```

This probe builds the real local-lite A2A/HTTP/crypto/SQLite app and enters its
lifespan so idle relays/recovery loops run, but starts neither daprd nor NATS.
Initial budget: <=192 MiB RSS, <=3% of one CPU core idle, <=5 seconds build/start
and <=4 MiB SQLite state. If it returns `FAIL_LOCAL_PYTHON`, stop native
validation and set **LOCAL ECONOMICS = REDESIGN_REQUIRED**.

If it passes, install the still-small validation-only dependencies:

```bash
"$PY" -m pip install --no-cache-dir --only-binary=:all: \
  -r "$REPO/requirements/local-validation.txt" \
  > "$EVIDENCE/install-validation.log" 2>&1
"$PY" -m pip check > "$EVIDENCE/pip-check-validation.log" 2>&1
```

Then read ADR 0011, `docs/LOCAL_RUNTIME.md`, `ARCHITECTURE.md`, `local_lite.py`,
`local_runtime.py`, TaskLedger/TaskAPI, transport, bridge and DLQ/replay.

Run `"$PY" tools/check.py` first. Execute these one at a time:

```text
tests/test_local_resource_probe.py
tests/test_local_python_probe.py
tests/test_local_stack_probe.py
tests/test_bootstrap_local_tools.py
tests/test_local_native_versions.py
tests/test_local_lite.py
tests/test_local_runtime.py
tests/test_local_workflow.py
tests/test_json_boundary_regressions.py
tests/test_domain.py
tests/test_reference_replay.py
tests/test_storage.py
tests/test_storage_recovery.py
tests/test_task_ledger.py
tests/test_task_api.py
tests/test_transport.py
tests/test_transport_lifecycle.py
tests/test_runtime.py
tests/test_operations_new.py
tests/test_projection_retention_new.py
```

Use this bounded template for every eligible file; substitute TEST/STEM each time:

```bash
TEST=tests/test_local_lite.py
STEM="$(basename "$TEST" .py)"
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
test "$RC" -eq 0
```

Unexpected skips, xfails, timeouts, empty selections or missing JUnit are not PASS.
For every real defect: preserve the minimal failure, add a regression test, fix the
root cause, rerun focused/affected lightweight tests, and create a focused local commit.

## Phase 2 - real local-lite economic gate

Proceed only if Phase 0 passes and Phase 1 has no unresolved local-lite defect.

For an acceptance result, use the repository-pinned private native tools even when
same-version host binaries happen to exist. Dapr `--version`/`--build-info` do
not identify the sidecar build flavor, and `pubsub.jetstream` in Dapr 1.18.3 is
compiled only with the `allcomponents` build tag. A same-version
`stablecomponents` binary therefore cannot prove this product topology.

Do NOT install anything into the host. Bootstrap the exact Dapr all-components
and NATS release archives only inside this run directory:

```bash
TOOLS="$STATE/tools"
"$PY" "$REPO/scripts/bootstrap_local_tools.py" --output "$TOOLS" \
  > "$EVIDENCE/bootstrap-local-tools.json"
export PATH="$TOOLS/bin:$PATH"
DAPR_BIN="$TOOLS/bin/daprd"
NATS_BIN="$TOOLS/bin/nats-server"
"$PY" "$REPO/scripts/check_local_native_versions.py" \
  --daprd "$DAPR_BIN" --nats-server "$NATS_BIN" \
  --bootstrap-manifest "$TOOLS/manifest.json" \
  --json "$EVIDENCE/native-versions-bootstrap.json"
```

Require the bootstrap to succeed, its `manifest.json` to contain
`"daprFlavor": "allcomponents"`, and the native-version checker to return
`PASS_NATIVE_VERSION`. Linux/architecture/download/checksum/version failures are
`INCOMPLETE_NATIVE_PREREQ` or `INCOMPLETE_NATIVE_VERSION`; they are not
permission to install packages or use an unproven host daprd. Record:

```bash
du -sb "$STATE/tools" > "$EVIDENCE/native-tools-size.txt" || true
```

The private tools are evidence fixtures and disappear with this validation run.
They are not added to PATH globally, copied to `~/.dapr`, or installed as
services.

Prepare one private profile:

```bash
PROFILE="$STATE/local-lite"
"$PY" -m a2a_mq.local_runtime prepare "$PROFILE"   > "$EVIDENCE/local-prepare.json"
```

Now measure the **actual** steady three-process product. Keep the default 60-second
idle window so periodic work is represented in the economics result:

```bash
set +e
"$PY" "$REPO/scripts/local_stack_probe.py"   --profile "$PROFILE"   --json "$EVIDENCE/local-stack-resources.json"   --log "$EVIDENCE/local-stack.log"
STACK_RC=$?
set -e
cat "$EVIDENCE/local-stack-resources.json"
```

Initial stack budget:

- exactly 3 product processes throughout the post-readiness idle window (startup helpers may transiently raise only the startup peak process count);
- peak aggregate RSS <= 384 MiB across startup and the idle sampling window;
- aggregate idle CPU <= 5% of one core;
- readiness <= 15 seconds;
- runtime SQLite + JetStream state <= 32 MiB both during the idle window and after a clean shutdown flush.

If it fails, classify **LOCAL ECONOMICS = REDESIGN_REQUIRED**. Do not start the
heavy distributed stack and do not raise the resource budget merely to pass.

## Phase 3 - real lightweight A2A smoke

Only after the real stack resource gate passes, start the same local profile for a
short functional smoke:

```bash
"$PY" -m a2a_mq.local_runtime run "$PROFILE"   > "$EVIDENCE/local-runtime.log" 2>&1 &
LOCAL_PID=$!
cleanup_local() {
  kill -TERM "$LOCAL_PID" 2>/dev/null || true
  wait "$LOCAL_PID" 2>/dev/null || true
}
trap cleanup_local EXIT

"$PY" - <<'PY'
import time, urllib.request
deadline = time.monotonic() + 20
while time.monotonic() < deadline:
    try:
        with urllib.request.urlopen("http://127.0.0.1:18000/health/ready", timeout=.5) as r:
            if r.status == 200:
                break
    except Exception:
        time.sleep(.1)
else:
    raise SystemExit("local-lite did not become ready")
PY

"$PY" scripts/a2a_interop.py   --url http://127.0.0.1:18000   --token-file "$PROFILE/client.jwt"   --allow-loopback-http   > "$EVIDENCE/a2a-interop.json"

cleanup_local
trap - EXIT
```

This smoke must exercise both official bindings via the existing script. Verify
that the runtime process and its two children are gone afterward. A connection
failure is FAIL, not an authorization/negative-test pass.

Do not enable MCP unless separately needed. If MCP is tested, install only
`.[mcp]` inside the venv and validate the reduced local-lite tool surface:
submit/get/list/cancel; continuation/approval are intentionally absent.

## Phase 4 - architecture review after measurements

Confirm the implementation still matches the intended local tier:

- local steady state is app + daprd + nats-server only;
- SQLite is the only application database;
- no PostgreSQL/Redis/Docker/Vault/OTLP/Placement/Scheduler dependency was added;
- Dapr is used as the Pub/Sub abstraction rather than direct broker calls in domain/runtime;
- command and DLQ subscriptions have distinct durable identities;
- inbound transient command failures have a bounded Dapr retry policy before DLQ;
- Workflow-only features fail closed rather than creating a second scheduler;
- the optional local Workflow tier is source-present and separate from the default;
- it reuses official Dapr Workflow + SQLite actor state and does not introduce Redis/PostgreSQL/Docker;
- local-lite does not claim independent offline worker-pool/observer semantics.

Record any mismatch as a product defect.

## Optional local Workflow tier

The optional Workflow implementation is part of the source review and
`tests/test_local_workflow.py`, but do not launch it automatically on this weak
machine. Its Python extra is `.[local,local-workflow]`; its native profile requires
pre-existing `daprd`, `nats-server`, `placement` and `scheduler`.

Only when the owner explicitly asks to evaluate Workflow-local economics, prepare
`a2amq-local-workflow` and run the same `scripts/local_stack_probe.py`. That
profile has its own initial gate: exactly 5 processes, <=640 MiB RSS, <=8% of one
CPU core idle, <=25 s readiness and <=64 MiB runtime state. A failure does not
invalidate the cheaper local-lite tier; it means the optional Workflow tier needs
redesign or should remain disabled on that host.

## Deferred gates

On this low-resource task do not run the full Compose/Kubernetes acceptance stack,
real PostgreSQL, Vault, RabbitMQ or broad load benchmarks. Native optional Workflow
execution is also deferred unless explicitly requested as described above. These
remain separate gates and must be reported as unexecuted, not PASS.

## Final deliverable

Write `LOCAL_VALIDATION_REPORT.md` with:

- starting/final SHA and worktree status;
- exact venv/interpreter;
- core resource JSON;
- venv disk delta for the local product extra;
- native prerequisite versions/availability;
- local-stack resource JSON if executed;
- exact lightweight test/JUnit counts;
- official A2A smoke result if executed;
- confirmed fixes and local commit SHAs;
- deferred gates/reasons;
- remaining local-product blockers.

Finish with independent verdicts:

- **CORE ECONOMICS:** PASS / FAIL
- **LOCAL-LITE ECONOMICS:** PASS / FAIL / REDESIGN_REQUIRED / INCOMPLETE_NATIVE_PREREQ
- **LIGHTWEIGHT CORRECTNESS:** PASS / FAIL / INCOMPLETE
- **OPTIONAL LOCAL-WORKFLOW:** SOURCE_READY / FAIL / INCOMPLETE_NATIVE_PREREQ / NOT_RUN
- **DISTRIBUTED/PRODUCTION ACCEPTANCE:** INCOMPLETE unless independently executed

Do not push.
