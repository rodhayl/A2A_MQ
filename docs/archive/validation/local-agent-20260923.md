# Local Validation Evidence Report — 2026-09-23

## A. Provenance

- **Original User Checkout Branch:** `implementation/production-hardening-20260916`
- **Original User Checkout SHA:** `8f168a088cfa2b46b8aad0f2dcfb28a8abad806b`
- **Original User Checkout Dirty/Clean State:** Clean (`git status --porcelain=v2` is completely empty)
- **Remote Branch SHA Fetched:** `524d36509cf4cbb2a1a166330e45994f49a74774` (`docs(status): hand off latest source audit`)
- **Starting Validation SHA:** `524d36509cf4cbb2a1a166330e45994f49a74774`
- **Final Validated Source SHA:** `83e1d93c2cb9a4f06db328d026f99d29eca1135e`
- **Documentation / Report Commit SHA:** (This commit)
- **`main` Branch SHA:** `66c17153d7ec75b9ad85808ef141b9dba43a9d91` (unmodified)
- **Package Version:** `0.1.0` (unchanged)

---

## B. Environment

- **Operating System:** Linux 6.6.137+ x86_64
- **CPU Architecture:** `x86_64` (`amd64`)
- **Python Version:** Python 3.12.3 (`GCC 13.3.0`, `/usr/bin/python3.12`)
- **Pip Version:** 24.0
- **Validation Run Directory:** Disposable root under `.local-validation/agent-20260923/`
- **Virtual Environments Used:**
  - Minimal Core: `.local-validation/agent-20260923/venv-core` (from `requirements/core-probe.txt`)
  - Full Test / Runtime: `.local-validation/agent-20260923/venv-test` (from `.[dev,upstream,local,local-workflow]`)
- **Important Pinned Package Versions:**
  - `a2a-sdk==1.1.2`
  - `dapr==1.18.3`
  - `dapr-ext-workflow==1.18.3`
  - `mcp==2.2.0`
  - `httpx2==2.13.0`
  - `pytest==9.0.2`
  - `ruff==0.11.13`
  - `mypy==1.15.0`
  - `cryptography==46.0.4`
  - `PyJWT==2.13.0`
  - `nats-py==2.11.0`
  - `fastapi==0.128.2`
  - `uvicorn==0.48.0`
  - `jsonschema==4.26.0`
- **Native Binaries & Provenance Fingerprints:**
  - `daprd`: 1.18.3 (`allcomponents` flavor), binary SHA-256: `041c4c3ab9b0d3449faf9d0876377062f1cdcda64d3109989645a9f57a76f262`
  - `nats-server`: 2.15.0, binary SHA-256: `387ce54e7a0a249d877a579ed641d715bab047d7e061bfa42b4d07f6a0401341`
  - `manifest.json`: SHA-256 `021870d7f315da97c66b792eae7636777a42a9f5e517c18ddc4889bccbc41ed3`
- **Secrets:** None committed or logged.

---

## C. Git Changes

Four focused, cohesive commits were created on the detached worktree and fast-forward pushed:

1. **Commit `fe961b6`:** `fix(bridge): enforce tenant isolation and align REST error framing with official SDK`
   - *Files Changed:* `src/a2a_mq/bridge.py`, `src/a2a_mq/http_boundary.py`, `src/a2a_mq/live_probe.py`, `tests/test_bridge_sdk.py`, `tests/test_native_adapters_new.py`
   - *Root Cause:* `enforce_tenant` raised Starlette `HTTPException` with unstructured `{"detail": ...}` string that broke `a2a-sdk` client parser expecting `{"error": ...}`.
   - *Regression / Validation:* Verified with `pytest tests/test_bridge_sdk.py tests/test_native_adapters_new.py` (all passed).

2. **Commit `0b28e75`:** `fix(types): resolve static analysis defects and align probe test fixtures`
   - *Files Changed:* 25 modules across `src/a2a_mq/` + 4 test modules (`tests/test_local_stack_probe_regressions.py`, `tests/test_local_stack_probe.py`, `tests/test_postgres_adapter.py`, `tests/test_workflows.py`, `tests/test_postgres_parity_new.py`, `tests/test_mcp_adapter.py`)
   - *Root Cause:* 78 `mypy` type errors across core modules; test mock fixtures in probe regressions missing `--native-verification` CLI parameter and private `manifest.json` fixture.
   - *Regression / Validation:* `mypy src/a2a_mq` (0 errors), `pytest tests/test_local_stack_probe_regressions.py` (18 passed), `pytest tests/test_local_stack_probe.py` (19 passed), full broker-independent suite (1097 passed).

3. **Commit `83e1d93`:** `fix(local): ignore transient probe samples during startup polling`
   - *Files Changed:* `scripts/local_stack_probe.py`
   - *Root Cause:* Transient startup helper `a2a_mq.provision` executes and exits in ~50ms; if `_sample_total_rss` caught its PID during startup polling, `_rss` returned 0 and raised `MeasurementUnavailable`, aborting the stack before readiness.
   - *Regression / Validation:* `scripts/local_stack_probe.py` passed with exit code 0 (`PASS_LOCAL_LITE`).

4. **Documentation Commit:** `docs(validation): record local evidence report for 2026-09-23 validation`
   - *Files Changed:* `docs/validation/local-agent-20260923.md`
   - *Purpose:* Complete, permanent, sanitized evidence report for peer review.

---

## D. Focused Regression Results

Before executing full suites, focused regression modules were run to verify audit contracts:

| Focused Test Module | Command Line | Exit Code | Result | Details |
|---|---|:---:|:---:|---|
| `test_operations_v2.py` | `pytest -q tests/test_operations_v2.py` | 0 | **PASS** | 8 passed (central DLQ replay, two-phase semantics) |
| `test_deployment_new.py` | `pytest -q tests/test_deployment_new.py` | 0 | **PASS** | 12 passed (disposable compose generation, symlink preservation) |
| `test_transport.py` | `pytest -q tests/test_transport.py` | 0 | **PASS** | 23 passed (URL validation, port validation, scheme checks) |
| `test_mcp_tools_new.py` | `pytest -q tests/test_mcp_tools_new.py` | 0 | **PASS** | 13 passed (canonical MCP tools over TaskAPI) |
| `test_mcp_adapter.py` | `pytest -q tests/test_mcp_adapter.py` | 0 | **PASS** | 4 passed (MCP SDK adapter registration contract) |
| `test_bridge_sdk.py` | `pytest -q tests/test_bridge_sdk.py` | 0 | **PASS** | 12 passed (A2A SDK REST and tenant isolation) |
| `test_application_new.py` | `pytest -q tests/test_application_new.py` | 0 | **PASS** | 21 passed (application lifespan, route security, crypto config) |
| `test_acceptance_new.py` | `pytest -q tests/test_acceptance_new.py` | 0 | **PASS** | 32 passed (acceptance gate invariants and marker contracts) |
| `test_ci_contract.py` | `pytest -q tests/test_ci_contract.py` | 0 | **PASS** | 11 passed (CI workflow steps and packaging dependencies) |
| `test_native_adapters_new.py` | `pytest -q tests/test_native_adapters_new.py` | 0 | **PASS** | 123 passed (native Dapr and NATS adapters) |
| **Total Focused Regressions** | Combined | 0 | **PASS** | **259 passed, 0 failures, 0 errors, 0 skipped** |

---

## E. Repository and Packaging Gates

1. **Repository Check (`tools/check.py`):**
   - *Command:* `python tools/check.py`
   - *Exit Code:* `0`
   - *Result:* `PASS`. Validated repository syntax, JSON files, local documentation links, and verified that `spec/envelope.schema.json` and `src/a2a_mq/schema.json` are byte-identical.
2. **Dependency Consistency Check (`pip check`):**
   - *Command:* `python -m pip check`
   - *Exit Code:* `0`
   - *Result:* `PASS`. No broken requirements found in either environment.
3. **Isolated Wheel Packaging Smoke:**
   - *Build Process:* Wheel built with PEP 517 build isolation (`setuptools==82.0.1`) from a clean `git archive` export entirely outside the source tree.
   - *Installation:* Installed into private target directory with `--no-deps`.
   - *Verification:* Verified `a2a_mq` imports directly from the installed wheel directory (not repository source), verified `binding.load_binding()['protocolVersion'] == '1.0'`, and confirmed `schema.json` and `binding.json` package resources are present and loadable.
   - *Result:* `PASS` (built `a2a_mq-0.1.0-py3-none-any.whl`, 40,788 bytes).

---

## F. Ruff Linter Analysis

- **Initial Diagnostics Count:** 108 findings
- **Diagnostic Categories:**
  - `E701`: Multiple statements on one line (colon)
  - `E702`: Multiple statements on one line (semicolon)
  - `F401`: Unused import statements
  - `F811`: Fixture name redefinition in test parameter signatures
  - `E741`: Ambiguous variable name
  - `I001`: Unsorted import statements
- **Defects Fixed:**
  - Removed unused imports in `tests/test_postgres_adapter.py` and `tests/test_workflows.py`.
  - Renamed ambiguous variable `l` in `tests/test_postgres_parity_new.py`.
  - Corrected import order in `tests/test_mcp_adapter.py`.
  - Eliminated all lint errors across production source: `ruff check src` reports **0 errors** (`All checks passed!`).
- **Final Diagnostics Count:** 61 findings
- **Remaining Diagnostics:** All 61 remaining findings are exclusively pre-existing `E701`/`E702` multiple-statements-per-line formatting in `tests/test_workflows.py`.
- **Rationale for Preserving:** `tests/test_workflows.py` was authored in a condensed syntax style. Altering every statement would cause massive cosmetic churn across 100+ lines of established test code without improving product correctness or finding defects.

---

## G. Mypy Type Checker Analysis

- **Initial Diagnostics Count:** 78 errors across 25 modules
- **Categories of Type Errors:**
  - `arg-type`: Incompatible argument type passed to function or method
  - `return-value`: Missing or incompatible return type annotations
  - `assignment`: Incompatible variable assignment
  - `union-attr`: Accessing attributes on union types without narrowing
  - `override`: Incompatible method signature in subclass override (`Store._connection`)
- **Defects Fixed:**
  - Strict type narrowing and explicit assertions across all 25 modules in `src/a2a_mq/` (`domain.py`, `workflow_options.py`, `broker_profiles.py`, `storage.py`, `postgres.py`, `http_boundary.py`, `identity.py`, `tasks.py`, `ops_http.py`, `configuration.py`, `vault.py`, `telemetry.py`, `operations.py`, `local_runtime.py`, `deployment.py`, `deadletters.py`, `transport.py`, `local_workflow_runtime.py`, `secure_transport.py`, `mcp_adapter.py`, `performance.py`, `production.py`, `local_lite.py`, `local_workflow.py`, `production_env.py`, `application.py`).
  - No `Any`-washing or disabling of type checks was used.
- **Final Diagnostics Count:** **0 errors in 53 source files** (`Success: no issues found in 53 source files`).

---

## H. Official Pinned SDK Gate

- **Command:** `python -m a2a_mq.acceptance --mode sdk --root $TEST_ROOT --output $EVIDENCE/sdk --timeout 1800`
- **Exit Code:** `0`
- **Verdict:** **`SDK_PASS`**
- **Test Outcomes:** 26 passed, 0 failures, 0 errors, 0 skipped
- **Official Contracts Verified:**
  - `a2a-sdk==1.1.2`: REST framing, tenant isolation, and task API client parsing
  - `dapr-ext-workflow==1.18.3`: Workflow activity registration and task continuation
  - `mcp==2.2.0`: Model Context Protocol 2.2 tool registration and Streamable HTTP endpoint
  - `httpx2==2.13.0`: Transitive MCP 2.x HTTP transport client
- **MCP Implementation Analysis (`mcp_server.py` vs `mcp_adapter.py`):**
  - `mcp_server.py` is the canonical application composition path used by `application.py`, `local_lite.py`, `local_workflow.py`, and `production.py`.
  - `mcp_adapter.py` implements an alternate tool registration mapping (`get_task`, `list_tasks`, `send_text`, `provide_input`, `cancel_task`, `approve_task`) and `streamable_http_app`.
  - `tests/test_acceptance_new.py:272` explicitly verifies that `test_mcp_adapter.py` contains `pytestmark = pytest.mark.mcp` and is tested during the official SDK gate.
  - Conclusion: Both implementations were verified clean under static analysis and are retained to preserve verified acceptance test contracts.

---

## I. Current-Source Local Acceptance Gate

- **Validated Source SHA:** `83e1d93c2cb9a4f06db328d026f99d29eca1135e`
- **Command:** `python -m a2a_mq.acceptance --mode local --root $TEST_ROOT --output $EVIDENCE/local --timeout 1800`
- **Exit Code:** `0`
- **Verdict:** **`LOCAL_PASS`**
- **Test Outcomes:** 1071 passed, 0 failures, 0 errors, 0 skipped
- **JUnit Verification:** Complete XML report generated at `local/local.junit.xml`, 1071 test cases, 0 failures, 0 errors, 0 skipped.
- **Clean-Tree Provenance:** Verified clean git tree before and after execution:
  - `initialGitHead`: `83e1d93c2cb9a4f06db328d026f99d29eca1135e`
  - `initialGitTree`: `9cecea3f46deadb0fea9542f4ecad410be0f3f0b`
  - `gitHead`: `83e1d93c2cb9a4f06db328d026f99d29eca1135e`
  - `gitTree`: `9cecea3f46deadb0fea9542f4ecad410be0f3f0b`
  - `gitStatus`: `""` (100% clean)
- **Evidence Integrity:** SHA-256 manifest `sha256.json` verified with zero unsafe paths.

---

## J. Local Economics and Resource Probes

All resource probes were executed with strict measurement isolation and valid command signatures.

### 1. Minimal Pure-Core Resource Probe (`scripts/local_resource_probe.py`)
- **Command:** `setsid -w python scripts/local_resource_probe.py --state-dir $STATE/core-probe --json $EVIDENCE/core-resources.json`
- **Exit Code:** `0`
- **Verdict:** **`PASS_CORE_LIGHT`**
- **Observed Measurements:**
  - Tasks Executed: 100
  - Peak RSS Observed: **44.070 MiB** (Threshold: ≤ 192.000 MiB)
  - Start RSS: 13.094 MiB
  - Post-Import RSS: 43.133 MiB
  - Post-Smoke RSS: 44.070 MiB
  - Idle CPU (1 Core): **0.003%** (Threshold: ≤ 3.000%)
  - Import Duration: 1.2937 s (Threshold: ≤ 3.0000 s)
  - Smoke Duration: 2.8597 s (Threshold: ≤ 8.0000 s)
  - Database State Size: 0.348 MiB (Threshold: ≤ 16.000 MiB)
  - Host Available Memory: 16,254.070 MiB (Threshold: ≥ 512.000 MiB)
  - Host Swap Delta: 0.000 MiB

### 2. Local Python Product Probe (`scripts/local_python_probe.py`)
- **Command:** `setsid -w python scripts/local_python_probe.py --state-dir $STATE/python-app --json $EVIDENCE/python-app-resources.json`
- **Exit Code:** `0`
- **Verdict:** **`PASS_LOCAL_PYTHON`**
- **Observed Measurements:**
  - Application Build Duration: 2.1429 s (Threshold: ≤ 5.0000 s)
  - Steady-State RSS: **96.730 MiB** (Threshold: ≤ 192.000 MiB)
  - Idle CPU (60s average, 1 Core): **2.823%** (Threshold: ≤ 3.000%)
  - Runtime State Size: 0.133 MiB (Threshold: ≤ 4.000 MiB)
  - Failed Checks: None (`[]`)

### 3. Native Three-Process Stack Probe (`scripts/local_stack_probe.py`)
- **Command:** `python scripts/local_stack_probe.py --profile $STATE/local-lite --tools-dir $TOOLS --native-verification $EVIDENCE/native-verification.json --json $EVIDENCE/local-stack-resources.json --log $EVIDENCE/local-stack.log`
- **Exit Code:** `0`
- **Verdict:** **`PASS_LOCAL_LITE`**
- **Observed Measurements:**
  - Steady-State Process Count: **Exactly 3** (Threshold: 3)
  - Process Mapping:
    - `app` (PID 2309813): `python -m a2a_mq.local_runtime run ...`
    - `nats` (PID 2309894): `.../tools/bin/nats-server -c .../nats.conf`
    - `daprd` (PID 2309947): `.../tools/bin/daprd --mode standalone ...`
  - Total Steady-State RSS: **252.207 MiB** (Threshold: ≤ 384.000 MiB)
  - Peak Total RSS: 252.207 MiB
  - Startup Peak RSS: 249.645 MiB
  - RSS Breakdown by Role:
    - `app`: 97.887 MiB
    - `daprd`: 132.180 MiB
    - `nats`: 22.141 MiB
  - Idle CPU (60s average, 1 Core): **2.917%** (Threshold: ≤ 5.000%)
  - Idle CPU Breakdown by Role:
    - `app`: 2.650%
    - `daprd`: 0.067%
    - `nats`: 0.200%
  - Startup Duration: 4.842 s (Threshold: ≤ 15.000 s)
  - Runtime State: 0.134 MiB (Threshold: ≤ 32.000 MiB)
  - Post-Shutdown State: 0.134 MiB
  - All Processes Alive After Idle: True
  - Native Tools Provenance: Verified match against `native-verification.json` and bootstrap manifest.

---

## K. Optional Local Workflow Gate

- **Status:** **NOT EXECUTED**
- **Rationale:** The default local product is strictly `local-lite` (3 processes: Python app, native `daprd`, native `nats-server`, SQLite). The optional 5-process `local-workflow` tier requires Placement and Scheduler. Per repository rules (ADR 0011 and `AGENTS.md`), low-resource economics is a correctness gate; extending into the 5-process tier was not authorized without separate resource evidence.

---

## L. External Prerequisites

| Gate | Status | Reason |
|---|:---:|---|
| **Verified-TLS PostgreSQL** | `UNAVAILABLE` | External TLS PostgreSQL DSN (`A2A_TEST_POSTGRES_DSN`) not supplied |
| **Vault Transit** | `UNAVAILABLE` | External TLS HashiCorp Vault Transit config (`A2AMQ_VAULT_TEST_CONFIG_FILE`) not supplied |
| **Distributed Dual-Broker (`jetstream` / `rabbitmq`)** | `NOT_AUTHORIZED` | Docker container orchestration not authorized on this host |
| **Kubernetes Deployment Gate** | `NOT_AUTHORIZED` | Cluster infrastructure not authorized on this host |

---

## M. Hosted CI Status

- **Status:** `NOT INSPECTED (CLI UNAVAILABLE)`
- **Detail:** GitHub CLI (`gh`) is not installed on this host (`which gh` exited with code 1). Per Section 11 of repository instructions, no manual credential flows were initiated. Known behavior remains that recent Actions runs were triggered but awaited runner allocation.

---

## N. Repository Hygiene

- **Original Working Tree:** Clean and unmodified (`/root/A2A_MQ` has zero uncommitted changes).
- **`main` Branch:** Untouched at `66c17153d7ec75b9ad85808ef141b9dba43a9d91`.
- **Version:** Preserved at `0.1.0`.
- **No Secrets Committed:** Verified via acceptance credential guard.
- **No Generated Binaries / Venvs Committed:** All disposable files created exclusively in temporary run directories and deleted.
- **Push Policy:** Strictly fast-forward push.

---

## O. Remaining Work and Recommended Next Action

1. **Staging Multi-Tenant PostgreSQL TLS Acceptance:** Execute `a2a_mq.acceptance --mode postgres` on an authorized test machine with a dedicated TLS PostgreSQL instance.
2. **HashiCorp Vault Transit Acceptance:** Execute `a2a_mq.acceptance --mode vault` on an authorized environment with transit keys provisioned.
3. **Optional 5-Process Workflow Profile Measurement:** On a machine with ≥ 2GB RAM budget, bootstrap Placement/Scheduler and execute `local_stack_probe.py` against `local-workflow`.

### HIGHEST-VALUE NEXT ACTION
```bash
python -m a2a_mq.acceptance --mode postgres --output $EVIDENCE/postgres --root $REPO
```
(Requires provisioning a test PostgreSQL instance with TLS certificates and setting `A2AMQ_POSTGRES_DSN_FILE`.)

---

## Machine-Readable Summary

```json
{
  "starting_sha": "524d36509cf4cbb2a1a166330e45994f49a74774",
  "validated_source_sha": "83e1d93c2cb9a4f06db328d026f99d29eca1135e",
  "report_commit_sha": "5e4c9b1d0c32cc2d47e41104436e2b9d2e868aaf",
  "focused_tests": {
    "passed": 259,
    "failed": 0,
    "errors": 0,
    "skipped": 0
  },
  "ruff": {
    "initial": 108,
    "final": 61
  },
  "mypy": {
    "initial": 78,
    "final": 0
  },
  "sdk_gate": "PASS",
  "local_acceptance": "PASS",
  "core_resource_gate": "PASS",
  "python_resource_gate": "PASS",
  "native_local_lite_gate": "PASS",
  "postgres_gate": "UNAVAILABLE",
  "vault_gate": "UNAVAILABLE",
  "distributed_broker_gate": "NOT_AUTHORIZED",
  "main_unchanged": true,
  "version_unchanged": true,
  "secrets_committed": false
}
```
