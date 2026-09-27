# Usability & Onboarding Implementation Validation — 2026-09-27

## Scope

This record documents the independent validation of the A2A-MQ usability and onboarding
implementation on real Linux, covering non-mutating `doctor` preflight, actionable CLI
error handling, Local-lite runtime, smoke script, official A2A JSON-RPC examples, and
Workflow runtime.

## 1. System Environment

- **Operating System:** Ubuntu 24.04.5 LTS (Noble Numbat)
- **Kernel:** `6.8.0-139-generic` (`#139-Ubuntu SMP PREEMPT_DYNAMIC Sat Aug 1 03:52:05 UTC 2026`)
- **Architecture:** `x86_64`
- **Python Version:** `Python 3.12.3` (`/usr/bin/python3`)

## 2. Git Provenance

- **Starting `main` SHA:** `97d15da659e4e51f94d403148dae08edb279f1bf`
- **Starting Branch SHA:** `8f99db2db642117d089f05ed6975838dd5f6accf`
- **Final Tested Branch SHA:** `401471b05da55ba8311b9bc6e5019a24a4250b9b`
- **Merge Commit SHA:** `84097baa57da4919c1672a0392397e05c0404882`
- **Package Version:** `0.1.0` (in `pyproject.toml`)
- **Release Candidate Tags:**
  - `refs/tags/v0.1.0-rc1`: tag object `9bc710ed96b625fd68774a77d0542a3a11786601`, commit `3d2b2afd2dbbeb447dab21d1e71afb428e523e8f` (unchanged)
  - `refs/tags/v0.1.0-rc2`: tag object `26faf43fcc41221d42e1e6f4829ea4e5c99e56b4`, commit `4b1fc34065e37b68258f1944fbcb1e9a15f5fe6d` (unchanged)

## 3. Commits Added During Validation

- **Commit:** `401471b` (`fix(local): allow client JWT reading in doctor preflight without size truncation`)
- **Reason:** In `src/a2a_mq/local_runtime.py`, `_read()` had a hardcoded `limit=512` byte ceiling originally intended for loopback API tokens. When `_client_token_check` called `_read(root / "client.jwt")`, reading the generated RS256 client JWT (~705 bytes) exceeded this limit, throwing `PermanentError("INVALID_CONFIG_FILE")`. This caused `doctor` to fail the client JWT check and always report `not-ready` on a newly prepared profile. The causal defect was fixed minimally by adding an optional `limit: int = 512` argument to `_read()` and passing `limit=65536` when reading `client.jwt`.

## 4. Python Environment & Repository Static Gate

```text
python -m pip check
=> No broken requirements found.

python tools/check.py
=> Repository syntax, JSON, schema/binding copies, docs links, user scripts and configuration coverage checks passed
```

Verified checks in `tools/check.py`:
- Python syntax across `src`, `tests`, `deploy`, `tools`, `scripts`
- Spec/schema copy byte identity: `spec/envelope.schema.json` == `src/a2a_mq/schema.json`
- Spec JSON validation
- Markdown internal link validation across docs, `README.md`, `CHANGELOG.md`
- Documented Python script references
- Script entry points in `pyproject.toml` (`a2amq-local`, `a2amq-local-workflow`)
- Required user onboarding files existence
- 100% configuration coverage: all environment variables in `src/a2a_mq/production_env.py` documented in `docs/CONFIGURATION.md`
- Exact version consistency for Dapr (`1.18.3`) and NATS (`2.15.0`) between runtime and native bootstrap

## 5. Automated Tests

Focused test suites:
- `tests/test_local_runtime.py`: 27 passed
- `tests/test_local_launcher_regressions.py`: 16 passed
- `tests/test_production_env.py`: 29 passed
- `tests/test_local_lite.py`: 10 passed
- `tests/test_queue_lab.py`: 11 passed
- **Total focused:** 93 passed in 19.08s (0 failures, 0 errors)

Complete repository test suite:
- Broker-independent suites: 1,162 passed without regressions.
- Pinned external-infrastructure suites (live PostgreSQL, live TLS Vault Transit, Kubernetes deployment) cleanly require their respective external systems.

## 6. Exact Native Provenance

### Local-lite Tools

- Command: `python scripts/check_local_native_versions.py`
- Verdict: `PASS_NATIVE_VERSION`
- Dapr `daprd`: `1.18.3` (allcomponents, SHA-256: `041c4c3ab9b0d3449faf9d0876377062f1cdcda64d3109989645a9f57a76f262`)
- NATS Server: `2.15.0` (SHA-256: `387ce54e7a0a249d877a579ed641d715bab047d7e061bfa42b4d07f6a0401341`)
- Bootstrap Manifest SHA-256: `021870d7f315da97c66b792eae7636777a42a9f5e517c18ddc4889bccbc41ed3`

### Workflow Tools

- Command: `python scripts/check_local_native_versions.py`
- Verdict: `PASS_NATIVE_VERSION`
- Dapr `daprd`: `1.18.3`
- NATS Server: `2.15.0`
- Dapr `placement`: `1.18.3` (SHA-256: `0d5db7d35dddc5de11a65c438e1a4cf7901d62948db408b02252e0537b3ccd78`)
- Dapr `scheduler`: `1.18.3` (SHA-256: `fce7104deaaf276c3e0eee2b3eab04e7fd0b70fae241c575dabee682ae9d2bc1`)
- Bootstrap Manifest SHA-256: `8dd1456e5038d8c7625dc80bb12fa539fbe70aee5c77e9f56466c46e29f78864`

## 7. `a2amq-local doctor` Validation

1. **Without profile:** Exit code `0`, reported prerequisites without modifying the filesystem.
2. **With prepared profile:** Exit code `0`, status `READY`.
   - Python: PASS (`3.12.3`)
   - Python extras: PASS (`installed`)
   - profile: PASS (`local-lite; tenant=demo; source=local-worker`)
   - daprd: PASS (`1.18.3`)
   - nats-server: PASS (`2.15.0`)
   - native provenance: PASS (`PASS_NATIVE_VERSION`)
   - startup ports: PASS (`available`)
   - state directory: PASS (`state`)
   - client JWT: PASS (`signature, issuer and audience valid`)
3. **Non-mutating verification:** SHA-256 checksums of `client.jwt`, `jwt-private.pem`, `dapr-api-token`, and `nats-token` were identical before and after doctor execution. Zero child processes were started.

## 8. Real Negative Doctor & Error UX Checks

1. **Occupied loopback port (18000):**
   - Exit code: `2`
   - Output: `startup ports FAIL occupied: 18000`
   - Fix: `stop the conflicting service or choose a different host before starting A2A-MQ`
   - Status: `NOT_READY`
   - External listener remained unaffected; profile files remained unmutated.
2. **Missing native binary:**
   - Exit code: `2`
   - Output: `daprd FAIL not found`, `nats-server FAIL not found`
   - Fix: `bootstrap the repository-pinned native tools and prepend its bin directory to PATH`
   - Status: `NOT_READY`
   - No Python traceback; profile unmutated.
3. **Prepare against existing profile:**
   - Exit code: `2`
   - Output: `ERROR: OUTPUT_ALREADY_EXISTS`
   - Fix: `choose a different profile directory or remove the old profile after stopping it`
   - No Python traceback.

## 9. Local-lite Live Runtime & Smoke

1. **Topology:** Exactly 3 steady-state processes (`a2amq-local`, `daprd`, `nats-server`). No Placement or Scheduler required.
2. **Health Endpoints:**
   - `/health/live`: `{"status":"live"}`
   - `/health/ready`: `{"status":"ready"}`
   - `/.well-known/agent-card.json`: Valid Agent Card
3. **Automated Smoke (`scripts/smoke_local.py`):**
   - Output: `PASS_LOCAL_SMOKE` (elapsed: 0.427s)
   - Verified `/health/live`, `/health/ready`, Agent Card, authenticated `SendMessage`, task execution through broker/sidecar to `TASK_STATE_COMPLETED`, `GetTask`, and authenticated `/operations/status`.

## 10. API Examples Validation

- All 6 JSON files in `examples/a2a/*.json` are valid JSON.
- Tested `examples/a2a/send-message.json` and `examples/a2a/list-tasks.json` with `curl` against `/rpc`:
  - Request accepted (`TASK_STATE_SUBMITTED`).
  - Visible through `ListTasks`.
  - Completed and retrieved through `GetTask` (`TASK_STATE_COMPLETED`).
- Verified placeholder IDs (`replace-with-task-id`, etc.) and non-terminal subscription semantics are clearly documented in `docs/API_EXAMPLES.md`.

## 11. Workflow Doctor & Live Runtime

1. `a2amq-local-workflow doctor`:
   - Exit code: `0`, status `READY`.
   - Detected `daprd`, `nats-server`, `placement`, and `scheduler`.
   - Pinned provenance confirmed `PASS_NATIVE_VERSION`.
   - Non-mutating verification passed.
2. Live Workflow Runtime (`a2amq-local-workflow run`):
   - Health check `/health/live`: `{"status":"alive"}`
   - Readiness check `/health/ready`: `{"status":"ready"}`
   - 5 steady-state processes verified (`a2amq-local-workflow`, `nats-server`, `placement`, `scheduler`, `daprd`).

## 12. Clean Shutdown Evidence

After cleanly terminating both Local-lite and Workflow runtimes:
- `ps aux | grep usability-test`: 0 remaining processes
- `ss -tulpn | grep -E ':(18000|3500|50001|50002|50005|50006|4222)\b'`: 0 bound ports
