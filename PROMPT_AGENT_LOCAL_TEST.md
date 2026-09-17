# /goal — Independently validate A2A-MQ locally and fix only proven defects

You are working **locally inside this A2A_MQ repository**. Treat the checked-out commit as the system under test. Your job is to perform an independent, adversarial validation of the integrated implementation, find real defects before making performance claims, fix defects only when they are reproducible and well understood, and leave a compact evidence package that another engineer can audit.

## Operating principles

1. **Start with a deliberate bug hunt and deterministic validation.** Read `AGENTS.md`, `README.md`, `docs/IMPLEMENTED_STACK.md`, `docs/IMPLEMENTATION_COVERAGE.md`, the relevant ADRs, `pyproject.toml`, acceptance code, deployment code, and the production/runtime entry points before drawing conclusions.
2. **Do not trust prior evidence or pass counts.** Re-run tests from this checkout. Existing files under `docs/validation/` are historical context only.
3. **Preserve settled architecture unless evidence falsifies it.** The integrated entry point is `a2a_mq.application:from_env`; existing `production.py` / `production_env.py`, `service:from_env`, and legacy/demo paths are intentionally preserved. Do not collapse these profiles merely to simplify tests.
4. **Prefer correctness and reproducibility over speed.** Runtime duration is not important. Token usage and unnecessary rework should be minimized.
5. **Use positive, evidence-driven changes.** If you find a defect, first create the smallest reproducer/regression test, then fix the root cause, then rerun the relevant narrow test and the broader affected suite.
6. **Never turn a missing dependency/service into a pass.** A required gate that cannot run is `INCOMPLETE`, not `PASS`. Required skips, xfails, empty JUnit files, import fallbacks, mocked native SDKs, or connection failures cannot satisfy acceptance.
7. **Keep the run local and safe.** Use disposable databases/containers/credentials. Do not contact production services, rotate/delete real Vault keys, alter external policies, or use paid model APIs. This project does not require an LLM API key.
8. **Do not push.** You may create local commits for fixes if useful, but leave pushing/merging to the owner.

## Phase 0 — Provenance and clean baseline

Record:

```bash
git rev-parse HEAD
git status --short
python --version
python -m pip --version
docker --version || true
docker compose version || true
```

The worktree should start clean. If it is not clean, preserve and report the pre-existing changes; do not discard them.

Create a fresh evidence directory outside the repository, for example:

```bash
EVIDENCE="$(mktemp -d /tmp/a2amq-agent-validation.XXXXXXXX)"
printf '%s\n' "$EVIDENCE"
```

## Phase 1 — Static bug hunt before broad execution

Inspect at minimum these areas as one system rather than isolated files:

- `src/a2a_mq/application.py`, `configuration.py`, `deployment.py`, `provision.py`
- `postgres.py`, `storage.py`, `tasks.py`, `workflows.py`, `recovery.py`, `schema_lifecycle.py`
- `deadletters.py`, `dlq_callback.py`, `maintenance.py`, `sqlite_archive.py`, `pg_backup.py`
- `bridge.py`, `task_api.py`, `mcp_server.py`, `mcp_adapter.py`
- `identity.py`, `protection.py`, `secure_transport.py`, `vault.py`
- `operations_http.py`, `admin.py`, `telemetry.py`, `projections.py`
- `acceptance.py`, `live_probe.py`, `performance.py`
- both deployment paths already present in the repository and all new tests.

Look specifically for correctness failures involving:

- tenant/consumer/owner isolation and authorization bypasses;
- message/task ID reuse, idempotency, stale leases/fences and duplicate delivery;
- PostgreSQL/SQLite semantic drift, transaction boundaries and schema races;
- outbox ambiguity, replay resurrection, DLQ tampering/truncation/expiry, encrypted-route downgrade;
- Workflow restart/reconciliation, cancellation races, waits, fan-out/fan-in, and terminal-state rewrites;
- cleanup/retention deleting data before runtime completion or publication drain;
- startup/shutdown partial failure, resource leaks, readiness lying, duplicated relays;
- MCP/A2A operations bypassing the common `Principal` authorization model;
- secret leakage in command lines, logs, telemetry, manifests, generated evidence or exception strings;
- deployment broker parity differences between JetStream and RabbitMQ;
- test gates that can accidentally pass because the real SDK/service did not execute.

Write concrete findings with file/line references into `$EVIDENCE/bug-hunt.md`. Do not invent issues merely to fill a quota.

## Phase 2 — Fresh environment, dependencies, packaging and static gates

Create/recreate a local virtual environment and install the complete development stack:

```bash
python3 -m venv .venv
. .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -e '.[dev,bridge,workflow,postgres,security,mcp,observability,deployment]'
python -m pip check
ruff check .
mypy
```

Then build and install a wheel into a directory outside the source tree. Verify that imports and package data (`schema.json`, `binding.json`, `py.typed`) work **without relying on `PYTHONPATH=src` or the source checkout**.

Any dependency-resolution or wheel failure is a real failure to report.

## Phase 3 — Complete deterministic local regression

First run the repository-wide non-live suite, not only the newly added tests:

```bash
python -m pytest -p no:cacheprovider -W error -q \
  -m 'not live and not postgres' \
  --junitxml="$EVIDENCE/non-live.xml" \
  2>&1 | tee "$EVIDENCE/non-live.log"
```

Then run the project's strict local acceptance driver:

```bash
python -m a2a_mq.acceptance --mode local --root "$PWD" \
  --output "$EVIDENCE/acceptance-local"
```

Also execute focused concurrency/restart/recovery/DLQ/retention/security tests independently when broad output makes diagnosis difficult.

A test run with unexpected skips is not accepted. Record the exact collected/passed/skipped/failed counts.

## Phase 4 — Native protocol SDK gate

Run the official A2A, MCP and Dapr SDK-backed acceptance path:

```bash
python -m a2a_mq.acceptance --mode sdk --root "$PWD" \
  --output "$EVIDENCE/acceptance-sdk"
```

Verify that the tests actually import and exercise the pinned native SDKs. Pay special attention to:

- Agent Card resolution;
- JSON-RPC and HTTP+JSON send/get/list/cancel;
- streaming and reconnect semantics;
- unsupported push-notification behavior;
- MCP authentication and owner-scoped tools;
- malformed/oversized bodies and authorization denials.

Do not replace a missing native SDK with fixtures to make this gate green.

## Phase 5 — Real disposable PostgreSQL

Use a dedicated disposable PostgreSQL database. Prefer a fresh local container if no disposable database is already available. Store the DSN in a private file outside the repository and point `A2AMQ_POSTGRES_DSN_FILE` at it. Use TLS where the acceptance path requires verified TLS; if a specific legacy gate explicitly supports an isolated insecure local fixture, keep that exception scoped to that test only.

Run:

```bash
python -m a2a_mq.acceptance --mode postgres --root "$PWD" \
  --output "$EVIDENCE/acceptance-postgres"
```

Independently stress at least:

- two replicas racing to claim the same command;
- duplicate command/result handling;
- restart/reopen persistence;
- task/consumer isolation;
- schema initialization by concurrent roles;
- retention/replay interactions;
- backup/restore on a disposable DB.

Inspect the DB after tests for leaked test rows or cross-scope corruption.

## Phase 6 — Real disposable Vault Transit

Only run this phase against a dedicated local/disposable Vault Transit test key. Prepare `A2AMQ_VAULT_TEST_CONFIG_FILE` exactly as documented in `docs/IMPLEMENTED_STACK.md`. The key must provide the versions expected by the test; never mutate a real production key or policy.

Run:

```bash
python -m a2a_mq.acceptance --mode vault --root "$PWD" \
  --output "$EVIDENCE/acceptance-vault"
```

Verify encryption/decryption, key-version read windows, tamper rejection, receiver/tenant binding, replay behavior and plaintext downgrade rejection. Search logs/evidence for Vault tokens, plaintext secret material and wrapped/unwrapped key leakage.

If a safe disposable Vault is unavailable, mark this phase `INCOMPLETE` with the exact missing prerequisite.

## Phase 7 — Real JetStream and RabbitMQ end-to-end stacks

You may create disposable containers only. Use two fresh output directories.

JetStream example:

```bash
ROOT_JS="$(mktemp -d /tmp/a2amq-js.XXXXXXXX)"
python -m a2a_mq.deployment compose --broker jetstream --source "$PWD" --output "$ROOT_JS/stack"
docker compose -f "$ROOT_JS/stack/compose.yaml" up --build -d
```

Generate and start a separate RabbitMQ stack with `--broker rabbitmq`.

For each broker, validate real behavior rather than only manifest rendering:

- actual command publication and worker competition;
- independent observer fan-out;
- duplicate/redelivery behavior;
- API process restart and worker restart while work is in flight;
- Dapr Workflow restart/reconciliation;
- input-required and auth-required continuations;
- cancellation before/during work;
- fan-out/fan-in completion;
- A2A native client and streaming;
- MCP tools;
- DLQ capture and replay;
- authorization denials and tenant/owner isolation;
- broker TLS/authentication and generated secret handling;
- cleanup without corrupting persisted task state.

Inspect container logs for exceptions, retries that never converge, unbounded loops, secret/payload leakage and false-ready states.

Always tear down the disposable stacks and volumes you created after evidence has been collected.

## Phase 8 — One strict full gate

When PostgreSQL, Vault and Docker prerequisites are all genuinely available, run:

```bash
python -m a2a_mq.acceptance --mode full --root "$PWD" \
  --output "$EVIDENCE/acceptance-full" \
  --allow-disposable-containers --timeout 1200
```

`FULL_PASS` is valid only if every required group executes successfully. Verify the generated verdict rather than trusting process exit alone.

If full mode cannot run because a prerequisite is unavailable, do not weaken the gate. Report `INCOMPLETE` and list the exact missing prerequisite(s).

## Phase 9 — Performance only after correctness

Only after correctness gates are green, inspect `python -m a2a_mq.performance --help` and run a small repeatable local benchmark against the disposable stack. Measure at minimum admission latency and throughput under modest concurrency. Performance must never override a correctness failure, and do not compare against historical numbers that were not measured in this run.

## Defect-fix loop

For every reproducible defect:

1. capture the minimal failing command/test and exact observed behavior;
2. add or strengthen a regression test that fails for the right reason;
3. implement the smallest root-cause fix consistent with the architecture;
4. rerun the focused test;
5. rerun the affected broader gate;
6. keep unrelated refactors out of the fix;
7. record the change and evidence in `$EVIDENCE/fixes.md`.

If a suspected issue cannot be reproduced, document it as unconfirmed rather than modifying code speculatively.

## Final deliverable

Produce `LOCAL_VALIDATION_REPORT.md` in the repository root and leave the raw/sanitized evidence under the external `$EVIDENCE` directory. The report must contain:

- tested Git SHA and whether the initial/final worktree was clean;
- environment/tool versions;
- bug-hunt findings, classified as fixed / confirmed-open / unconfirmed;
- exact commands executed;
- for every gate: `PASS`, `FAIL`, or `INCOMPLETE` — never infer PASS;
- exact test counts and unexpected skips/xfails;
- PostgreSQL/Vault/JetStream/RabbitMQ evidence actually executed;
- security and secret-leak checks;
- any local commits created for fixes;
- remaining release blockers;
- a concise final verdict distinguishing **implementation correctness evidence** from **production certification**.

End your response with a compact table of gates and statuses plus the paths to `LOCAL_VALIDATION_REPORT.md` and the external evidence directory. Do not push anything.
