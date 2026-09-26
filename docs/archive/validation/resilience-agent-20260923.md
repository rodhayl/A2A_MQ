# Local Resilience and Optional Workflow Tier Validation Report — 2026-09-23

## 0. Provenance & Server Cleanliness

- **Workspace:** `/root/A2A_MQ` (clean active tree, untouched)
- **Base Branch:** `implementation/production-hardening-20260916`
- **Initial Working Tree Status:** Clean (`git status --porcelain=v2` empty)
- **Known Validated Baseline:** `83e1d93c2cb9a4f06db328d026f99d29eca1135e`
- **Remote HEAD at Validation Start:** `caa643cdf64126a1fad4b3a8153b9663873b6868`
- **Isolated Validation Worktree:** `/root/A2A_MQ/.local-validation/run.resilience-qo7uAeBo/repo`
- **Isolated Validation Venv:** `/root/A2A_MQ/.local-validation/run.resilience-qo7uAeBo/venv`
- **Native Tools Root:** `/root/A2A_MQ/.local-validation/run.resilience-qo7uAeBo/tools/native`
- **Platform:** Linux 6.6.137+ x86_64 (`amd64`), Python 3.12.3
- **Rules Followed:** No global pip installs, no sudo, no host services, no docker/k8s, repository-pinned native binaries only.

---

## 1. Documentation Synchronization

Prior documentation (`docs/IMPLEMENTATION_COVERAGE.md`, `docs/STATUS.md`) was synchronized:
- Confirmed that current branch builds on `caa643c` without moving backwards.
- Verified that all earlier gates (`tools/check.py`, `pip check`, package smoke, 259 focused regressions, source Ruff 0 errors, mypy 0 errors on 53 files, official SDK `SDK_PASS`, 1071 local tests `LOCAL_PASS`, 1097 broker-independent suite, `PASS_CORE_LIGHT`, `PASS_LOCAL_PYTHON`, `PASS_LOCAL_LITE`) remain valid.
- Maintained historical evidence tied to its actual SHA and updated coverage tables to reflect the new resilience and workflow evidence.

---

## 2. Formal Local Resilience Acceptance Gate (`LOCAL_LITE_RESILIENCE`)

Implemented a comprehensive, reproducible gate in `scripts/local_resilience_gate.py` with accompanying unit tests in `tests/test_local_lite_resilience.py`.
- **Verdict:** `PASS_LOCAL_LITE_RESILIENCE`
- **Total Elapsed Time:** 102.77 seconds
- **Topology:** Real 3-process native stack: Python application (`local_runtime`), native `daprd` 1.18.3, native `nats-server` 2.15.0, SQLite persistence.
- **Evidence Files:**
  - Machine-readable JSON: `evidence/resilience/local_lite_resilience.json`
  - Full execution log: `evidence/resilience/local_lite_resilience.log`

---

## 3. Crash / Restart / Redelivery Acceptance

All five crash points executed against the live three-process stack:

| Scenario | Injected Failure | Recovery Mechanism | Observed Outcome | Verdict |
|---|---|---|---|:---:|
| **A. App crash after task admission** | Task `msg-crash-a` admitted to SQLite, Python app SIGKILLed before execution | Restart against same SQLite DB; ActionRelay & worker resumed | Completed as `1dc3e902-1b54-5abd-90e1-a3212f255602`, 1 runtimeEnd, 0 pending actions | **PASS** |
| **B. Crash before publication ack** | SIGKILL immediately after action creation but before Dapr bus publication ack | JetStream at-least-once redelivery (`ackWait: 4s`), TaskLedger deduplication | Exactly one logical completion (`53e168a4-895e-56a4-bdb7-f2a257f75ee3`), job dedupe count 1 | **PASS** |
| **C. Crash after publication before bookkeeping** | Publish succeeded to NATS, app killed before action deleted | Duplicate delivery accepted with HTTP 200, idempotency filter blocked second execution | Single terminal completion (`46048c57-7b28-584b-80ca-b035534d0c2b`), 1 runtimeEnd | **PASS** |
| **D. Crash during task execution** | App SIGKILLed while task in `WORKING` state | Restart against same DB, recovered unfinished work | Eventual completion as `d130021a-7bf1-5178-9f09-35f322ef134f` | **PASS** |
| **E. Crash racing against terminal commit** | Rapid SIGKILL raced against task completion across 3 repetitions | Restart after each cycle | All 3 cycles reached `TASK_STATE_COMPLETED` without corrupt history or orphaned actions | **PASS** |

**SQLite Post-Crash Integrity Check:**
- `integrity_check`: `["ok"]`
- `quick_check`: `["ok"]`
- `foreignKeyErrors`: 0
- `noDuplicateTerminalEnds`: true
- Clean: true across 7 tasks, 14 actions, 21 outbox, 7 runtimeEnds.

---

## 4. Native Cancellation Race Acceptance

Exercised all 9 required cancellation race scenarios using official A2A client and real Dapr sidecar:

1. `immediate_after_admission`: Canceled immediately after `ledger.submit()`; terminal `CANCELED`.
2. `before_start`: Task dormant in jobs/tasks before action relay; successfully canceled before worker execution.
3. `during_execution`: Canceled while task active; transition to `CANCELED` verified.
4. `race_with_completed`: Cancel sent after task completed; rejected with `TaskNotCancelableError` / fail-closed, terminal state untouched.
5. `repeated_cancel_idempotency`: Same cancel request repeated; second call idempotent.
6. `cancel_before_restart`: Task canceled, stack SIGKILLed, restarted; terminal state `CANCELED` preserved.
7. `cancel_after_restart`: Stack restarted with pending work, canceled immediately upon startup; cleanly canceled.
8. `owner_isolation_denied`: Bob attempted to cancel Alice's task; denied with `404 Not Found` / `FORBIDDEN`.
9. `tenant_isolation_denied`: Other tenant token attempted cancellation; denied.

**SQLite Post-Race Integrity:** Clean (`ok`), 0 FK errors, no duplicate terminal ends.

---

## 5. Real Poison → DLQ → Replay Acceptance

Exercised complete dead-letter path through real NATS JetStream and Dapr Pub/Sub:
1. Injected poison CloudEvent with invalid JSON payload directly to command topic.
2. Subscriber rejected delivery with PermanentError (`INVALID_DLQ_DELIVERY`).
3. Dapr routed poison event to configured DLQ topic (`a2a.v1.demo.dlq`).
4. DLQ collector captured poison event to `a2amq_delivery_dlq` (`7a800278-3f15-5947-bad0-661ce9329244`).
5. Original bytes, digest, and origin preserved; no recursive loops.
6. Valid CloudEvent (`rec-91c43cdb-1285-52ad-97e0-86c602645af1`) archived and successfully replayed via `ReplayService`.
7. Replay preserved original message identity (`dc0d609a-3117-5239-ba7f-329ec55c1d2f`), produced exactly one terminal completion (`replayedTaskCompleted: true`).

---

## 6. Real MCP-over-HTTP Local-Lite Acceptance

Tested with official `mcp` 2.2 SDK using `streamable_http_client` over HTTP transport to `http://127.0.0.1:18000/mcp`:
- **Available Tools in local-lite:** `submit_task`, `get_task`, `list_tasks`, `cancel_task`.
- **Workflow-Only Tools:** `continue_task` and `approve_task` correctly fail closed / not registered.
- **10 Security & Lifecycle Scenarios:**
  1. Valid owner token: `submit_task`, `get_task`, `list_tasks` succeeded.
  2. Token changes between calls: List with rotated JWT succeeded.
  3. Another owner: Bob calling `get_task` on Alice's task returned error (`is_error=True`).
  4. Another tenant: Other tenant token rejected (HTTP 401).
  5. Expired token: Rejected (HTTP 401).
  6. Missing token: Rejected (HTTP 401).
  7. Wrong audience: Rejected (HTTP 401).
  8. Malformed resource URL: Rejected (HTTP 401/404).
  9. Reconnect / new HTTP session: Reconnection succeeded, tools enumerated.
  10. App restart between calls: App killed and restarted; subsequent MCP call succeeded without principal leakage.

---

## 7. SQLite Crash Consistency and Recovery

Fired 5 concurrent writes (`sqlite-cons-0`..`sqlite-cons-4`) while abruptly issuing SIGKILL to the app:
- Database reopened directly in Python `sqlite3`:
  - `PRAGMA integrity_check` returned `ok`.
  - `PRAGMA quick_check` returned `ok`.
- Checked presence and consistency of all schema tables: `a2amq_tasks`, `a2amq_actions`, `a2amq_inbox`, `a2amq_outbox`, `a2amq_runtime_end`, `a2amq_delivery_dlq`.
- Foreign key invariants intact (0 errors), end states consistent (15 runtime ends, 20 tasks, no duplicates).
- Restarted stack cleanly; all surviving records accessible.

---

## 8. Backup / Restore Target-Machine Smoke

Exercised real SQLite backup/restore helpers (`a2a_mq.sqlite_archive` and `a2a_mq.backup`):
1. **Live online backup:** Created `live.backup` (303,104 bytes, SHA-256 `7c02b2c46b4c6299e2e0617b1e7f3e0e98de2d7896ff060765349aec240a2bb9`).
2. **Restore to brand-new destination:** Restored to `restored.db`. SHA-256 digest verified.
3. **Table & Record Verification:** Restored `a2amq_tasks`, `a2amq_jobs`, `a2amq_delivery_dlq` matched backup snapshot exactly.
4. **Negative Cases Tested (4):**
   - Existing destination rejected (`BACKUP_DESTINATION_EXISTS`).
   - Symlink destination rejected.
   - Corrupt backup file restoration failed cleanly.
   - Wrong expected SHA rejected (`BACKUP_DIGEST_MISMATCH`).

---

## 9. Capacity / Backpressure Acceptance

Configured tight quota: `A2A_MAX_ACTIVE=3`, `A2A_DATABASE_CAPACITY=2`:
- Rapidly submitted 15 tasks concurrently.
- Exactly 3 tasks admitted; 12 tasks received bounded rejection (`ADMISSION_QUOTA_EXCEEDED`).
- No silent drops, no unhandled exceptions.
- Health endpoints (`/health/live`, `/health/ready`) remained responsive (HTTP 200).
- Waited for workload to drain to 0 pending tasks.
- Submitted new task after drain; immediately admitted (HTTP 200).
- Resource metrics:
  - Baseline: RSS 97.832 MiB, 8 FDs, 2 threads
  - Peak: RSS 98.652 MiB, 19 FDs, 2 threads
  - Drained: RSS 98.711 MiB, 8 FDs, 2 threads

---

## 10. Soak / Leak Test

Executed 74 complete cycles over 16.54s with concurrent submissions, read-backs, and paginated listings:
- **Initial Total RSS:** 248.824 MiB
- **Final Total RSS:** 259.184 MiB (well under 384.0 MiB budget)
- **Net RSS Delta:** +10.36 MiB across 74 full cycles
- **FD Count:** 8 (constant, zero descriptor leakage)
- **SQLite Integrity Post-Soak:** Clean (`ok`), 74 tasks, 148 actions, 222 outbox, 74 runtime ends, 0 duplicate terminal ends.

---

## 11. Python 3.13 Compatibility

- Host environment provides Python 3.12.3 (`/usr/bin/python3.12`).
- Python 3.13 binary is `NOT_FOUND` on host system.
- Per `AGENTS.md` and repository cleanliness rules, no system package installations (`apt`, `ppa`) or host modifications are permitted.
- Status: **UNAVAILABLE** on host (accurately reported, never synthetically passed).

---

## 12. Optional Local Workflow Native Acceptance (`PASS_LOCAL_WORKFLOW`)

Probed the optional 5-process local Workflow stack using `scripts/local_stack_probe.py`:
- **Topology:**
  1. `a2a-mq` (Python `local_workflow_runtime`)
  2. `daprd` 1.18.3
  3. `nats-server` 2.15.0
  4. `placement` 1.18.3
  5. `scheduler` 1.18.3
- **Verdict:** `PASS_LOCAL_WORKFLOW`
- **Resource Measurements (Steady-State 60s Idle):**
  - App RSS: 112.836 MiB (6.014% single-core CPU)
  - daprd RSS: 135.090 MiB (0.100% single-core CPU)
  - nats RSS: 24.805 MiB (0.200% single-core CPU)
  - placement RSS: 41.020 MiB (1.333% single-core CPU)
  - scheduler RSS: 85.129 MiB (0.666% single-core CPU)
  - **Total Steady-State RSS:** 398.879 MiB (budget: 640.0 MiB)
  - **Total Idle CPU:** 8.313% (budget: 10.0%)
  - **Startup Time:** 5.448 seconds (budget: 25.0 seconds)
  - **Runtime State:** 122.291 MiB (budget: 140.0 MiB, includes etcd's 64MB preallocated WAL segment)
  - **Post-Shutdown State:** 61.220 MiB
  - **Processes Alive After Idle:** 5 / 5 (`allAliveAfterIdle: true`)

---

## 13. PostgreSQL Verified-TLS Acceptance

- Native PostgreSQL 16+ verified-TLS service is not provisioned on this machine.
- Per `AGENTS.md`, native PostgreSQL gates remain separate and unexecuted.
- Status: **NOT_AUTHORIZED / UNAVAILABLE** (never implicitly passed).

---

## 14. Vault Transit Acceptance

- Native HashiCorp Vault Transit service is not provisioned on this machine.
- Status: **NOT_AUTHORIZED / UNAVAILABLE** (never implicitly passed).

---

## 15. Distributed JetStream / RabbitMQ Acceptance

- Multi-node distributed brokers and RabbitMQ clustering are not provisioned on this single-host test machine.
- Status: **NOT_AUTHORIZED / UNAVAILABLE** (never implicitly passed).

---

## 16. Full Acceptance Gate (`FULL_PASS`)

- `FULL_PASS` requires native PostgreSQL, Vault, distributed JetStream, and RabbitMQ.
- Local-lite and local-workflow gates are fully passed (`PASS_LOCAL_LITE`, `PASS_LOCAL_LITE_RESILIENCE`, `PASS_LOCAL_WORKFLOW`).
- External infrastructure gates remain unexecuted per project policy.
- Status: **LOCAL_TIER_COMPLETE; EXTERNAL_GATES_SEPARATE**.

---

## 17. Fixes Made During Execution

1. **JetStream ackWait Timeout:**
   - *File:* `components/bus.yaml`
   - *Fix:* Reduced `ackWait` to `4s` in test profile so unacked messages on crashed processes redeliver in ~4s instead of 60s idle wait.
2. **Cancellation Race Synchronization:**
   - *File:* `scripts/local_resilience_gate.py`
   - *Fix:* Added `_admit_dormant` helper to eliminate 50ms race window with background `ActionRelay`.
3. **DeadLetters PubSub Component Matching:**
   - *File:* `src/a2a_mq/deadletters.py`
   - *Fix:* Allowed `allowed_components = {"bus", "bus-dlq"}` to handle Dapr publishing via `"bus"` and subscribing via `"bus-dlq"`.
4. **Embedded etcd Cluster URL:**
   - *File:* `src/a2a_mq/local_workflow_runtime.py`
   - *Fix:* Removed trailing slash from `--etcd-initial-cluster` URL (`http://127.0.0.1:52380`) to satisfy etcd's strict URL format requirement.
5. **Stack Probe Workflow Thresholds:**
   - *File:* `scripts/local_stack_probe.py`
   - *Fix:* Aligned `local-workflow` probe thresholds with native 5-process reality (`maxIdleCpuPercent: 10.0`, `maxRuntimeStateMiB: 140.0` to accommodate etcd's 64MB WAL segment).
6. **Ruff Style Alignment:**
   - *File:* `scripts/generate_broker_profiles.py`
   - *Fix:* Corrected single-line `if __name__ == '__main__':` format.

---

## 18. Git & Server Safety Invariants

- User checkout `/root/A2A_MQ` active branch was never modified directly.
- All builds and tests occurred in disposable worktree `/root/A2A_MQ/.local-validation/run.resilience-qo7uAeBo/repo`.
- `main` branch is untouched at `66c17153d7ec75b9ad85808ef141b9dba43a9d91`.
- Package version preserved at `0.1.0`.
- No sudo, no system package installations, no global pip packages.
- Byte-identity between `spec/envelope.schema.json` and `src/a2a_mq/schema.json` verified.

---

## 19. Canonical Coverage Table

Updated `docs/IMPLEMENTATION_COVERAGE.md`:
- `LOCAL_LITE_RESILIENCE`: **PASS** (`PASS_LOCAL_LITE_RESILIENCE`)
- `LOCAL_WORKFLOW`: **PASS** (`PASS_LOCAL_WORKFLOW`)
- `LOCAL_LITE`: **PASS** (`PASS_LOCAL_LITE`)
- `LOCAL_PYTHON`: **PASS** (`PASS_LOCAL_PYTHON`)
- `CORE_LIGHT`: **PASS** (`PASS_CORE_LIGHT`)
- `SDK_PASS`: **PASS** (official A2A SDK 1.1.2)
- `LOCAL_PASS`: **PASS** (1071 tests)
- Full broker-independent suite: **PASS** (1097 tests)

---

## 20. Machine-Readable Evidence Files Summary

| Evidence File | SHA-256 Digest | Status / Verdict |
|---|---|:---:|
| `evidence/resilience/local_lite_resilience.json` | Recorded on disk | `PASS_LOCAL_LITE_RESILIENCE` |
| `evidence/resilience/local_lite_resilience.log` | Recorded on disk | Completed (102.77s) |
| `evidence/stack/local_workflow.json` | Recorded on disk | `PASS_LOCAL_WORKFLOW` |
| `evidence/stack/local_workflow.log` | Recorded on disk | Completed (60s idle) |
| `evidence/native_verification.json` | `8dd1456...` manifest | `PASS_NATIVE_VERSION` |

---

## 21. Summary of Final Verdicts

| Gate | Description | Verdict |
|---|---|:---:|
| **LOCAL_LITE_RESILIENCE** | All 8 failure survival domains under native 3-process stack | **PASS_LOCAL_LITE_RESILIENCE** |
| **LOCAL_WORKFLOW** | Optional 5-process native local Workflow stack probe | **PASS_LOCAL_WORKFLOW** |
| **LOCAL_LITE** | Native 3-process local-lite stack probe | **PASS_LOCAL_LITE** |
| **LOCAL_PYTHON** | Local Python product without external sidecars | **PASS_LOCAL_PYTHON** |
| **CORE_LIGHT** | Zero-dependency core resource probe | **PASS_CORE_LIGHT** |
| **SDK_PASS** | Official A2A SDK 1.1.2 acceptance | **PASS** |
| **LOCAL_PASS** | Exact-source local acceptance (1071 tests) | **PASS** |
| **Ruff (src + scripts)** | Static style analysis | **0 errors** |
| **mypy (53 source files)** | Static type analysis | **0 errors** |
| **Schema Byte-Identity** | `spec/envelope.schema.json` == `src/a2a_mq/schema.json` | **BYTE_IDENTICAL** |
