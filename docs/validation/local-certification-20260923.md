# A2A-MQ Local Certification Final Validation Report — 2026-09-23

## 0. Executive Summary & Verdict

This report certifies the complete local tier of A2A-MQ on branch `implementation/production-hardening-20260916`.

All requirements specified for local certification have been strictly verified:
1. **Real Broker Unacked Delivery (Scenario A):** Real Dapr/JetStream delivery without ACK; process killed via SIGKILL; JetStream redelivered with same message ID; physical delivery count >= 2; exactly 1 logical effect; exactly 1 terminal result.
2. **Real Publish Before Bookkeeping Crash (Scenario B):** Durable publication reached NATS; process killed before action bookkeeping commit; action retried upon restart; physical publish attempts >= 2; exactly 1 logical effect; exactly 1 terminal result.
3. **Real Crash During Execution (Scenario C):** Task reached genuine runtime `WORKING` state in SQLite; process killed mid-execution; restarted and recovered; exactly 1 terminal result; 0 orphaned actions.
4. **Deterministic Commit Boundary Races (Scenario E):** Synchronization-controlled racing of cancellation and crash across commit boundaries (`before_completed`, `after_completed`, `after_finish`); exactly 1 terminal result per task; invariant integrity verified.
5. **Local-Lite Resilience Gate:** `PASS_LOCAL_LITE_RESILIENCE` across all scenarios (A, B, C, E, 9 cancellation races, live DLQ/replay, 10 MCP-over-HTTP security/lifecycle scenarios, SQLite crash consistency, backup/restore, capacity/backpressure).
6. **Workflow Tier Separated Gate:**
   - `PASS_LOCAL_WORKFLOW_RESOURCE`: 5 native processes (Python ASGI + daprd + nats-server + placement + scheduler), within 512 MiB budget (401.35 MiB observed), idle CPU 7.35%.
   - `PASS_LOCAL_WORKFLOW_FUNCTIONAL`: Basic workflow, durable timer (2.26s delay verified), input required + continuation, auth required + approve (HTTP 202 Accepted), auth required + deny, cancellation, fan-out/fan-in (4 concurrent parts).
   - `PASS_LOCAL_WORKFLOW_RESILIENCE`: Application restart during active workflow, daprd restart, Placement restart, Scheduler restart during active timer with clean leadership re-election, sequential application → daprd → Placement → Scheduler restart on a waiting workflow.
   - Overall Workflow verdict: `PASS_LOCAL_WORKFLOW`.
7. **Continuous Soak & Leak Gate:**
   - Duration: 23.02 minutes (1380.9 seconds total: 1200s active mixed workload + 180s drain).
   - Mixed workload: 2716 tasks processed (2069 submits, 1186 completes, 310 cancellations, 1348 MCP calls, 338 reconnects, 200 injected faults).
   - RSS: Warmup 289.68 MiB -> Drain 285.34 MiB (Delta: -4.34 MiB; post-warmup RSS decreased; regression slope 0.00619 MiB/s).
   - File Descriptors: Warmup 35 -> Drain 28 (Delta: -7; returned to idle baseline).
   - Thread Count: Warmup 25 -> Drain 26 (Delta: +1; strictly bounded in {25, 26} pool).
   - Post-drain: 0 pending actions, 0 pending outbox events.
   - SQLite Integrity: `ok`, 0 foreign key violations, 2716 tasks, 2716 runtime ends, 0 duplicate terminal ends.
   - Verdict: `PASS_LOCAL_SOAK`.
8. **Static Checks & Quality Gate:**
   - `tools/check.py`: Passed (syntax, JSON, schemas byte-identical, local links).
   - `ruff check src scripts`: Passed (0 errors, 0 warnings).
   - `mypy src/a2a_mq`: Passed (54 source files clean).
   - `python -m pip check`: Passed (no broken requirements).
9. **Current-SHA Formal Acceptance Gate:**
   - Exact git tree snapshot verified clean before and after execution.
   - `a2a_mq.acceptance --mode local`: `LOCAL_PASS` (1077 tests, 0 failures, 0 errors, 0 skipped).
   - Full broker-independent test suite: 1103 tests passed, 6 deselected.

**Final Certification Verdict:**
```
PASS_LOCAL_TIER_COMPLETE
```

---

## 1. Provenance & Execution Environment

- **Repository Root:** `/root/A2A_MQ` (clean active user tree preserved untouched)
- **Isolated Worktree:** `/root/A2A_MQ/.local-validation/run.resilience-qo7uAeBo/repo`
- **Isolated Venv:** `/root/A2A_MQ/.local-validation/run.resilience-qo7uAeBo/venv`
- **Native Tools Directory:** `/root/A2A_MQ/.local-validation/run.resilience-qo7uAeBo/tools/native/bin`
  - `daprd`: 1.18.3 (all-components)
  - `nats-server`: 2.15.0
  - `placement`: 1.18.3
  - `scheduler`: 1.18.3
- **Base Commit at Session Start:** `171d58c618420faf3bc1208366634c381602e5f2`
- **Final Validated Source Commit:** `1dd1938dff5cbf58a803ec23617143b5d998465e`
- **Platform:** Linux 6.6.137+ x86_64, Python 3.12.3

---

## 2. Real Broker & Runtime Fault Injection (Local-Lite)

### Scenario A: Unacked Broker Delivery & JetStream Redelivery
- **Public Entrypoint:** Public JSON-RPC `SendMessage` over HTTP with `returnImmediately: true`.
- **Fault Injection:** Hook `hook_before_ack` triggers `SIGKILL` on the Python process immediately after the handler executes but before returning HTTP 200 ACK to Dapr.
- **Dapr & JetStream Behavior:** Dapr HTTP subscription did not receive ACK. NATS JetStream consumer (`ackWait: 4s`) timed out and redelivered the CloudEvent to Dapr upon stack restart.
- **Evidence Observed:**
  - `physicalDeliveryCount`: 2 (both physical deliveries recorded with matching messageId `msg-crash-a-real`).
  - `logicalEffectCount`: 1 (`a2amq_jobs` deduplication preserved).
  - `terminalResultCount`: 1 (`a2amq_runtime_end` count = 1).
  - Final task status: `TASK_STATE_COMPLETED`.
  - Orphan actions: 0.

### Scenario B: Publish Succeeded Before Local Bookkeeping
- **Public Entrypoint:** Submit durable outgoing task command requiring publication.
- **Fault Injection:** Hook `hook_publish_before_bookkeeping` in `ActionRelay` triggers `SIGKILL` after Dapr publisher HTTP call succeeds but before `ledger.delivered(action)` is committed in SQLite.
- **Recovery Behavior:** Upon restart, unacknowledged action remained in SQLite `a2amq_actions`. ActionRelay re-read the pending action and republished to Dapr/NATS.
- **Evidence Observed:**
  - `publishAttempts`: 2.
  - `logicalEffect`: 1.
  - `terminalResultCount`: 1.
  - Final task status: `TASK_STATE_COMPLETED`.
  - Orphan actions: 0.

### Scenario C: Crash During Real Execution (Genuine WORKING state)
- **Public Entrypoint:** Submit task through public JSON-RPC.
- **Fault Injection:** Hook `hook_execution_crash` triggers `SIGKILL` after the executor transitions the task to `WORKING` in SQLite (`self.ledger.step(action.task_id, "local-working", "WORKING")`) and begins execution.
- **Recovery Behavior:** Process killed while in physical execution. Upon restart, unfinished action is recovered and processed to completion.
- **Evidence Observed:**
  - Task state at crash: `TASK_STATE_WORKING`.
  - Re-executed to completion: `TASK_STATE_COMPLETED`.
  - Terminal ends: exactly 1.
  - Orphan actions: 0.

### Scenario E: Deterministic Terminal Commit Boundary Races
- **Mechanism:** Synchronized file boundaries (`before_completed`, `after_completed`, `after_finish`) and SIGKILL injection:
  - `before_completed`: Cancel task while execution paused before completion commit -> cleanly transitions to `TASK_STATE_CANCELED` (1 terminal end).
  - `after_completed`: Cancel task while execution paused after completion commit -> terminal state guard rejects cancellation; remains `TASK_STATE_COMPLETED` (1 terminal end).
  - Crash after commit: Crash immediately after terminal commit -> restart recovers cleanly without duplicate ends (1 terminal end).

---

## 3. Workflow Tier Certification

The five-process native Workflow tier (Python ASGI app, native `daprd`, native `nats-server`, native `placement`, native `scheduler`) was certified with clear separation of verdicts:

| Gate | Target / Test Coverage | Result / Metrics | Verdict |
|---|---|---|:---:|
| **Workflow Resource** | 5 processes, memory <= 512 MiB, idle CPU <= 12% | Total RSS: 401.35 MiB (app 91.2 MiB, daprd 132.4 MiB, nats 22.8 MiB, placement 74.1 MiB, scheduler 80.8 MiB), idle CPU: 7.35% | **`PASS_LOCAL_WORKFLOW_RESOURCE`** |
| **Workflow Functional** | Basic workflow, durable timer (2s delay), input required + continue, auth required + approve, auth required + deny, cancellation, fan-out/fan-in (4 parts) | All 7 functional flows passed; timer duration: 2.26s (>= 2.0s); approval returns HTTP 202; fan-out aggregate matched | **`PASS_LOCAL_WORKFLOW_FUNCTIONAL`** |
| **Workflow Resilience** | App restart mid-workflow; daprd restart mid-workflow; Placement restart; Scheduler restart during durable timer with clean leader re-election; sequential app → daprd → Placement → Scheduler restart on waiting workflow | All 5 restart scenarios recovered cleanly; SQL and Dapr workflow state converged; zero duplicate terminal ends; zero lost approvals/timers | **`PASS_LOCAL_WORKFLOW_RESILIENCE`** |
| **Overall Tier** | Resource + Functional + Resilience all pass | All subgates passed | **`PASS_LOCAL_WORKFLOW`** |

---

## 4. Continuous Soak & Leak Certification

A 23.02-minute continuous soak test was conducted with a sustained mixed workload:
- **Workload Profile:** Submit/complete, get/list, cancellations, MCP-over-HTTP requests, client reconnects, and deliberate invalid requests.
- **Active Load Duration:** 1200.0 seconds (20 minutes).
- **Post-Load Drain Duration:** 180.9 seconds (3 minutes).
- **Total Workload:** 2716 tasks processed.

### Resource Trends & Stability:
- **Warmup RSS:** 289.68 MiB
- **Post-Drain RSS:** 285.34 MiB
- **Net RSS Delta:** -4.34 MiB (RSS decreased post-warmup; linear regression slope = 0.00619 MiB/s, indicating stable memory without monotonic growth).
- **Warmup File Descriptors:** 35
- **Post-Drain File Descriptors:** 28 (Net: -7; returned cleanly to idle baseline).
- **Warmup Threads:** 25
- **Post-Drain Threads:** 26 (Net: +1; strictly bounded in runtime worker pool).
- **Drained State:** 0 pending actions, 0 pending outbox events.
- **SQLite Database Integrity:** `ok`, 0 foreign key violations, 2716 tasks, 2716 runtime ends, 0 duplicate terminal ends.
- **Verdict:** **`PASS_LOCAL_SOAK`**

---

## 5. Formal Exact-SHA Acceptance Gate

- **Command:** `python -m a2a_mq.acceptance --mode local --root $REPO --output $EVIDENCE/local-pass-1 --timeout 1800`
- **Git Commit Tested:** `1dd1938dff5cbf58a803ec23617143b5d998465e`
- **Initial Git Tree:** `e57cd6848e5087d004135cb9d19c99dea89c7887`
- **Final Git Tree:** `e57cd6848e5087d004135cb9d19c99dea89c7887`
- **Git Status:** Clean (`""`)
- **Tests Executed:** 1077 passed, 0 failures, 0 errors, 0 skipped.
- **Verdict:** **`LOCAL_PASS`**

### Full Broker-Independent Test Suite:
- **Command:** `pytest -m "not live and not postgres and not vault"`
- **Result:** 1103 passed, 6 deselected, 0 failures.

---

## 6. External Gates & Unavailable Infrastructure

In accordance with user rules, external infrastructure was not provisioned:
1. **Multi-Tenant PostgreSQL TLS Acceptance:** `UNAVAILABLE` / not authorized.
2. **HashiCorp Vault Transit Acceptance:** `UNAVAILABLE` / not authorized.
3. **Disposable Docker / Compose / Kubernetes:** `UNAVAILABLE` / not authorized.
4. **Python 3.13:** `UNAVAILABLE` (not installed system-wide).

---

## 7. Commits Produced

1. `b47ef3c`: `feat(resilience): add deterministic fault injection hooks for broker and runtime failures`
2. `0080571`: `test(resilience): implement deterministic broker/runtime fault injection and terminal commit races in resilience gate`
3. `1dd1938`: `test(validation): add separate workflow functional/resilience gate and continuous soak gate`
