# A2A-MQ Local Recertification & Production Gate Report — 2026-09-24

## 0. Executive Summary & Verdict

This report certifies the complete local tier of A2A-MQ on branch `implementation/production-hardening-20260916` following the Workflow resource policy correction commit `dbc2c78ac1d2ffac3dbf4157269f206c7bb6b707` and subsequent defect fix commit `d72112641ee4a43a1a668426402c01e35f316b54`.

All requirements specified for local certification and ADR 0011 contract have been verified:
1. **Workflow Resource Policy & Defect Fix:**
   - ADR 0011 budget: 5 steady-state processes, RSS <= 640 MiB, idle CPU <= 8%, state <= 64 MiB, readiness <= 25s.
   - Identified root cause in `0.tmp` WAL preallocation inflating durable state, duplicate `ActionRelay` tasks and tight 100ms idle polling inflating CPU, and drain polling race before SQLite integrity assertion. Fixed in `d721126` with deterministic unit regression in `tests/test_local_stack_probe.py`.
   - Verified figures: 5 processes, Peak RSS 370.69 MiB (budget 640 MiB), Idle CPU 4.23% (budget 8.0%), State 61.26 MiB (budget 64 MiB), Startup 6.18s (budget 25s).
2. **Local-Lite Resource Probe:** 3 native processes (`python`, `daprd`, `nats-server`), Peak RSS 249.88 MiB (budget 384 MiB), Idle CPU 3.07% (budget 5.0%), State 0.13 MiB (budget 32 MiB), Startup 4.79s (budget 15s).
3. **Core & Python Product Probes:**
   - Core: Peak RSS 44.07 MiB, Idle CPU 0.005%, State 0.35 MiB, Startup 0.12s (`PASS_CORE_LIGHT`).
   - Python product: Build 2.19s, Peak RSS 96.67 MiB, Idle CPU 2.73%, State 0.13 MiB, Startup 1.15s (`PASS_LOCAL_PYTHON`).
4. **Local-Lite Resilience Gate (`PASS_LOCAL_LITE_RESILIENCE`):**
   - 113.45s elapsed across all scenarios: Scenario A (unacked broker delivery + SIGKILL + JetStream redelivery, deliveryCount=2, logicalEffect=1, terminalResult=1), Scenario B (publish before bookkeeping crash, publishAttempts=2), Scenario C (crash during execution in WORKING state), Scenario E (commit boundary races), 9 cancellation races, live DLQ quarantine & replay over JetStream, 10 MCP-over-HTTP lifecycle/security scenarios, SQLite crash consistency, backup/restore, capacity/backpressure.
5. **Workflow Tier Separated Gate (`PASS_LOCAL_WORKFLOW`):**
   - `PASS_LOCAL_WORKFLOW_RESOURCE`: 5 processes within 640 MiB budget (370.69 MiB observed), idle CPU 4.23%, state 61.26 MiB.
   - `PASS_LOCAL_WORKFLOW_FUNCTIONAL`: 6 scenarios (basic workflow, durable timer delay, input required + continuation, auth required + approve, auth required + deny, native fan-out/fan-in).
   - `PASS_LOCAL_WORKFLOW_RESILIENCE`: 5 restart scenarios (app restart during active workflow, daprd restart, placement restart, scheduler restart with clean leadership re-election, sequential app -> daprd -> placement -> scheduler restart).
6. **Continuous Soak & Leak Gate (`PASS_LOCAL_SOAK`):**
   - Duration: 23.03 minutes (1381.7s total: 1200s active mixed workload + 180s drain).
   - Mixed workload: 2688 tasks processed (2048 submits, 1172 completes, 308 cancellations, 1334 MCP calls, 332 reconnects, 196 injected faults).
   - RSS: Warmup 289.93 MiB -> Drain 290.03 MiB (Delta: +0.10 MiB).
   - File Descriptors: Warmup 38 -> Drain 28 (Delta: -10; returned to baseline).
   - Threads: Warmup 25 -> Drain 25 (Delta: 0; strictly bounded).
   - SQLite Integrity: `ok`, 0 foreign key violations, 2688 tasks, 2688 runtime ends, 0 duplicate terminal ends.
7. **Static Checks & Quality Gate:**
   - `tools/check.py`: Passed (syntax, JSON, schemas byte-identical, local links).
   - `ruff check .`: Passed (0 errors).
   - `mypy src/a2a_mq`: Passed (54 source files clean).
   - `python -m pip check`: Passed (no broken requirements).
8. **Current-SHA Formal Acceptance Gate:**
   - Exact git tree snapshot verified clean before and after execution.
   - `a2a_mq.acceptance --mode local`: `LOCAL_PASS` (1100 tests, 0 failures, 0 errors, 0 skipped).
   - Strict broker-independent test suite: 1126 tests passed, 0 failures, 0 errors, 0 skipped.
   - Official SDK gate: `SDK_PASS` (26 tests passed).
   - Package/supply-chain gate: `PASSED` (isolated wheel build and clean target import smoke).
   - Independent MVP reviewer: `LOCAL_PASS` (1112 tests passed, 0 failures, 0 errors, 0 skipped).
9. **Production-Deployment Gate:**
   - Implemented `scripts/production_deployment_gate.py` with manifest synthesis and Kubernetes rollout/security verification.
   - Correctly reports `INCOMPLETE_PREREQUISITE` (`missingPrerequisites: ["kubectl"]`) with exit code 2 when cluster access is absent.
   - `productionCertified: false` and `upstreamBindingCertified: false` remain explicitly false.

**Local Certification Verdict:**
```
PASS_LOCAL_TIER_COMPLETE
```

---

## 1. Provenance & Execution Environment

- **Repository Root:** `/root/A2A_MQ`
- **Branch:** `implementation/production-hardening-20260916`
- **Initial Remote HEAD:** `dbc2c78ac1d2ffac3dbf4157269f206c7bb6b707`
- **Validated Source SHA:** `d72112641ee4a43a1a668426402c01e35f316b54`
- **Git Tree:** `b529a7834bf80640bad469a741df6bdf3b80113e`
- **Main SHA:** `66c17153d7ec75b9ad85808ef141b9dba43a9d91` (untouched)
- **Package Version:** `0.1.0` (untouched)
- **Isolated Venv:** `.local-validation/run.resilience-qo7uAeBo/venv`
- **Native Tools Directory:** `.local-validation/run.resilience-qo7uAeBo/tools/native/bin`
  - `daprd`: 1.18.3 (all-components)
  - `nats-server`: 2.15.0
  - `placement`: 1.18.3
  - `scheduler`: 1.18.3
- **Platform:** Linux 6.6.137+ x86_64, Python 3.12.3

---

## 2. Resource Gate Measurements

| Gate | Steady Roles | Peak RSS (MiB) | Budget RSS | Idle CPU (%) | Budget CPU | State (MiB) | Budget State | Readiness (s) | Budget Readiness | Verdict |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| Core Probe | 1 | 44.07 | <= 192 | 0.005% | <= 3.0% | 0.35 | <= 16 | 0.12 | <= 5.0 | `PASS_CORE_LIGHT` |
| Python Product | 1 | 96.67 | <= 192 | 2.73% | <= 3.0% | 0.13 | <= 4 | 1.15 | <= 5.0 | `PASS_LOCAL_PYTHON` |
| Local-Lite Stack | 3 | 249.88 | <= 384 | 3.07% | <= 5.0% | 0.13 | <= 32 | 4.79 | <= 15.0 | `PASS_LOCAL_LITE` |
| Local Workflow | 5 | 370.69 | <= 640 | 4.23% | <= 8.0% | 61.26 | <= 64 | 6.18 | <= 25.0 | `PASS_LOCAL_WORKFLOW` |

### Per-Role RSS Breakdown (Local Workflow)
- `app` (Python ASGI): 129.56 MiB
- `daprd` (Dapr sidecar): 121.22 MiB
- `nats` (NATS Server): 19.34 MiB
- `placement` (Dapr Placement): 36.57 MiB
- `scheduler` (Dapr Scheduler): 64.00 MiB
- **Aggregate Peak RSS:** 370.69 MiB (Budget <= 640 MiB)

---

## 3. External Infrastructure & Production Status

- **PostgreSQL verified-TLS:** UNAVAILABLE (Docker/Compose not installed on host).
- **Vault Transit:** UNAVAILABLE (Docker/Compose not installed on host).
- **JetStream / RabbitMQ Distributed Live:** UNAVAILABLE (Docker/Compose not installed on host).
- **Kubernetes Production Gate:** INCOMPLETE_PREREQUISITE (`kubectl` CLI and cluster connectivity unavailable on host).
- `productionCertified`: **false** (strictly preserved).
- `upstreamBindingCertified`: **false** (strictly preserved).
