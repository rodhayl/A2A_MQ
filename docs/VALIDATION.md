# Validation record

## Current scope

## Current source status

The branch contains both local tiers described in ADR 0011 plus the distributed
production-like implementation.

The latest exact-SHA production certification and full acceptance evidence is
[production-certification-20260925.md](validation/production-certification-20260925.md) at
`6458195b171020ece1911e68bc6d14998d216164` (clean tree `ed9af9e6c04a3b6bba1f0880ca40d7f9a7f4e758`). On that SHA:

- **Formal Acceptance Gate (`a2a_mq.acceptance`) achieved `FULL_PASS`**:
  - `local`: 1149 tests passed (0 failures, 0 errors, 0 skipped);
  - `sdk`: 26 tests passed (official upstream SDK & canonical MCP contracts);
  - `postgres`: 4 tests passed using real verified-TLS PostgreSQL 16 (`sslmode=verify-full`);
  - `vault`: 1 test passed using real TLS HashiCorp Vault Transit;
  - `repository-check`, `dependency-check`, `lint`, `types`, `package`: all passed;
  - `jetstream`: 12 live probe checks passed on disposable NATS JetStream container stack;
  - `rabbitmq`: 12 live probe checks passed on disposable RabbitMQ container stack.
- **Production Deployment Gate (`scripts/production_deployment_gate.py`) achieved `PRODUCTION_DEPLOYMENT_PASS`**:
  - Live execution against 3-node Kubernetes v1.31.0 cluster with Dapr 1.18.3 and NATS StatefulSet;
  - Application image: `a2amq-prod:6458195-ca@sha256:a54c969183a0c314c77df93c7761db53de19dea8da9e9b20ddebccec425958af`;
  - All 19 required production checks passed (100% passing, 0 missing, 0 incomplete);
  - Verified image digest across all application pods, security context (non-root UID 10001, drop ALL, read-only rootfs), Dapr sidecar injection, PDB enforcement, PostgreSQL state store v2 attachment, authenticated E2E task lifecycle, owner/tenant 403 denial, authenticated MCP query (6 tools), workflow restart with UID change, DLQ routing and replay, rolling restart zero-loss, and 135s JetStream broker outage recovery;
  - Clean automated post-run resource deletion (0 remaining gate resources).

The preceding local-tier execution evidence is
[local-certification-20260924.md](validation/local-certification-20260924.md) at
`d72112641ee4a43a1a668426402c01e35f316b54`. On that SHA:

- core economics (`PASS_CORE_LIGHT`) and warmed Python product (`PASS_LOCAL_PYTHON`) passed;
- repository-pinned Dapr 1.18.3 all-components and NATS Server 2.15.0 were
  checksum and version verified (`PASS_NATIVE_VERSION`);
- the real native three-process local-lite stack passed its resource budget and
  functional roundtrip (`PASS_LOCAL_LITE`);
- official A2A SDK gate passed (`SDK_PASS`);
- formal local resilience gate passed with real broker/runtime fault injection
  (`PASS_LOCAL_LITE_RESILIENCE`, 113.45s);
- five-process native local Workflow tier passed separated resource, functional,
  and resilience gates (`PASS_LOCAL_WORKFLOW`) with peak RSS 370.69 MiB <= 640 MiB,
  idle CPU 4.23% <= 8%, runtime state 61.26 MiB <= 64 MiB;
- 23.03-minute continuous soak test verified stable post-warmup RSS (+0.10 MiB delta),
  zero FD/thread leak, and clean post-drain state (`PASS_LOCAL_SOAK`);
- formal local acceptance produced `LOCAL_PASS` with 1100 tests (0 failures, 0 errors,
  0 skipped); full broker-independent suite passed with 1126 tests; mypy passed
  with 0 errors across 54 source files;
- independent reviewer verified `LOCAL_PASS` with 1112 tests;
- isolated package wheel build and clean import smoke passed;
- overall local tier certification re-established: `PASS_LOCAL_TIER_COMPLETE`.

A subsequent audit and production gate completion is documented in
[production-gate-audit-20260924.md](validation/production-gate-audit-20260924.md):
- Empirical investigation into Dapr Scheduler `0.tmp` preallocation verified that it is 64 MB of unwritten all-zero buffer space created by embedded etcd, cleanly deleted on clean shutdown, and safe to delete without recovery loss.
- Replaced blanket `.tmp` exclusion with race-safe `O_NOFOLLOW | O_NONBLOCK`, bounded (<= 128 MiB), regular-file verified WAL scratch accounting; verified by 5-process Workflow resource gate (`PASS_LOCAL_WORKFLOW`: peak RSS 369.75 MiB, idle CPU 3.90%, runtime state 61.26 MiB running / 61.22 MiB post-shutdown).
- Implemented and hardened `scripts/production_deployment_gate.py` as a full live Kubernetes acceptance runner with 19 required checks: real observable E2E operational verification using official repository client SDKs (`a2a.client` and `mcp.client`), canonical access denial verification (rejecting 5xx/timeouts), foreign-owner 403, cross-tenant 403, authenticated MCP, worker restart with verified Ready state and pod UID replacement verification (`uid_before != uid_after`), DLQ routing and replay requiring run-owned quarantine record and verified downstream execution (`replayedAt`), rolling restart zero-loss with API pod Ready UID set replacement verification, managed-by broker outage recovery with explicit `a2amq.io/allow-production-gate-disruption=true` opt-in and mandatory 0 replicas observation and canonical hold duration (`broker_outage_hold_seconds`), PDB selector role match enforcement, strict Dapr PostgreSQL state store v2 contract validation, strict simulation isolation (`SIMULATED_VALIDATION_PASS`), running image digest validation, run-isolated cleanup (`components.dapr.io` and `a2amq.io/gate-run-id`) with 0-residual verification, and 46 automated unit/regression tests (100% passing); executed against the host returning clean `INCOMPLETE_PREREQUISITE` (exit 2) due to absent cluster infrastructure.
- Broker-independent test suite passed: 1160 tests; all 46 production gate tests 100% passing.

## Historical environment

Environment observed: Python 3.13.5; local SQLite; no Docker, Dapr runtime,
NATS server or PostgreSQL server installed. Direct GitHub clone and package
metadata download attempts failed in the execution container. The GitHub connector
works and is used for repository writes.

Phase results are appended after execution. HTTP mocks, real SQLite transactions
and schema tests must be reported under their own names. Clean dependency
resolution, lint/type tools, upstream SDK, Dapr/JetStream and PostgreSQL require
separate evidence when available.

## Phase 0

15 baseline/schema tests passed. Offline editable install and repository checks
passed. See [phase 0 evidence](validation/phase0.md). No live infrastructure pass
is claimed.

## Phase 1

121 tests passed; 2 upstream SDK tests skipped because the optional SDK is absent.
Repository checks passed. See [phase 1 evidence](validation/phase1.md).

## Phase 2

156 tests passed; 3 skipped (2 upstream SDK, 1 real Docker broker fixture).
Generation/repository checks and real loopback HTTP/CLI tests passed. Broker
integration is not claimed. See [phase 2 evidence](validation/phase2.md).

## Phase 3a

The consumer-scoped SQLite implementation and its 103-test record are described in
[storage validation](validation/storage-20260917.md) and
[ADR 0005](adr/0005-consumer-scoped-sqlite.md).

## Phase 3b/3c

[ADR 0006](adr/0006-durable-echo-runtime.md) defines worker/relay/cancellation
semantics. [The service guide](DURABLE_SERVICE.md) covers authenticated local use.
The [latest evidence](validation/durable-service-20260917.md) distinguishes the
184-test selected-suite pass, offline packaging and the failed/unverified hosted
gate. Neither mock publication nor a real HTTP sidecar fixture proves broker parity.
