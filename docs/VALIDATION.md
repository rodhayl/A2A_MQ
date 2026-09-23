# Validation record

## Current scope

## Current source status

The branch contains both local tiers described in ADR 0011 plus the distributed
production-like implementation.

The latest exact-SHA local execution evidence is
[local-certification-20260923.md](validation/local-certification-20260923.md) at
`1dd1938dff5cbf58a803ec23617143b5d998465e`. On that SHA:

- core economics (`PASS_CORE_LIGHT`) and warmed Python product (`PASS_LOCAL_PYTHON`) passed;
- repository-pinned Dapr 1.18.3 all-components and NATS Server 2.15.0 were
  checksum and version verified (`PASS_NATIVE_VERSION`);
- the real native three-process local-lite stack passed its resource budget and
  functional roundtrip (`PASS_LOCAL_LITE`);
- official A2A SDK gate passed (`SDK_PASS`);
- formal local resilience gate passed with real broker/runtime fault injection
  (`PASS_LOCAL_LITE_RESILIENCE`);
- five-process native local Workflow tier passed separated resource, functional,
  and resilience gates (`PASS_LOCAL_WORKFLOW`);
- 23.02-minute continuous soak test verified stable post-warmup RSS, zero FD/thread
  leak, and clean post-drain state (`PASS_LOCAL_SOAK`);
- formal local acceptance produced `LOCAL_PASS` with 1077 tests (0 failures, 0 errors,
  0 skipped); full broker-independent suite passed with 1103 tests; mypy passed
  with 0 errors across 54 source files;
- overall local tier certification achieved: `PASS_LOCAL_TIER_COMPLETE`.


Subsequent source/test/harness hardening through `75f886b1` removed full-lint
blockers and tightened validation-only fault injection. Those commits are not part
of the executed evidence above. Until they are rerun, the current branch HEAD is
**not** an exact-SHA `PASS_LOCAL_TIER_COMPLETE` even though the prior certification
remains valid historical evidence for its recorded SHA.

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
