# Validation record

## Current scope

The latest phase-3 continuation is recorded in
[durable-service validation](validation/durable-service-20260917.md), with source
commits, 184 selected tests, real HTTP/process scope and an offline wheel smoke.
Read [current status](STATUS.md) and [independent review](../TEST_MVP.md) before
interpreting a pass. The original full MVP remains unrecovered and unaccepted.

The following phase results and the
[earlier hardening execution](validation/hardening-20260916.md) are historical.
They are not acceptance evidence for the current source SHA or a certificate for
the full MVP. Absence of a runtime is not a passing integration gate.

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
