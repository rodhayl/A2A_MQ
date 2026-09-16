# Validation record

## Current hardening scope

The following phase results are historical. They are not acceptance evidence for
the final hardening SHA and are not a certificate for the unrecovered full MVP.
Read [current status](STATUS.md), the [hardening execution record](validation/hardening-20260916.md)
and [independent review procedure](../TEST_MVP.md) for exact scope and blockers.

## Historical environment

Environment observed: Python 3.13.5; local SQLite; no Docker, Dapr runtime,
NATS server or PostgreSQL server installed. Direct GitHub clone and package
metadata download attempts failed in the execution container. The GitHub connector
works and is used for repository writes.

Phase results are appended after execution. Absence of a runtime is not a passing
integration gate. HTTP mocks, real SQLite transactions and schema tests must be
reported under their own names. Full clean dependency resolution, lint/type tools,
upstream SDK, Dapr/JetStream and PostgreSQL require separate evidence when available.

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
