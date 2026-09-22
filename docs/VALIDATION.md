# Validation record

## Current scope

## Current source status

The branch contains both local tiers described in ADR 0011 plus the distributed
production-like implementation.

The latest exact-SHA local execution evidence is
[local-run-20260922.md](validation/local-run-20260922.md) at
`3415b10ddcfc5c721f9872c926fead5e11897543`. On that SHA:

- core economics passed;
- the warmed Python local product passed;
- 118 selected local regressions passed;
- repository-pinned Dapr 1.18.3 all-components and NATS Server 2.15.0 were
  checksum/version verified;
- the real native three-process local-lite stack passed its resource budget and
  functional roundtrip;
- repository contracts and isolated wheel packaging smoke passed;
- formal local acceptance produced `LOCAL_PASS` with 929 tests and clean exact
  Git provenance.

That evidence remains attached to `3415b10`. Later source/harness hardening on
this branch does not inherit the verdict. Read [current status](STATUS.md) for the
current source SHA and the exact rerun boundary.

The optional five-process local Workflow tier still has no equivalent native
resource/restart acceptance evidence. Distributed PostgreSQL verified-TLS, Vault,
official SDK and dual-broker/full-stack gates also remain separate and incomplete.

The next machine run should first verify the post-`3415b10` commits with focused
regressions, then rerun exact-SHA local acceptance. Repeating the expensive resource
probes is only required when their relevant runtime/measurement bytes changed or
when a new target-machine measurement is explicitly needed.

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
