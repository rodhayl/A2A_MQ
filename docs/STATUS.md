# Source and acceptance status - 2026-09-17

## Present source

Branch: `implementation/production-hardening-20260916`.
Base: `66c17153d7ec75b9ad85808ef141b9dba43a9d91` (phase 2).
Version: **0.1.0**. No main merge, force update, release or certification is issued.

The source includes envelope validation, task-state helpers, Dapr HTTP publishing
and callbacks, strict JUnit evidence, JSON/transport hardening, stable echo event
bytes, and frozen-SHA review tooling. It now also includes consumer-scoped SQLite
persistence, verified replay evidence, a bounded asynchronous database lane,
durable echo workers, independent snapshot observers, an outbox relay and an
authenticated internal query service. These are incremental implementations,
not a checksum-verified recovery of the original later snapshot.

Phase 3a is commit `a2ffbf787150563652085de934a2d67b7ae13b28`; recovery checks are
`9dcd38e0b0aee8936d9089490c2d51e4a18a1479`; runtime integration is
`4e68deeb9d0c2c06722cd070dbd07a736533386a`; the service is
`64694be6f69f0033cb20cd3a32d762d37b901c24`.

[Durable service usage](DURABLE_SERVICE.md) documents the explicit new entry point.
The old `demo:create_app` and `make up` fixture remain volatile and disposable.
[New validation](validation/durable-service-20260917.md) reports **184 selected
combined tests passed**, real SQLite/process/loopback HTTP scopes and an offline
installed-wheel smoke. It is not the whole repository suite, hosted CI or a live
Dapr/broker gate. [Independent review](../TEST_MVP.md) retains a distinct full-MVP
`INCOMPLETE` verdict. Architecture/requirements and old phase records are design
or historical material, not evidence that every target capability exists.

## Original snapshot recovery

The staged handoff at `e80096da84190a8af9aa044a4bd97b0123fca4ad` expected a bundle with
SHA-256 `47a2c5418ec35d0d1c5fbcbb0fa4cd502b8479760b935a3b94ed01efe93965e9`,
10 checkpoints and final tree `79521dec0000e246baaab5bf4129841aa4799914`.
No verified reconstruction of that tree was obtained.

The committed `.bundle/part-01` blob `f347f90d3b4db2c80ae61d718b69c41af3dc9be8`
has a literal `...` suffix. The alternate `.sync/base-719f.b64` blob
`d22c45df80281ce9fc8fe6e29bfc16bedbba18e7` also has a literal ellipsis suffix.
Encoded-tail inspection distinguished stored bytes from tool display truncation.
The other legacy fragments have not established complete checksum-verified recovery.

Continue implementation against the accessible architecture and tested baseline.
Any subsequently recovered snapshot must pass its checksum and `git bundle verify`,
then be compared with current changes before integration. Keep recovery payloads
outside the product branch and preserve the newer tested work.

## Staged patch disposition

The following records the original September 16 replay assessment; the new local
phase-3 implementation is narrower than those original full-MVP patches.

| Block | Disposition |
| --- | --- |
| 02 | Standalone JUnit gate recovered and strengthened; CI adapted to the available source. Full-stack gates remain separate. |
| 03-04 | Original canonical agent E2E/flush patch not replayed. New SQLite worker/relay/HTTP tests cover a narrower reference path; bridge and Workflow are still absent. |
| 05 | Full chaos matrix not replayed. New process-crash and cancellation checks are not full-stack chaos. |
| 06-07 | Full security/config/coverage integration not replayed. New mandatory app-token checks and fixed-scope queries are not production identity or tenant authorization. |
| 08 | Original full-MVP wheel/migration smoke not replayed. A separate offline installation smoke now verifies this current reference package. |
| 09 | Intended 0.3.1 completion claims remain unapplied; the package is experimental 0.1.0. |
| 10 | Limited frozen-SHA review tooling exists. Full-MVP acceptance remains explicitly incomplete. |

## Remaining implementation and acceptance

Phase 3 still needs a PostgreSQL adapter with real transactional parity tests and
an explicit durable-service Dapr/broker deployment. The new service must pass live
retention, competition, fan-out, retry/DLQ and sidecar-token tests. SQLite is local
single-host storage; the reference SQL effect does not establish exactly-once
external tool execution. Published outbox retention is coupled to replay evidence.

Phases 4-10 retain Dapr Workflow/Agents, standard A2A bridge interoperability,
production identity/TLS, optional encrypted envelopes/KMS, retention/quotas/HA,
RabbitMQ parity and the formal binding decision. No PostgreSQL, actual NATS or
RabbitMQ broker, Dapr Workflow, MCP or Vault/KMS service was exercised here.

Hosted run `35194462365` at `64694be6...` failed without step summaries for either
Python job; the underlying cause is unestablished. This is not a successful hosted
test gate. Subsequent commits require their own successful hosted execution.
Python 3.12, Ruff/mypy, the full existing regression suite, fresh locked dependency
resolution, full-MVP packaging/migrations, supply-chain and production acceptance
remain unverified. The offline wheel experiment used preinstalled dependencies.

Local test success, hosted success, full-MVP acceptance and production readiness
are separate statements; none implies the next one.
