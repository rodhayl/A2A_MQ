# ADR 0003 - Full snapshots and direct transactional storage

Status: accepted 2026-09-15.

Use complete A2A Task snapshots for the initial result stream. Strict adjacent
transition validation belongs at the executor; a projection sees missing/reordered
versions and instead checks reachability plus monotonic version/owner/context.
Reject terminal mutation and same-version conflicts. Delta events are a later
assembly contract, not incomplete snapshots.

Use SQLite locally and PostgreSQL for the shared SQL transaction boundary. Keep
Dapr Workflow state separate. Implementing inbox/effect/outbox atomicity through
separate generic state and publish calls would introduce a dual-write gap. Direct
SQL is deliberately a small storage-specific boundary, not a new workflow runtime.

Costs: duplicated snapshot bytes, database-specific transaction tests and explicit
migration/cleanup work before production. Benefits: simpler recoverable projection
and a testable atomic effect fixture without a bespoke event-sourcing subsystem.
