# ADR 0005 - Consumer-scoped transactional SQLite reference

Status: accepted 2026-09-17 for the single-host reference backend.

## Decision

Implement a file-backed SQLiteStore bound at construction to an immutable
StoreScope(tenant, consumer). Every inbox, effect, task and outbox primary key
includes both fields. Replicas use the same logical consumer; independent
observers use different consumers even when sharing one database file.

Claims retain the complete canonical command wire and digest. A live claim is
busy, never completed. Expired leases can be reclaimed with an incremented fence.
Committing checks scope, owner, digest, status, current fence and unexpired lease.
STALE_FENCE and LEASE_EXPIRED are retryable: they must not quarantine work another
replica can still finish. Renewal cannot shorten an existing lease.

One BEGIN IMMEDIATE transaction commits the SQL reference effect, full Task
snapshot, immutable outbox event and completed inbox outcome. Exceptions including
BaseException roll back the entire transaction. This is not an exactly-once
contract for arbitrary external effects or long-running workflows.

Snapshot projection atomically records its event inbox and updates the named
projection. It rejects tenant, owner and context conflicts, current-version
conflicting payloads, terminal rewrites and unreachable higher-version states.
Older snapshots are recorded as consumed without replacing newer state. Separate
observers independently consume the same event IDs.

Outbox attempts and acknowledgments are scoped and verify immutable wire/digest.
Publication remains at-least-once; crashes after publication and before marking
allow identical bytes to be republished. Published timestamps are idempotent.
Internal get/list calls use bound scope, parameterized queries and keyset pages
of at most 200 items. This is not a public A2A API or authorization service.

## Storage lifecycle

Use WAL and synchronous=FULL with short-lived connections. Busy errors are
retryable. Reject in-memory databases, unsupported schema versions, incomplete
versioned schemas and the earlier unscoped draft tables. Assigning legacy records
to an inferred consumer would silently corrupt ownership, so there is no automatic
legacy migration. Keep the database on a local filesystem, not NFS.

## Consequences and gates

This implements the SQLite part of phase 3, not PostgreSQL or the complete MVP.
The SQL effect fixture is deliberately narrow. Retention, quotas, backup/restore,
shared PostgreSQL transactions and live broker behavior remain separate gates.
See ../validation/storage-20260917.md for executed evidence.
