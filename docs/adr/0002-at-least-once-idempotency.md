# ADR 0002 - At-least-once delivery, explicit effect boundary

Status: accepted, corrected 2026-09-15.

Do not claim end-to-end exactly-once side effects. Deduplicate by tenant, logical
consumer and immutable message ID; reject content reuse conflicts. A durable claim
is a lease, not completion. Reclaim abandoned leases with fencing tokens.

Commit the local reference SQL effect, Task, completed inbox and outbox atomically.
ACK after that transaction. Publish stored events with their original IDs and
mark sent only after success. A publish/mark crash may duplicate an event.

This protects the transactional reference fixture. External tools require a stable
destination idempotency key or reconciliation. A local effect marker cannot prove
that an external action happened exactly once. Tests must expose this distinction.
