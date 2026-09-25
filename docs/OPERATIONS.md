# Operations and recovery

## Development versus production

`deploy/` is a loopback-only local fixture, not hardened deployment guidance. The
SQLite backend is for a single host. A live PostgreSQL test is required before a
shared multi-host deployment. No HA/RPO/RTO promise follows from a unit test.
Dapr Workflow state and the application SQL state need separate backup planning.

## Health and failure classification

Liveness answers whether the process runs. Readiness must check store access and
the Dapr outbound API, not merely return HTTP 200. Readiness does not measure broker
quorum health or backlog. Transient failures retry; permanent malformed commands
are handed to a confirmed DLQ before ACK. If the DLQ is unavailable, do not ACK.
Delivery exhaustion in JetStream is a stranded-message/alert condition unless a
verified dead-letter path handles it; do not assume `maxDeliver` performs a move.

## Required signals before production

Measure accepted publishes, callback attempts, duplicate/conflict counts, busy and
expired leases, old fencing tokens, pending/oldest outbox, outbox publish failures,
projection version/lag, DLQ count/age, dependency readiness and storage capacity.
Structured logs contain identifiers, route and reason codes, not prompts, tokens,
raw payloads, exception response bodies or ciphertext keys. Metrics/exported traces
are phase-8 work beyond initial trace-context propagation.

## Retention invariants

Retain dedup/effect identities at least through the maximum replay/redelivery window.
Retain decrypt capability through retained ciphertext lifetime when crypto is added.
Do not delete completed inbox rows simply because broker ACKs succeeded. Keep task
projections/backups beyond broker retention when queries require it. Initial SQL
records are retained without automatic cleanup; plan quotas/cleanup before production.
Snapshots are bounded but large history/artifacts must be externalized deliberately.

## Recovery procedure

Given a command ID: locate its tenant/consumer inbox entry; distinguish busy lease
from completed outcome; inspect pending outbox and projection version; then inspect
consumer backlog and DLQ metadata. Never mark a processing claim done manually to
clear a queue. A reclaimed lease must get a new token/fence. A stale worker fails
its commit check. A terminal task is not reopened by replay.

For outbox recovery, resend the stored envelope unchanged. A positive publish may
have occurred even after a timeout. Reusing its ID makes this ambiguity safe for
idempotent observers. Mark sent only after a confirmed response. Multiple relays
may send duplicates; no exactly-once publisher claim is made.

For DLQ replay distinguish same-command retry (same immutable ID/body) from new work
(new ID and explicit causation). Editing the body under the old ID is a conflict.
Replay authorization, inspection CLI and automated cleanup are later release gates.

## Deploy and drain

Create streams before producers, preserve role-specific durable/group identities,
and start observers independently from workers. Bound SQL busy timeout, callback
concurrency, Dapr request timeout, ack wait and payload size coherently. Retry and
lease timeouts must permit recovery before command retention expires.

Stop intake, let bounded in-flight transactions finish, stop the relay after its
current publish, then exit without blanket ACKs. Pending outbox rows survive a
restart. Rollback migrations are not implemented yet: initial schema creation is
idempotent, but schema upgrades require a future explicit migration mechanism.

Before production test broker loss, DB restart/restore, disk full, lease takeover,
Dapr consumer restarts, poisoned messages, unauthorized routes and backups. Record
versions and observed behavior; keep unexecuted tests visible in validation.
