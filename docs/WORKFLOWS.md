# Workflows and failure boundaries

## Durable task path

A command is durably admitted, consumed at least once, fenced by tenant/consumer
state, and completed with transactional task/outbox updates. Publishing an event and
locally recording its publish marker are separate steps; duplicate delivery remains
possible and is handled by stable event identity.

## Crash matrix

| Crash point | Durable state | Recovery |
| --- | --- | --- |
| before claim commit | no reservation | redelivery may claim |
| after claim commit | lease/fence only | expiry + fenced takeover |
| during SQL effect transaction | rolled back | reclaim and retry |
| after outcome commit, before broker ACK | terminal inbox/outbox exists | reuse committed outcome |
| after event publish, before sent marker | event may duplicate | republish same ID; observers deduplicate |

A stale fence cannot commit over a newer owner.

## Local-lite

Local-lite supports immediate deterministic task execution and durable restart/
redelivery while collapsing API + execution into one Python process. It deliberately
does not emulate Workflow-only waits, approvals, timers or fan-out/fan-in.

## Workflow tier

The optional local and distributed Workflow profiles implement:

- input-required/auth-required pauses and continuation;
- owner-coordinated cancellation;
- delays/timers;
- fan-out/fan-in;
- worker restart/replacement recovery.

Workflow state is durable; completion already committed is never rewritten by a late
cancellation or replay.

The current live/full acceptance exercises Workflow fan-out/fan-in, abrupt worker
restart and cancellation. The Kubernetes JetStream deployment gate verifies task
completion across worker replacement.

## Offline/fan-out semantics

Distributed worker replicas share the intended competing consumer identity.
Independent observers use distinct durable identities, so they fan out rather than
load-balance one another. Retention determines how far a late observer can rebuild;
SQL backups/projections cover history beyond broker retention.

## Encryption

Optional encryption changes payload confidentiality/authenticity but not delivery,
fencing, idempotency or Workflow state semantics.
