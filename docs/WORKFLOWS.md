# Workflows and failure boundaries — v0.1.0-rc2

## Durable task path

A command is durably admitted, delivered at least once, and completed through
transactional task/outbox state. Publishing an event and recording its local publish
marker are separate operations; duplicate publication remains possible and is
handled with stable event identity/idempotency.

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

Local-lite supports immediate deterministic task execution and durable
restart/redelivery while combining API + local executor in one Python process.

It deliberately rejects Workflow-only features:

- input-required continuation;
- auth-required approval;
- non-zero delays/timers;
- fan-out/fan-in.

Queue Lab's pause switch is only a local delivery-boundary test control. It does not
implement a workflow scheduler.

## Local Workflow

The optional local Workflow profile uses the official Dapr Workflow runtime and adds
Placement + Scheduler.

It supports:

- input-required/auth-required pauses;
- continuation;
- approve/reject;
- durable timers/delays;
- cancellation;
- fan-out/fan-in paths.

RC2 focused Linux validation exercised input continuation, approve/reject and a
two-second durable timer on the real five-process local Workflow topology.

## Distributed Workflow

The distributed profile separates API and Workflow workers and uses shared durable
state. Worker replicas share the intended logical consumer/workflow identity.

Earlier broader acceptance exercised fan-out/fan-in, cancellation and worker
restart/replacement. The earlier JetStream Kubernetes deployment gate verified task
completion across worker replacement.

These are historical SHA/image-bound results; see [VALIDATION.md](VALIDATION.md).

## Terminal state and replay

A task outcome already committed as terminal is not reopened by a late cancellation
or duplicate/replayed command. Idempotency and immutable identities protect the
committed result.

## Independent observers

Distributed worker replicas compete within their logical consumer group.
Independent observers use distinct durable identities so they receive their own copy
rather than load-balance each other.

Broker retention controls how far a late observer can rebuild directly from the
message log; SQL backup/projection strategy covers longer history.

## Encryption

Optional payload protection changes confidentiality/authenticity of transported
content. It does not change delivery semantics, fencing, idempotency or Workflow state
rules.
