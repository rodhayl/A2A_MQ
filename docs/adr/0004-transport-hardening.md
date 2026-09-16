# ADR 0004: phase-2 transport lifecycle and deterministic reference replay

Status: implemented on the hardening branch, locally tested; live Dapr integration
unverified. This does not add the missing durable storage or workflow layers.

## Decision

The echo fixture derives event identity from tenant, logical consumer, command ID
and sequence. Its `createdAt` is now copied from the immutable request rather than
read from the clock on every attempt. This fixture-specific timestamp is not a
measurement of processing time. Retrying the same command emits exactly the same
wire bytes for each event ID, including after a fresh process start. Production
execution should persist outcomes/outbox bytes rather than recompute external effects.

Dapr publish and outbound health calls consume only response status and close
streamed responses without buffering irrelevant bodies. An outer cooperative timeout
bounds the awaited HTTP operation. Numeric timeout configuration excludes booleans,
nonfinite and out-of-range values.

Subscriber `callback_timeout` defaults to 30 seconds and accepts values in (0, 300].
It starts before semaphore admission and covers body reading and the awaited handler.
Quarantine has its own bounded attempt after a permanent error; readiness also has
a bounded wait. These are separate budgets, not a universal 30-second wall-clock SLA.
Timeouts return RETRY, and successful quarantine is required before acknowledging a
permanent failure. External cancellation is propagated rather than converted to SUCCESS.
Handlers must cooperate with cancellation; CPU-blocking or cancellation-suppressing
code needs an independent process supervision boundary.

App-token authentication uses pure ASGI middleware so body reading and cancellation
are not altered by a task-wrapping HTTP middleware. Duplicate token headers are
rejected. CloudEvent ID, source and type must agree with the envelope in addition
to the existing tenant/topic checks. This is consistency validation, not a signature,
full tenant authorization model or production workload identity.

## Validation and limits

Regression tests cover response closure, header waits, callback admission/body/handler
budgets, slot release, readiness, shutdown cleanup, credential ambiguity, metadata
consistency and external cancellation. Read the exact executed scopes in
[the hardening record](../validation/hardening-20260916.md). Delivery remains
at-least-once; volatile demonstration processing remains volatile; external effects
need their own idempotency contract.
