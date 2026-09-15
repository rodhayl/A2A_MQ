# ADR-0002: Guarantee at-least-once delivery and require idempotent processing

- Status: Accepted
- Date: 2026-09-15

## Context

Durable brokers can redeliver messages when a worker crashes or acknowledgements are lost. External side effects create a classic gap between "effect happened" and "message was acknowledged". Marketing an end-to-end exactly-once guarantee would be incorrect unless every external system participates in the same atomic transaction, which is not realistic for general agents.

## Decision

A2A-MQ defines its transport guarantee as **at least once**.

Correctness is achieved through:

- immutable logical message IDs;
- durable deduplication records;
- task state/version guards;
- application idempotency keys for external side effects;
- durable workflows where long-running coordination is needed;
- bounded retry and DLQ policies.

A retry by the producer for the same logical command reuses the same message ID.

## Consequences

- duplicate physical deliveries are expected and tested;
- worker handlers must not assume one delivery equals one execution attempt;
- applications integrating non-idempotent external APIs must use their idempotency features or a transactional/outbox-style design;
- documentation must never claim generic end-to-end exactly-once execution.
