# ADR-0001: Use Dapr + NATS JetStream as the reference foundation

- Status: Accepted
- Date: 2026-09-15

## Context

A2A-MQ needs durable publish/subscribe, retries, worker pools, fan-out observers, durable long-running orchestration, state, security primitives and observability. Building those capabilities directly would dominate the project and duplicate mature infrastructure.

Candidate foundations considered:

- direct NATS JetStream;
- RabbitMQ;
- Apache Kafka;
- Dapr + pluggable broker;
- Temporal/Restate plus a broker;
- a custom broker/runtime.

## Decision

Use **Dapr as the application/runtime abstraction** and **NATS JetStream as the default reference Pub/Sub backend**.

Use Dapr Workflow/Dapr Agents for durable orchestration rather than implementing a custom workflow engine.

Application/domain code must not depend directly on NATS APIs on the reference path.

## Rationale

- Dapr removes substantial bespoke work across pub/sub, workflow, state, identity, resiliency and telemetry.
- JetStream is lightweight to self-host and provides the durable queue/event properties needed by the MVP.
- Dapr preserves the option to validate RabbitMQ or another backend later.
- The project can focus on A2A semantics, routing, security envelope and interoperability.

## Consequences

Positive:

- smaller implementation surface;
- faster MVP;
- broker portability;
- mature operational primitives.

Negative:

- Dapr becomes a runtime dependency for the reference architecture;
- backend differences still exist and must be tested rather than assumed away;
- JetStream's Dapr consumer configuration requires care for competing vs fan-out consumers.

## Rejected

### Direct JetStream only

Less infrastructure, but would require us to implement more state/workflow/security/telemetry integration and would couple application code to NATS.

### RabbitMQ as first backend

Excellent queue semantics and viable second backend, but JetStream gives a compact reference deployment and subject-based routing. No claim is made that JetStream is universally better.

### Kafka as first backend

Very strong event-streaming platform but heavier than needed for the initial durable task-dispatch workload.

### Custom broker/workflow engine

Rejected categorically unless future evidence shows an essential requirement cannot be met with mature infrastructure.
