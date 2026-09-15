# Local deployment fixture

This directory is for development only. No production TLS, identity mapping, key
management or multi-node durability claim is made. Never use real secrets/tasks.

Phase 0 establishes the component contract. Phase 2 adds the executable compose
fixture and role-specific generated components. The chosen Dapr runtime baseline
is 1.18.2. Python code uses Dapr's HTTP API, not an unpinned SDK.

Use one retained limits-policy stream covering `a2a.v1.demo.>` and provision it
before publishing. A worker pool uses the same durable/group in all replicas.
Observer A and B use different pairs, each subscribed to the single event route.
Do not reuse one durable consumer across different subscription filters.

The old global component is now a publisher-only illustration; it does not assert
competing/fan-out configuration. ACK timing is bounded short-task completion plus
SQL outbox persistence, not the duration of a future long-running agent workflow.

Real exit tests require Docker with Dapr/JetStream images and, separately,
PostgreSQL. Missing binaries/network must be reported as unavailable, never replaced
with a mock result described as a live broker pass. See `docs/VALIDATION.md`.
