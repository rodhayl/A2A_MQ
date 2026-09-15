# ADR 0001 - Dapr Pub/Sub with JetStream

Status: accepted, refined 2026-09-15.

Keep the public adapter on Dapr's HTTP Pub/Sub API. Use JetStream as the first
retained broker and RabbitMQ as a later portability test. Do not require a Dapr
Python SDK just to publish HTTP. Pin the runtime in deployment configuration;
Dapr Workflow/Agents enters in phase 4.

Reason: reuse mature delivery now and a durable runtime later without implementing
a broker. Cost: sidecars, versioned configuration and broker-specific validation.
Direct NATS could be smaller for a single-broker product; this trade-off is explicit.

Streams must be provisioned before sends. Each filter and logical consumer group
needs its own component identity. Replica groups compete; independent observer
groups fan out. See foundation evidence and the live tests. Portability and
production readiness are not established by this decision alone.
