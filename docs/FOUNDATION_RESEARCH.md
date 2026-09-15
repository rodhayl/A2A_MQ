# Foundation evidence and decision

Reviewed 2026-09-15. Entries below distinguish upstream capability from this
repository's implementation and from architectural judgment. Public primary sources
can change; selected version pins are baselines, not claims of perpetual latestness.

| Foundation | Reused capability | Work that remains | Decision |
|---|---|---|---|
| Dapr + JetStream | Pub/Sub API, retained messages, durable consumer groups | A2A profile, SQL consistency, operational validation | Reference |
| Dapr + RabbitMQ | Same API boundary with another mature broker | Queue/DLQ/ordering equivalence tests | Phase 9 |
| Direct JetStream | Retention and explicit consumer acknowledgements | Runtime-specific client wiring and workflow integration | Simpler only for NATS-only deployments |
| Temporal / Restate + broker | Durable execution | A2A and broker adapters; operate another runtime | Reuse when already deployed |
| Agent gateway | Routing, policy, protocol connectivity | Retained work and execution state | Complement, not a demonstrated queue replacement |
| New broker | No proven infrastructure reused | Persistence, replication, protocol and operations | Rejected |

Ratings are engineering judgments, not benchmarks. Dapr adds sidecars, configuration
and failure modes; a bare NATS adapter may be smaller for a one-broker deployment.
The value of Dapr here is reuse of the wider runtime when phase 4 arrives. We do
not require Dapr Agents in the pure domain, and do not claim it already performs
our task protocol mapping. Upstream overlap must be reviewed again before phase 4.

## Verified details that affect implementation

Dapr's JetStream component requires streams to exist. Its competing pattern uses
`durableName` and `queueGroupName`, not `consumerID`. Independent observers need
distinct pairs. NATS limits retention is appropriate for later observers; work-queue
retention is not a substitute for fan-out. An exhausted delivery count is not an
automatic move into a dead-letter queue. Dapr has an explicit dead-letter facility.

Dapr can preserve a user-provided CloudEvent. Runtime service mTLS does not by itself
secure the broker connection. The project must configure that connection separately.
A2A 1.0 examples use protobuf enum/field JSON conventions. The published Python SDK
1.1.2 declares 1.0 support, but optional SDK tests must actually run before conformance
is asserted. The SDK is not needed merely to call Dapr's HTTP Pub/Sub API.

## Primary sources

- A2A specification: https://a2a-protocol.org/latest/specification/
- A2A custom bindings: https://a2a-protocol.org/latest/topics/custom-protocol-bindings/
- Python SDK release: https://pypi.org/project/a2a-sdk/1.1.2/
- Dapr runtime releases: https://github.com/dapr/dapr/releases
- JetStream component: https://docs.dapr.io/reference/components-reference/supported-pubsub/setup-jetstream/
- CloudEvents: https://docs.dapr.io/developing-applications/building-blocks/pubsub/pubsub-cloudevents/
- Dead letters: https://docs.dapr.io/developing-applications/building-blocks/pubsub/pubsub-deadletter/
- Pub/Sub scopes: https://docs.dapr.io/developing-applications/building-blocks/pubsub/pubsub-scopes/
- NATS consumers: https://docs.nats.io/nats-concepts/jetstream/consumers
- Dapr Workflow: https://docs.dapr.io/developing-applications/building-blocks/workflow/workflow-overview/

Not all linked adjacent products were empirically evaluated. No unverified adoption
count, 'only implementation' statement or production benchmark is part of this decision.
