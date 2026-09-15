# Foundation Research and Decision Matrix

This note captures the reasoning behind the reference stack so implementation agents do not need to repeat the same architecture investigation.

## Objective

Minimize bespoke engineering while preserving:

- A2A interoperability;
- durable task delivery;
- worker pools and fan-out observers;
- long-running durable orchestration;
- security boundaries;
- self-hostability;
- broker portability.

## Options

| Option | Durable queue | Durable workflows | A2A-native | Broker portability | Operational weight | Bespoke work for A2A-MQ | Decision |
|---|---:|---:|---:|---:|---:|---:|---|
| Direct NATS JetStream | Excellent | No | No | Low | Low | Medium/High | Good broker, not enough alone |
| RabbitMQ | Excellent | No | No | Low | Medium | Medium/High | Strong second backend |
| Kafka | Excellent event log | No | No | Low | High | High | Not first choice for task bus |
| Apache Pulsar | Excellent | No | No | Low | High | High | Too heavy for MVP |
| Dapr + JetStream | Excellent | Yes via Dapr Workflow/Agents | Adapter required | High | Medium | **Low** | **Reference choice** |
| Dapr + RabbitMQ | Excellent | Yes | Adapter required | High | Medium | Low | Portability target |
| Temporal + broker | Excellent workflow | Yes | Adapter required | Medium | Medium/High | Medium | Excellent alternative when Temporal already exists |
| Restate + broker | Excellent workflow | Yes | Adapter required | Medium | Medium | Medium | Promising alternative, not minimal reference stack |
| Agent gateway only | No | No | Often A2A-aware | N/A | Low/Medium | High for durability | Complement, not task broker |
| Custom broker/runtime | Unknown | Unknown | Could be | None | Very high | **Very high** | Rejected |

Ratings are architecture judgments for this project's goal, not general product rankings.

## Why Dapr + JetStream wins for the reference implementation

### Dapr removes non-differentiating work

The project can use Dapr for:

- Pub/Sub abstraction;
- workflows/durable execution;
- state-store APIs;
- resiliency;
- workload identity and mTLS in Daprized environments;
- topic publish/subscribe scopes;
- secret-store integration;
- observability integration.

The A2A-MQ code can therefore stay focused on protocol mapping and correctness.

### JetStream fits the default deployment

JetStream gives:

- persistent streams;
- explicit acknowledgements;
- redelivery;
- durable consumers;
- queue-group/competing-consumer patterns;
- fan-out through distinct durable consumers;
- a small local deployment footprint.

Dapr's current JetStream component specifically supports `durableName` and `queueGroupName` for competing consumers. This must be reflected carefully in deployment configuration.

## Why RabbitMQ is still important

RabbitMQ is arguably the most familiar enterprise task-queue option and has excellent routing, quorum queue, dead-letter and operational capabilities. The project gains credibility only if "broker-neutral through Dapr" is demonstrated rather than asserted, so RabbitMQ is the preferred second-backend conformance target.

## Why not make Dapr Agents itself the public protocol?

Dapr Agents is an implementation/runtime layer, while A2A is the interoperability contract. A2A-MQ should keep these concerns separate:

- external semantics: A2A;
- queue envelope/profile: A2A-MQ;
- durable runtime/reference implementation: Dapr Agents/Workflow;
- broker: JetStream by default.

This avoids locking remote peers to one agent framework.

## Existing adjacent projects

### A2A custom protocol bindings

A2A explicitly supports additional bindings beyond its core bindings. This is the standards path A2A-MQ can eventually use after conformance work.

### A2A experimental SLIMRPC binding

The A2A ecosystem has explored alternate transport bindings such as SLIMRPC. That validates the general idea that A2A semantics can ride over transports other than direct core HTTP/gRPC bindings, but it does not provide the exact durable broker/task-bus design in this repository.

### Agent gateways

Agent gateways are useful for routing, policy, identity and observability across A2A/MCP traffic, but they are not by themselves a durable work queue with offline workers and retained commands.

### Temporal/Restate

Both durable-execution systems are strong alternatives when an organization already operates them. A2A-MQ should remain architecturally compatible with replacing Dapr Workflow behind adapter boundaries, but supporting several workflow engines in the MVP would increase work without validating the core hypothesis.

## Research conclusion

There is enough existing infrastructure that creating a new queue is unnecessary. There is still useful missing glue: a precise A2A-aware durable messaging profile, mappings, secure envelopes, bridges, conformance tests and examples. That is the project's intended differentiation.

## Primary references

- A2A specification: https://a2a-protocol.org/latest/specification/
- A2A custom protocol bindings: https://a2a-protocol.org/latest/topics/custom-protocol-bindings/
- A2A project: https://github.com/a2aproject/A2A
- Dapr Agents: https://docs.dapr.io/developing-ai/dapr-agents/
- Dapr Workflow: https://docs.dapr.io/developing-applications/building-blocks/workflow/workflow-overview/
- Dapr Pub/Sub: https://docs.dapr.io/developing-applications/building-blocks/pubsub/
- Dapr JetStream component: https://docs.dapr.io/reference/components-reference/supported-pubsub/setup-jetstream/
- Dapr Pub/Sub scopes: https://docs.dapr.io/developing-applications/building-blocks/pubsub/pubsub-scopes/
- NATS JetStream concepts: https://docs.nats.io/nats-concepts/jetstream
- RabbitMQ documentation: https://www.rabbitmq.com/docs/
