# A2A-MQ: Durable Queue-Native Communication for Agent2Agent Systems

## Abstract

Agent-to-agent systems need more than request/response RPC. Real deployments contain long-running tasks, temporarily unavailable workers, retries, human-in-the-loop pauses, heterogeneous agent runtimes, fan-out observers, security boundaries, and workloads that must survive process restarts. The A2A protocol provides a strong semantic model for tasks, messages, artifacts, agent discovery and lifecycle transitions, while mature brokers and workflow engines already solve durable delivery and execution.

A2A-MQ proposes a **queue-native A2A interoperability profile** rather than a new broker or agent framework. It maps A2A operations and events onto durable publish/subscribe infrastructure, using Dapr as the portability and runtime layer and NATS JetStream as the default broker. The design preserves A2A task semantics, introduces explicit routing and correlation envelopes, uses at-least-once delivery with idempotent consumers, and optionally encrypts message payloads end-to-end.

The main result of this design study is that a useful product can be built with comparatively little bespoke infrastructure: the novel work is concentrated in protocol mapping, task correlation, secure routing, conformance, bridges and developer tooling.

## 1. Problem

Direct HTTP/gRPC A2A communication works well when both peers are online and the caller can maintain a connection. It is less natural when:

- tasks run for minutes or hours;
- consumers scale independently;
- workers may be offline when work is submitted;
- several orchestrators need the same event stream;
- execution must survive restarts;
- back-pressure must be explicit;
- work needs dead-lettering and replay;
- the broker/operator should not necessarily see plaintext task content.

Traditional message brokers solve these delivery concerns, but they know nothing about A2A concepts such as `Task`, `Message`, `Artifact`, `contextId`, interrupted states, cancellation, or agent capability discovery.

A2A-MQ bridges those layers.

## 2. Design hypothesis

The hypothesis is:

> A thin A2A-aware messaging layer on mature durable infrastructure can provide the operational advantages of enterprise queues without inventing a parallel agent protocol or reimplementing a workflow engine.

The implementation should therefore minimize custom code. Dapr Agents and Dapr Workflow supply durable orchestration; Dapr Pub/Sub supplies a broker-neutral API; JetStream supplies durable low-latency messaging; the A2A-MQ layer supplies semantics and interoperability.

## 3. Upstream protocol alignment

The design targets A2A 1.0 semantics and was checked against the A2A v1.0.1 protocol definitions. A2A's core unit of work is a `Task`, with states including `SUBMITTED`, `WORKING`, `COMPLETED`, `FAILED`, `CANCELED`, `INPUT_REQUIRED`, `REJECTED`, and `AUTH_REQUIRED`. A2A also defines task status updates, artifact updates, task retrieval, listing, cancellation and subscription semantics.

The A2A specification permits additional protocol bindings beyond the core JSON-RPC, gRPC and HTTP+JSON bindings. A2A-MQ uses that extensibility model, but it deliberately treats formal custom-binding conformance as a later gate. Until that gate passes, the safe description is **A2A-compatible MQ transport/profile**.

## 4. Reference stack selection

### 4.1 Dapr instead of bespoke infrastructure

Dapr removes a large amount of plumbing:

- Pub/Sub abstraction and broker components;
- application identities and mTLS between Daprized services;
- topic scoping and access control;
- resiliency policies;
- state stores;
- workflows with durable execution;
- OpenTelemetry-compatible observability.

Dapr Agents builds higher-level agent patterns on these primitives. The project should reuse them rather than duplicating them.

### 4.2 JetStream instead of implementing a broker

JetStream provides persisted streams, durable consumers, acknowledgement/redelivery, retention policies, subject-based routing and clustering while keeping deployment comparatively small. It is a strong default for a self-hosted agent task bus.

RabbitMQ remains a viable backend through Dapr and should be added as an interoperability target, not as the first implementation target. Kafka is intentionally not the default because A2A-MQ's primary workload is durable task dispatch plus events rather than very-high-volume append-only event analytics.

## 5. Conceptual model

A2A-MQ separates three concepts:

1. **Commands** — one logical executor should act, e.g. send message, cancel task, continue an interrupted task.
2. **Events** — zero or more listeners observe status, artifacts, completion or failure.
3. **Queries** — task retrieval/listing is served from the durable task projection, optionally bridged to standard A2A HTTP/gRPC APIs.

This separation avoids pretending every RPC maps cleanly to pub/sub.

## 6. Routing model

The broker topology is static and low-cardinality. Task IDs are message metadata, not topic names.

Reference logical topics:

- `a2a.command.agent.<agentId>` — direct commands to an agent pool;
- `a2a.command.skill.<skill>` — capability-routed commands;
- `a2a.event.task` — task lifecycle events;
- `a2a.event.artifact` — artifact update events;
- `a2a.event.agent` — agent availability/capability changes;
- `a2a.dlq.<class>` — dead-letter classes.

Tenant/environment isolation can be represented as a prefix or as separate Dapr pubsub components/accounts. Large installations SHOULD favor infrastructure-level namespace/account isolation over ever-growing topic strings.

## 7. Delivery semantics

End-to-end exactly-once execution is not promised. The transport is **at least once**.

Every envelope contains an immutable `messageId` and operation identity. Consumers maintain a deduplication record and task transition guard. A redelivered command must either:

- return/re-publish the previously persisted outcome; or
- safely resume the same durable workflow;

but must not repeat a non-idempotent side effect without an application-level idempotency mechanism.

This is a core invariant, not an optimization.

## 8. Durable task lifecycle

A command accepted by the system creates or advances durable task state. Dapr Workflow is used for long-running orchestration. Worker code emits canonical A2A state changes and artifacts. A task projection stores the latest task representation for `GetTask` and `ListTasks`.

Interrupted A2A states map naturally to durable workflow suspension:

- `INPUT_REQUIRED`: workflow waits for a continuation message;
- `AUTH_REQUIRED`: workflow waits for refreshed/approved authorization material;
- cancellation: workflow receives a cancellation event and transitions to `CANCELED` when safely stopped.

## 9. Security model

Transport security and content confidentiality are distinct.

Baseline:

- TLS/mTLS where supported;
- Dapr workload identities;
- least-privilege publish/subscribe scopes;
- isolated broker credentials;
- secret-store backed configuration;
- no credentials inside A2A message bodies.

Optional payload confidentiality uses envelope encryption. The A2A payload is serialized, encrypted using an authenticated symmetric content-encryption key (AES-256-GCM in the reference profile), and the content key is wrapped for the authorized receiver or receiver group using a KMS/key-encryption key. A single global symmetric key is explicitly rejected because compromise would expose all historical and future traffic and makes rotation/authorization coarse.

## 10. Standard A2A interoperability

A bridge exposes normal A2A HTTP+JSON/JSON-RPC/gRPC endpoints and translates calls to the MQ profile. This provides immediate interoperability with standard A2A clients even before they understand an MQ binding.

Agent Cards can advertise the standard bridge endpoint. A future version may additionally advertise an A2A-MQ custom interface after binding requirements and interoperability tests are complete.

## 11. Multi-orchestrator model

No central master orchestrator is required. Multiple orchestrators may:

- publish commands for different domains;
- consume the global event stream using independent consumer groups;
- correlate events via `taskId`, `contextId`, `causationId` and `correlationId`;
- spawn child tasks and retain references using A2A task/message relationships.

The bus therefore acts as a coordination substrate, not as the source of business logic.

## 12. Failure model

The design assumes failures are normal:

- duplicate delivery;
- out-of-order event observation;
- worker crash after external side effect but before ack;
- broker disconnect;
- orchestrator restart;
- poison messages;
- unavailable KMS/secret store;
- cancellation racing completion;
- stale Agent Cards/capability caches.

The implementation plan and test plan require deterministic behavior for each case.

## 13. What A2A-MQ deliberately does not build

- a new message broker;
- a replacement for Dapr Workflow/Temporal/Restate;
- an LLM agent framework;
- a new agent discovery standard;
- a global identity provider;
- a bespoke distributed consensus system;
- a proprietary payload model.

Avoiding these areas is the main mechanism for keeping the project small and maintainable.

## 14. Evaluation criteria

The project is successful if the MVP demonstrates:

1. a standard A2A client can submit work through the bridge;
2. the receiving worker may be temporarily offline without losing the task;
3. a worker crash causes safe redelivery/resume;
4. duplicate delivery does not duplicate task side effects in the reference sample;
5. multiple workers compete for a command without double execution;
6. multiple orchestrators independently observe task events;
7. task state and artifacts survive component restarts;
8. unauthorized applications cannot publish/subscribe outside configured scopes;
9. encrypted mode keeps A2A payload content opaque to the broker;
10. the same application-level adapter can run against at least one second Dapr broker component with no domain-code changes.

## 15. Conclusion

There is a real gap between synchronous A2A interfaces and the durable asynchronous behavior needed by many enterprise agent workloads. However, most of the infrastructure required to fill that gap already exists. The highest-leverage implementation is therefore a narrow layer that preserves A2A semantics while delegating delivery, durability, workflow execution, identity and telemetry to proven infrastructure.

That is the scope of A2A-MQ.

## References

- A2A specification: https://a2a-protocol.org/latest/specification/
- A2A custom protocol bindings: https://a2a-protocol.org/latest/topics/custom-protocol-bindings/
- A2A GitHub project: https://github.com/a2aproject/A2A
- Dapr Agents: https://docs.dapr.io/developing-ai/dapr-agents/
- Dapr Workflow: https://docs.dapr.io/developing-applications/building-blocks/workflow/workflow-overview/
- Dapr Pub/Sub: https://docs.dapr.io/developing-applications/building-blocks/pubsub/
- Dapr JetStream component: https://docs.dapr.io/reference/components-reference/supported-pubsub/setup-jetstream/
- Dapr Pub/Sub scopes: https://docs.dapr.io/developing-applications/building-blocks/pubsub/pubsub-scopes/
- Dapr security: https://docs.dapr.io/concepts/security-concept/
- NATS JetStream: https://docs.nats.io/nats-concepts/jetstream
