# Requirements

Requirement IDs are stable. Implementations and tests SHOULD reference them directly.

## 1. Functional requirements

### Protocol and task semantics

- **FR-001 — A2A semantic compatibility.** The system MUST preserve the meaning of A2A `Message`, `Task`, `TaskStatus`, `Artifact`, `contextId`, `taskId`, and task lifecycle states.
  - Acceptance: canonical objects can round-trip through the MQ adapter without semantic loss for fields supported by the selected A2A version.

- **FR-002 — Submit work.** A producer MUST be able to submit an A2A message/task command without the destination worker being online.
  - Acceptance: publishing succeeds while all workers are stopped; after workers start, exactly one worker pool instance receives the logical command.

- **FR-003 — Task lifecycle.** Workers MUST be able to emit `SUBMITTED`, `WORKING`, `COMPLETED`, `FAILED`, `CANCELED`, `INPUT_REQUIRED`, `REJECTED`, and `AUTH_REQUIRED` task states where applicable.
  - Acceptance: state transition validation rejects illegal transitions and accepts all documented legal transitions.

- **FR-004 — Artifacts.** Workers MUST be able to publish task artifact updates, including chunk metadata required to reconstruct streamed/partial artifacts.

- **FR-005 — Get task.** The system MUST expose the latest durable task projection by `taskId`.

- **FR-006 — List tasks.** The system MUST support bounded task listing/filtering sufficient to implement the targeted A2A `ListTasks` behavior.

- **FR-007 — Cancel task.** An orchestrator MUST be able to request cancellation and observe the resulting terminal state.

- **FR-008 — Continue interrupted task.** A producer MUST be able to continue a task in `INPUT_REQUIRED` or `AUTH_REQUIRED` state using the existing `taskId/contextId`.

- **FR-009 — Correlation.** Every command/event MUST expose `messageId`, `correlationId`, `causationId`, `taskId` where known, and `contextId` where known.

- **FR-010 — Agent targeting.** Commands MUST support direct agent targeting and capability/skill targeting.

- **FR-011 — Worker pools.** Multiple replicas subscribing to the same command route MUST form a competing-consumer group so one logical worker executes a command.

- **FR-012 — Fan-out observers.** Multiple orchestrators/observers MUST be able to independently consume the same task event stream using different subscription identities.

- **FR-013 — Standard A2A bridge.** The reference implementation MUST expose at least one standard A2A server binding (HTTP+JSON preferred for MVP) backed by the MQ transport.

- **FR-014 — Agent discovery.** The bridge MUST expose/consume A2A Agent Card information without inventing an incompatible discovery format.

### Delivery and durability

- **FR-020 — At-least-once delivery.** Command transport MUST tolerate redelivery and MUST NOT rely on broker-level exactly-once claims for application correctness.

- **FR-021 — Idempotency.** Consumers MUST deduplicate commands using immutable message/operation identity before non-idempotent side effects.

- **FR-022 — Durable command retention.** Accepted commands MUST survive worker and adapter restarts within configured broker retention limits.

- **FR-023 — Durable task projection.** Latest task state MUST survive orchestrator/worker restarts.

- **FR-024 — Retry.** Transient failures MUST be retried under explicit policy with bounded backoff.

- **FR-025 — Dead letter.** Poison/permanently failing messages MUST be moved or republished to a dead-letter route with enough metadata for diagnosis/replay.

- **FR-026 — Back-pressure.** Consumers MUST have configurable concurrency and in-flight limits.

- **FR-027 — Ordering guard.** Task projection MUST reject stale state updates using sequence/version information even if events are observed out of order.

### Security

- **FR-030 — Workload identity.** Each application/adapter MUST have a distinct runtime identity in production deployments.

- **FR-031 — Topic authorization.** Publish and subscribe permissions MUST be least-privilege and independently configurable per workload.

- **FR-032 — Transport encryption.** Production traffic MUST use encrypted transport wherever the selected broker/runtime supports it.

- **FR-033 — Optional payload encryption.** Producers MUST be able to encrypt the A2A payload independently of broker transport encryption.

- **FR-034 — Authenticated encryption.** The reference encrypted envelope MUST use an AEAD cipher; the MVP profile uses AES-256-GCM.

- **FR-035 — Key isolation.** The design MUST NOT require one global symmetric payload key shared by all agents.

- **FR-036 — Key rotation.** Encrypted envelopes MUST carry a non-secret key identifier/version sufficient for controlled rotation.

- **FR-037 — Metadata minimization.** Sensitive user content MUST NOT be placed in plaintext routing headers.

- **FR-038 — Secret management.** Broker credentials, wrapping keys and tokens MUST come from environment/secret stores, never source-controlled configuration.

### Observability and operations

- **FR-040 — Distributed tracing.** The envelope MUST carry W3C trace context or an equivalent mapping usable by OpenTelemetry.

- **FR-041 — Metrics.** The runtime MUST expose publish rate, consume rate, in-flight commands, processing latency, retry count, duplicate count, DLQ count and task terminal-state counts.

- **FR-042 — Structured logs.** Logs MUST be structured and include `messageId`, `taskId`, `contextId`, consumer identity and attempt where known.

- **FR-043 — Health/readiness.** Services MUST expose health/readiness checks that distinguish process health from dependency readiness.

- **FR-044 — Replay tooling.** Operators MUST have a controlled mechanism to inspect and replay DLQ messages without hand-editing broker internals.

### Portability

- **FR-050 — Dapr API boundary.** Domain application code MUST use the A2A-MQ adapter/Dapr API rather than JetStream-specific APIs in the MVP path.

- **FR-051 — Second-broker proof.** Before v1.0, the reference suite MUST pass against at least one second Dapr Pub/Sub backend (RabbitMQ is the preferred candidate).

- **FR-052 — Versioned envelopes.** The MQ envelope MUST contain a schema version and support additive evolution.

## 2. Non-functional requirements

- **NFR-001 — Simplicity.** A local developer MUST be able to start a complete reference stack with one documented command after prerequisites are installed.

- **NFR-002 — No custom broker.** The project MUST NOT implement broker persistence, consensus or replication.

- **NFR-003 — Horizontal scalability.** Stateless adapter/bridge components MUST support horizontal scaling.

- **NFR-004 — Availability.** No single orchestrator process may be required for already-accepted work to remain durable.

- **NFR-005 — Recovery.** Killing a worker while a task is in progress MUST result in retry/resumption according to documented policy rather than silent task loss.

- **NFR-006 — Performance target.** On a local reference environment, broker/adapter overhead excluding agent inference SHOULD keep median one-way command dispatch latency below 50 ms and p95 below 200 ms under the baseline test workload. These are engineering targets, not protocol guarantees.

- **NFR-007 — Payload size discipline.** Large binary artifacts SHOULD use object storage/URLs rather than unbounded broker messages. The default maximum inline payload MUST be configurable.

- **NFR-008 — Resource bounds.** Consumer concurrency, message size, history size, retries and retention MUST all have bounded configuration.

- **NFR-009 — Compatibility.** The standard A2A bridge MUST pass upstream SDK interoperability tests chosen for the pinned A2A release.

- **NFR-010 — Security default.** Example production manifests MUST be secure-by-default; deliberately insecure local-dev settings must be labeled as such.

- **NFR-011 — Auditable transitions.** Every task state transition MUST be attributable to a message/event and timestamp.

- **NFR-012 — Deterministic contracts.** Protocol schemas and state-transition rules MUST be machine-testable.

## 3. Explicit non-requirements for MVP

- **NR-001:** globally ordered events across unrelated tasks.
- **NR-002:** end-to-end exactly-once side effects.
- **NR-003:** arbitrary dynamic topic creation per task.
- **NR-004:** replacing A2A Agent Cards with a proprietary registry protocol.
- **NR-005:** implementing an LLM planning/orchestration framework.
- **NR-006:** implementing a distributed KMS.
- **NR-007:** broker federation across clouds in MVP.
- **NR-008:** formal standardization of the custom binding before interoperability evidence exists.

## 4. Release gates

### MVP gate

All FR-001 through FR-027, FR-040 through FR-043, FR-050, FR-052 and NFR-001 through NFR-005 must pass. Security baseline FR-030 through FR-032 and FR-038 is mandatory. Payload encryption FR-033 through FR-037 may ship as feature-flagged beta but must have tests before being described as supported.

### v1.0 gate

All requirements above must pass, including second-broker portability, payload encryption/key rotation and standard A2A bridge interoperability. The project must also complete the conformance review in `docs/PROTOCOL.md` before advertising an `A2A-MQ` custom protocol binding in Agent Cards.
