# Protocol Profile

## 1. Status

This document defines the A2A-MQ transport/profile used by the reference implementation.

It is **not yet claimed as an official A2A custom protocol binding**. The implementation must first satisfy the interoperability and operation-completeness gates in this document. Until then, standard clients use the A2A bridge.

## 2. Versioning

Two versions are independent:

- `a2aVersion`: upstream A2A protocol version the payload conforms to;
- `envelopeVersion`: A2A-MQ transport envelope version.

MVP values:

```json
{
  "a2aVersion": "1.0",
  "envelopeVersion": "1.0"
}
```

Consumers MUST reject unsupported major envelope versions and SHOULD accept additive unknown fields within a supported major version.

## 3. Envelope

Canonical logical envelope:

```json
{
  "envelopeVersion": "1.0",
  "a2aVersion": "1.0",
  "messageId": "uuid",
  "operation": "message.send",
  "kind": "command",
  "source": "orchestrator-a",
  "target": {
    "agentId": "research-agent",
    "skill": null
  },
  "taskId": null,
  "contextId": "uuid-or-null",
  "correlationId": "uuid",
  "causationId": null,
  "sequence": 1,
  "createdAt": "2026-09-15T16:00:00Z",
  "expiresAt": null,
  "traceparent": "00-...",
  "contentType": "application/json",
  "contentEncoding": "identity",
  "encryption": null,
  "payload": {}
}
```

When encryption is enabled, `payload` is replaced by ciphertext fields described in `SECURITY.md` and the schema.

### 3.1 Required invariants

- `messageId` is immutable and globally collision-resistant.
- `correlationId` groups request/result/event flow.
- `causationId` points to the message/event that directly caused the current envelope, when known.
- `taskId` and `contextId` preserve upstream A2A meaning.
- `sequence` is monotonic within the emitting authority's task stream; consumers MUST NOT interpret it as a global broker sequence.
- routing metadata must not contain sensitive natural-language task content.

## 4. Kinds

- `command`: asks one logical executor to act.
- `event`: immutable notification that something occurred.
- `reply`: optional point-to-point response for transport-level acknowledgements/errors; task outcomes should normally be represented as canonical task/event payloads.

## 5. Operations

### 5.1 `message.send`

Payload: A2A `SendMessageRequest` or a serialization-equivalent object pinned to the selected SDK.

Route:

- explicit agent: `a2a.command.agent.<agentId>`;
- skill route: `a2a.command.skill.<skill>`.

Semantics:

1. validate envelope and A2A payload;
2. deduplicate by `messageId`/operation identity;
3. route to worker/workflow;
4. create/advance the A2A task if a task is produced;
5. publish task status/artifact events;
6. persist task projection before final command acknowledgement according to processing policy.

`returnImmediately=false` is an API-level behavior of the standard bridge. It does not require the broker publisher itself to keep a network connection open. The bridge may wait on correlated durable events/projection changes.

### 5.2 `task.get`

Served by the task projection. Native MQ clients SHOULD use the SDK query API rather than publishing a broker request. A pure-MQ request/reply form MAY be implemented later for environments where no query endpoint is allowed.

### 5.3 `task.list`

Served by indexed task projection with bounded pagination/filter semantics.

### 5.4 `task.cancel`

Command payload identifies `taskId` and optional metadata/reason.

Route: preferably to the logical owner agent/workflow route stored in the task projection. Cancellation is a request, not an immediate assertion that the task is canceled. The worker/workflow emits the canonical resulting task status.

### 5.5 `task.continue`

Transport helper for sending a new A2A message associated with an existing task/context after `INPUT_REQUIRED` or `AUTH_REQUIRED`. Semantically this maps to the upstream continuation/message-send behavior rather than inventing a new A2A task state.

### 5.6 `task.subscribe`

Native MQ consumers subscribe to `a2a.event.task` and filter/correlate by `taskId`. The standard A2A bridge converts matching events to the upstream streaming interface.

### 5.7 Push notification configuration

Not required for initial native-MQ clients because the event bus already supplies asynchronous updates. The standard bridge MUST either implement upstream push notification configuration correctly or advertise the operation as unsupported according to A2A rules. v1.0 conformance requires an explicit decision and test coverage; silent partial behavior is forbidden.

## 6. Events

### 6.1 `task.status`

Payload: A2A `TaskStatusUpdateEvent` semantics.

Route: `a2a.event.task`.

Required envelope fields: `taskId`, `contextId`, `correlationId`, `causationId`, `sequence`.

### 6.2 `task.artifact`

Payload: A2A `TaskArtifactUpdateEvent` semantics.

Route: `a2a.event.artifact`.

Chunk flags (`append`, `lastChunk` or serialization-equivalent upstream fields) must be preserved exactly.

### 6.3 `agent.changed`

Optional operational event for capability/availability cache invalidation. It does not replace Agent Cards as the interoperability contract.

## 7. Task state transition profile

Reference allowed transitions:

```text
UNSPECIFIED -> SUBMITTED
SUBMITTED -> WORKING | REJECTED | CANCELED | FAILED
WORKING -> WORKING | COMPLETED | FAILED | CANCELED | INPUT_REQUIRED | AUTH_REQUIRED | REJECTED
INPUT_REQUIRED -> WORKING | CANCELED | FAILED | REJECTED
AUTH_REQUIRED -> WORKING | CANCELED | FAILED | REJECTED
COMPLETED -> terminal
FAILED -> terminal
CANCELED -> terminal
REJECTED -> terminal
```

Repeated `WORKING -> WORKING` is allowed for progress updates. The transition validator must follow the pinned upstream A2A semantics; this table is the implementation profile and must be revised if upstream semantics differ.

## 8. Idempotency

### 8.1 Producer

A retry of the same logical publish MUST reuse the same `messageId`. Creating a new ID means creating a new logical command.

### 8.2 Consumer

Before invoking non-idempotent business logic, consumer checks the dedup store:

- unseen: atomically claim/process;
- processing: avoid parallel duplicate execution; redelivery policy determines wait/nack;
- completed: do not execute again; optionally republish stored outcome if needed;
- permanently failed: route according to failure policy.

The dedup retention window MUST be at least as long as the maximum plausible broker redelivery/replay window for protected operations.

## 9. Acknowledgement rule

A command MUST NOT be acknowledged merely because it was deserialized.

Default safe policy:

- durable workflow task: ack after durable workflow acceptance and dedup claim are persisted;
- short synchronous task: ack after durable task outcome/projection is persisted and required events are publish-safe;
- poison/permanent validation error: publish deterministic rejection/error, then ack or DLQ according to configured class;
- transient dependency error: do not ack; retry/redeliver under bounded policy.

## 10. Error classes

Transport profile errors:

- `INVALID_ENVELOPE`
- `UNSUPPORTED_ENVELOPE_VERSION`
- `UNSUPPORTED_A2A_VERSION`
- `UNROUTABLE`
- `UNAUTHORIZED_ROUTE`
- `DECRYPTION_FAILED`
- `EXPIRED`
- `DUPLICATE`
- `TRANSIENT_DEPENDENCY_FAILURE`
- `PERMANENT_PROCESSING_FAILURE`
- `PROJECTION_CONFLICT`

Where a standard A2A error mapping exists, the bridge must map to it rather than exposing transport-internal codes as the public A2A API contract.

## 11. Expiry

Commands MAY carry `expiresAt`. Consumers receiving an expired command MUST NOT execute business logic. They publish/record an expiry outcome appropriate to the operation and acknowledge or DLQ it according to policy.

Broker retention TTL and semantic command expiry are separate concepts.

## 12. Large artifacts

Default rule:

- small text/JSON parts may be inline;
- large binary artifacts SHOULD be stored in object storage and represented through an A2A URL/file reference with appropriate authorization;
- inline maximum size is deployment-configurable and tested.

Do not fragment arbitrary huge files into thousands of broker messages unless the application explicitly needs chunk streaming.

## 13. Trace context

The envelope carries `traceparent` and optional `tracestate`. New processing spans link or parent correctly according to asynchronous tracing conventions. `correlationId` is a business/protocol correlation identifier and does not replace a trace ID.

## 14. Topic/route normalization

Agent IDs and skill route names used in topics must match a strict safe character profile. The reference implementation normalizes to lowercase ASCII slug identifiers and rejects ambiguous/unsafe route strings. Human-readable names remain inside Agent Cards/payload metadata.

## 15. Custom-binding conformance gate

Before A2A-MQ is advertised as a custom A2A protocol binding, all of the following must be true:

1. every mandatory A2A operation for the selected protocol version has a documented mapping or standards-compliant unsupported behavior;
2. serialization rules are normative and machine-tested;
3. errors are mapped consistently;
4. authentication/security advertisement has a defined mapping;
5. Agent Interface discovery syntax for the binding is specified;
6. at least two independent implementations, or one implementation plus an independent conformance client, interoperate;
7. bridge vs native-MQ semantic equivalence tests pass;
8. the profile documents ordering, delivery and streaming semantics without claiming guarantees the broker/runtime cannot provide.

Until then, Agent Cards should advertise the standard bridge interface only.
