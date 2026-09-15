# Protocol profile - revision 2

Status: experimental `envelopeVersion=1.0`, `a2aVersion=1.0`. The public A2A binding
is not implemented in phases 0-3. The normative outer shape is
`spec/envelope.schema.json`; runtime validation adds semantic cross-field checks.
The packaged schema must be byte-identical to that file.

## Identity and addressing

Every envelope has `tenantId`, `messageId`, `correlationId`, `source`, `createdAt`,
`operation`, `kind` and `payload`. A command has exactly one non-null target:
`agentId` or `skill`. Events have no target, but require task/context/causation IDs
and a positive `sequence`. Task and context IDs are opaque strings, not necessarily
UUIDs. Transport message IDs are UUIDs. The A2A inner message ID is a separate ID.
Retries MUST reuse the complete immutable envelope. A new outer ID means new work.

Routes derive from validated fields, never from natural-language content or an
untrusted arbitrary reply URL. Tokens match `[a-z0-9][a-z0-9-]{0,62}`. Tenant scope
is mandatory in routing, deduplication, SQL keys and queries.

## Serialization

The wire document is UTF-8 JSON. Duplicate object keys, non-finite numbers, excessive
nesting and oversized documents are rejected before application logic. Timestamps
must include timezone information. Unknown outer fields are rejected in revision
1.0; additive application information belongs in `extensions`. A future schema
revision requires an explicit compatibility change, not a wildcard version match.

A2A 1.0 uses protobuf-style JSON names and enum values, e.g. `ROLE_USER`,
`TASK_STATE_COMPLETED`, `parts: [{"text": "hello"}]`. Do not mix the 0.3 `kind/text`
shape with the 1.0 profile. Payload codecs validate the supported structural subset
and retain extension fields. Full upstream interoperability remains a distinct
SDK/bridge test, not a conclusion from round-trip equality.

## Supported operations by layer

| Operation | Payload | Initial behavior |
|---|---|---|
| `message.send` | A2A SendMessageRequest | New deterministic reference task |
| `task.snapshot` | Complete A2A Task | Durable monotonic projection |
| `task.status` | A2A TaskStatusUpdateEvent | Codec; not snapshot application |
| `task.artifact` | A2A TaskArtifactUpdateEvent | Codec; delta assembly deferred |
| cancel / continuation / streaming | Upstream A2A semantics | Explicitly unsupported until phase 4/5 |
| get / list | Stored Task objects | Internal query API, not an MQ operation |

For task-bearing payloads, outer/inner task and context IDs must agree. A send may
refer to an existing task in the codec; the phase-3 echo worker rejects that path.
An unsupported encrypted envelope is rejected. Plaintext never acts as a fallback.

## Dapr envelope

Publish a CloudEvent 1.0 with the outer message ID as its `id`, explicit JSON
content type, a generated `source`, and the A2A-MQ envelope as `data`. Preserve
trace context. The subscriber checks the configured component/topic against the
computed route. Neither CloudEvent `source` nor envelope `source` authenticates a
publisher; transport identity and authorization are separate security controls.

## State validation and snapshot ordering

Local executor transitions use a documented strict transition graph. Public A2A
terminal states are completed, failed, canceled and rejected. Unknown/unspecified
is not emitted as a task status. Repeated nonterminal progress can be allowed;
terminal tasks are not reopened.

A projection can begin from any valid full snapshot. A newer snapshot is accepted
only if its state is reachable from the current state, or is an identical terminal
snapshot. It need not be the immediately adjacent execution state. Reject same
version with different identity/content, changed context/owner, and terminal
mutation. Suppress duplicate event IDs before applying state. Sequence values
are task-authority versions, not timestamps, broker offsets or delivery attempts.

## Outcomes and error semantics

Broker publish success means accepted by the transport, not task completion. A
claim means reserved, not done. ACK only after safe durable outcome acceptance.
Permanent validation/expiry/identity-conflict errors are quarantined through a
confirmed DLQ publish before success. Transient dependency failures return RETRY.
Unexpected exceptions are retryable and logged without payloads. A DLQ publish
failure also returns RETRY. `DROP` is not used as a synonym for dead-lettering.

Semantic expiry is checked before new execution; completed duplicate outcomes
remain completed. Broker retention is independent. `maxDeliver` exhaustion alone
does not move a JetStream message to a DLQ; configure and test Dapr's dead-letter
and retry behavior explicitly.
