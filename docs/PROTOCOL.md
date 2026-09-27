# Protocol profile — v0.1.0-rc2

A2A-MQ uses an experimental outer envelope (`envelopeVersion=1.0`) carrying A2A
1.0 payloads. The public A2A binding is implemented with the pinned official SDK.

The normative outer schema is `spec/envelope.schema.json`; the packaged
`src/a2a_mq/schema.json` must remain byte-identical.

## Identity and routing

Every envelope carries tenant, message/correlation identity, source, creation time,
operation, kind and payload. Commands have one canonical target (agent or skill).
Event payloads carry task/context/causation identity and a positive sequence.

Retries reuse immutable message identity/content. A new outer message ID represents
new work.

Routes are derived from validated fields, never an untrusted reply URL. Tenant scope
is mandatory in routing, deduplication, storage keys and queries.

The built-in Local-lite profile subscribes to its configured
`a2a.v1.<tenant>.command.agent.<source>` route and does not synthesize arbitrary
routes through Queue Lab.

## Serialization

Wire format is UTF-8 JSON. Duplicate keys, non-finite numbers, excessive nesting and
oversized documents fail closed.

Unknown outer fields are rejected for envelope 1.0; extensions belong in the explicit
extension surface.

A2A payloads use the pinned 1.0 SDK shape/enum values. Legacy 0.3 payload shapes are
not part of this profile.

## Supported A2A lifecycle

Implemented surfaces include:

- `SendMessage`;
- `SendStreamingMessage`;
- `GetTask`;
- `ListTasks`;
- `CancelTask`;
- `SubscribeToTask` for non-terminal tasks;
- Agent Card discovery;
- Workflow continuation/input/auth surfaces where the Workflow profile is active.

Local-lite deliberately rejects Workflow-only semantics rather than faking them.

RC2 focused tests and Chromium smoke exercised send/get/list/cancel, streaming,
subscription and terminal-subscription rejection using the official A2A surface.

## Dapr / CloudEvent boundary

Dapr Pub/Sub carries CloudEvents 1.0.

CloudEvent ID/source/type, topic and tenant must agree with the decoded A2A-MQ
envelope. Envelope/CloudEvent source fields are protocol claims, not workload
authentication.

Production identity relies on verified principals/workload configuration, Dapr API
and app tokens, component scopes and broker policy.

## Delivery semantics

Delivery is at-least-once.

Protection against retry/stale writers comes from:

- stable immutable identity;
- content hashes/conflict detection;
- tenant/consumer scoping;
- leases/fencing where distributed claims are used;
- transactional task/inbox/outbox state;
- monotonic task projection.

No generic exactly-once guarantee is made for arbitrary external effects.

## DLQ replay

Replay preserves/revalidates the archived identity and route. It does not convert a
dead letter into new business intent and does not bypass owner/tenant authorization.

## Optional payload protection

Encrypted routes use authenticated encryption plus Vault-backed key wrapping under
the configured receiver/route contract. Plaintext is not accepted as a silent
fallback on an encrypted route.

Earlier full acceptance includes a real TLS Vault Transit gate; RC2 did not rerun it.

## Version/evidence boundary

Package version remains `0.1.0`; the latest tested release candidate is
`v0.1.0-rc2`.

Protocol implementation status is current here; exact test evidence is SHA-bound in
[VALIDATION.md](VALIDATION.md).
