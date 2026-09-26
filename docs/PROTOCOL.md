# Protocol profile — current v0.1.0 pre-release

A2A-MQ uses an experimental outer envelope (`envelopeVersion=1.0`) carrying A2A
1.0 payloads. The public A2A binding is implemented with the pinned official SDK.

The normative outer schema is `spec/envelope.schema.json`; the packaged
`src/a2a_mq/schema.json` must remain byte-identical.

## Identity and routing

Every envelope carries tenant, message/correlation identity, source, creation time,
operation, kind and payload. Commands have exactly one canonical target (agent or
skill). Event payloads carry task/context/causation identity and a positive sequence.

Retries reuse immutable message identity/content. A new outer message ID represents
new work. Routes are derived from validated fields, never free-form text or an
untrusted reply URL.

Tenant scope is mandatory in routing, deduplication, SQL keys and queries.

## Serialization

Wire format is UTF-8 JSON. Duplicate keys, non-finite numbers, excessive nesting and
oversized documents fail closed. Unknown outer fields are rejected for envelope 1.0;
extensions belong in the explicit extension surface.

A2A payloads use the pinned 1.0 SDK shape and enum values. Do not mix legacy 0.3
payload shapes into the 1.0 profile.

## Supported lifecycle

- `message.send`: durable task admission.
- task snapshots/status/artifacts: validated task lifecycle projection.
- get/list: durable query surfaces.
- cancel: owner-coordinated cancellation.
- continuation / input-required / auth-required: distributed/Workflow profile.
- streaming/subscription: official A2A client surface.
- local-lite deliberately rejects Workflow-only semantics rather than faking them.

## Dapr/CloudEvent boundary

Dapr Pub/Sub carries CloudEvents 1.0. CloudEvent identity/source/type must agree with
the decoded A2A-MQ envelope, and tenant/topic/route checks remain enforced.

CloudEvent/envelope source fields are claims, not workload authentication. Production
identity uses JWT/workload policy, Dapr API/app tokens, Dapr component scopes and
broker policy.

## Delivery semantics

Delivery is at-least-once. Stable identity, immutable content hashes, fencing,
transactional inbox/outbox state and monotonic projections protect retries and stale
writers. No generic exactly-once guarantee is made for arbitrary external effects.

## Optional payload protection

Encrypted routes use authenticated encryption plus Vault-backed key wrapping under
the configured route/receiver contract. Plaintext is not accepted as a silent
fallback on an encrypted route. Real TLS Vault Transit is part of current FULL_PASS.
