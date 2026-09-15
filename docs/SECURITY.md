# Security Model

## 1. Goals

A2A-MQ must protect:

- who may publish commands;
- who may consume commands/events;
- confidentiality and integrity in transit;
- optional confidentiality of payloads from broker/operators;
- integrity of task state transitions;
- credentials and authorization material;
- auditability without leaking sensitive content.

## 2. Threat model

Consider:

- compromised worker attempting to read unrelated tasks;
- compromised orchestrator publishing to unauthorized agent routes;
- broker operator or broker compromise observing stored messages;
- replay of previously valid commands;
- duplicate/redelivered messages;
- tampering with ciphertext or routing metadata;
- malicious oversized/poison payloads;
- stale key use after rotation;
- accidental secret logging.

A2A-MQ does not attempt to protect a payload from an endpoint that is legitimately authorized to decrypt it and is itself compromised.

## 3. Layered controls

### Layer A — network/runtime

- TLS to broker in production;
- Dapr mTLS/workload identity where applicable;
- network policies/firewalls;
- broker authentication credentials scoped per workload/environment.

### Layer B — routing authorization

Use Dapr Pub/Sub scopes and broker-native account/subject permissions where useful. An agent that consumes `a2a.command.skill.translate` should not automatically be able to consume `a2a.command.skill.payroll`.

Defense in depth is encouraged: Dapr scope + broker subject/account permission.

### Layer C — application authorization

The adapter validates envelope source/target and operation policy. Topic access alone does not authorize every A2A operation.

### Layer D — payload encryption (optional)

Encrypt A2A payload content before publishing when confidentiality must survive broker compromise/operator access.

## 4. Why not one shared symmetric key?

A single symmetric key across orchestrators/workers is simple but undesirable:

- any participant can decrypt every payload;
- compromise has a very large blast radius;
- offboarding is difficult;
- rotation requires coordinated global changes;
- sender/receiver authorization cannot be expressed cleanly.

The reference design uses per-message random content keys plus key wrapping.

## 5. Envelope encryption profile

Reference algorithm:

1. generate a random 256-bit content-encryption key (CEK);
2. serialize the A2A payload deterministically enough for the implementation contract;
3. encrypt using AES-256-GCM with a fresh 96-bit nonce;
4. authenticate selected non-secret envelope fields as Additional Authenticated Data (AAD);
5. wrap the CEK using the receiver/group key in a KMS or key-encryption service;
6. publish ciphertext, nonce, tag/combined ciphertext, wrapping metadata and key ID.

Example logical encryption object:

```json
{
  "alg": "A256GCM",
  "keyManagement": "KMS-WRAP",
  "keyId": "agent-group/research:v3",
  "wrappedKey": "base64...",
  "nonce": "base64...",
  "aadProfile": "a2a-mq-v1"
}
```

The exact serialized representation belongs to `spec/envelope.schema.json` and implementation tests.

## 6. AAD

AAD SHOULD include immutable routing/correlation fields needed to detect tampering, such as:

- envelope version;
- message ID;
- operation;
- source;
- target routing identity;
- task ID/context ID when present;
- created timestamp;
- expiry timestamp.

Fields expected to be changed by intermediary infrastructure must not be included unless the change protocol is explicit.

## 7. Key distribution options

Preferred order:

1. cloud/on-prem KMS with per-service authorization;
2. Vault/HSM-backed wrapping service;
3. development-only static KEK injected through a secret store.

Static keys committed to source/config files are forbidden.

For group consumption (worker pool), workers share authorization to unwrap a group KEK through KMS policy; they do not need to share plaintext KEK material in configuration.

## 8. Rotation

Every encrypted envelope names a key ID/version. Rotation procedure:

1. provision new KEK version;
2. allow authorized consumers to read old + new versions;
3. producers begin wrapping with new version;
4. wait past maximum message retention/replay window;
5. retire old encrypt permission;
6. retire old decrypt permission when no retained messages require it, subject to archival policy.

Do not destroy a key version while durable encrypted messages still depend on it.

## 9. Replay protection

Replay is handled by multiple controls:

- immutable `messageId` deduplication;
- semantic `expiresAt` where appropriate;
- legal task transition validation;
- bounded dedup retention at least as long as supported replay/redelivery window;
- optional signed/audited operator replay action.

Encryption alone does not prevent replay.

## 10. Authorization material

Do not put reusable access tokens, private keys or passwords directly into ordinary A2A text parts.

For `AUTH_REQUIRED` continuation, prefer:

- short-lived scoped token references;
- secret-store handles;
- delegated authorization workflows;
- opaque credentials delivered through a dedicated secure channel where possible.

Logs must redact authorization headers and secret material.

## 11. Broker/operator confidentiality caveat

Even with encrypted payloads, some metadata remains visible to infrastructure so routing works, potentially including:

- topic/subject;
- source workload identity;
- target route;
- timestamps;
- message size;
- task/context IDs if kept plaintext.

High-sensitivity deployments may pseudonymize identifiers or use coarser routing namespaces, but should not encrypt fields the broker/runtime needs for routing.

## 12. Input validation

Consumers enforce:

- maximum message size;
- maximum parts/artifacts/history limits;
- supported content/media types;
- safe URL schemes where URL parts are dereferenced;
- schema validation;
- strict route identifier syntax;
- expiry checks;
- decompression limits if compression is introduced.

Never deserialize arbitrary executable object formats from untrusted agents.

## 13. Audit

Security-relevant events include:

- authorization denied;
- decryption/authentication failure;
- expired command;
- DLQ transition;
- replay action;
- key version change;
- repeated duplicate attack pattern;
- invalid state transition.

Audit records should contain identifiers and reason codes, not sensitive prompt/task contents by default.

## 14. Minimum production checklist

- [ ] broker TLS enabled;
- [ ] workload identities separated;
- [ ] Dapr publish/subscribe scopes configured;
- [ ] broker credentials not shared globally;
- [ ] secrets from secret store;
- [ ] route permissions tested negatively;
- [ ] logs reviewed for secret leakage;
- [ ] message/payload size limits set;
- [ ] DLQ operator permissions restricted;
- [ ] encrypted payload mode uses KMS/real key wrapping if required;
- [ ] rotation/recovery runbook tested;
- [ ] dependency/security scanning in CI.
