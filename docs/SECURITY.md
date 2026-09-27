# Security model — v0.1.0-rc2

## Trust boundaries

Treat these as separate boundaries:

- client/publisher identity;
- tenant/task ownership;
- route authorization;
- broker access;
- worker execution;
- task/DLQ query access;
- application SQL state;
- Dapr Workflow state;
- key/secret material.

Envelope and CloudEvent source fields are protocol claims. Production authorization is
derived from verified principals/workload configuration, not payload text.

Dapr mTLS does not by itself secure the Dapr-to-broker connection. Production
deployments require broker TLS/credentials, Dapr component scopes/tokens and network
policy.

## Core controls

| Threat | Current control / evidence |
| --- | --- |
| Duplicate/replayed command | immutable namespaced identity/hash + task/inbox/outbox idempotency |
| Stale executor | lease fencing at commit/takeover boundaries |
| Conflicting task projection | owner/context/sequence/terminal monotonicity checks |
| Malformed/large input | bounded strict JSON/schema validation |
| Foreign task access | verified principal + tenant/scope + task subject/owner checks |
| Route confusion | derived routes + component/topic/envelope consistency checks |
| Credential misuse | loopback local bindings; Dapr/app tokens; scoped production secrets |
| DLQ tampering | archive digest/integrity verification + authorized replay |
| Payload exposure | optional Vault-backed transport payload protection |
| Logging leakage | identifier/reason-oriented logging contract; no credentials/private keys |
| Authorized endpoint compromise | residual risk; deployment isolation/rotation/operator responsibility |

## Local profiles

Local-lite and local Workflow bind their application, Dapr and broker/control-plane
interfaces to loopback and generate owner-private profile files.

Generated profile directories contain secrets, including local signing material and
tokens. They must not be committed or made group/world readable.

Queue Lab:

- has no server-side JWT bypass;
- keeps the bearer token in browser `sessionStorage`;
- serves local static assets with restrictive CSP;
- does not expose private signing keys, Dapr/NATS tokens or raw SQLite access;
- remains subject to ordinary A2A/operations authorization.

RC2 explicitly validated an invalid bearer token returning 401 and successful
reconnect with the current generated token.

## Distributed / production

Production rendering uses:

- digest-pinned application images;
- non-root containers;
- read-only root filesystem;
- dropped Linux capabilities;
- Dapr token/secret references;
- public-key-only JWT config in the API pod;
- administrator-managed PostgreSQL/broker/Dapr/network/TLS infrastructure.

The renderer intentionally does not generate production credentials or a trust root.

## Payload encryption

Optional transport protection uses authenticated encryption with Vault Transit-backed
key wrapping and routing metadata as authenticated context.

Payload protection does **not** imply encryption of SQL, Workflow history, backups or
broker metadata. Those remain deployment responsibilities.

Queued/replayed ciphertext also requires a compatible key-retention/rotation window.

## Validation boundary

RC2 focused validation covers the local user-facing auth/owner/DLQ/session paths.

Earlier JetStream Kubernetes evidence covered authenticated A2A/MCP access, negative
owner/tenant paths and workload security configuration; earlier full acceptance
includes real TLS Vault Transit.

These are engineering acceptance results, not signed security certification.

- `productionCertified=false`
- `upstreamBindingCertified=false`
