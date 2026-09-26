# Security model — current v0.1.0 pre-release

## Trust boundaries

Treat publisher identity, tenant routing, broker access, worker execution, task
queries, SQL state, DLQ data and key material as separate boundaries.

Envelope/CloudEvent source fields are claims. Production authorization is derived
from verified principals/workload configuration, not payload fields.

Dapr mTLS does not by itself secure Dapr-to-broker traffic. Production profiles
require broker TLS/credentials, Dapr component scopes/tokens and network policy.

## Controls

| Threat | Current control / evidence |
| --- | --- |
| Duplicate/replayed command | namespaced immutable identity/hash + inbox/outbox semantics; exercised by local/live acceptance |
| Stale executor | lease fencing at SQL commit and takeover behavior |
| Conflicting projection | owner/context/version monotonicity checks |
| Malformed/large input | bounded strict JSON/schema validation |
| Secret/prompt leakage | identifier-oriented logging and secret-file boundaries |
| Forged owner/tenant | verified principal mapping; Kubernetes gate passed foreign-owner/cross-tenant denial |
| Broker/operator payload exposure | optional Vault-backed payload protection; real TLS Vault gate passed |
| Credential misuse | least privilege, Dapr scopes/tokens, role-specific secrets and deployment isolation |
| Compromised authorized endpoint | residual risk; rotation/isolation/operator responsibility |

## Local versus distributed

Local examples bind loopback and are not production trust boundaries. SQLite is
single-host. Production manifests use digest-pinned images, non-root/read-only
containers, dropped capabilities, Dapr mTLS and secret references.

## Payload encryption contract

Optional transport protection uses vetted authenticated encryption and Vault Transit
key wrapping with routing metadata authenticated as AAD. Queued/replayed ciphertext
requires compatible read-key windows during rotation.

Transport protection does not imply encryption of SQL/Workflow history; storage and
backup encryption remain infrastructure responsibilities.

## Evidence boundary

The JetStream Kubernetes gate verified authenticated A2A/MCP access, negative
owner/tenant paths and workload security configuration. This is acceptance evidence,
not a signed security certification. `productionCertified` remains false.
