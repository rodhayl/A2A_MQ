# Security model and implementation boundary

## Trust boundaries

Protect publishers, tenant routing, broker access, worker execution, task queries,
SQL state, DLQ data and key material separately. An envelope's `source`, tenant,
role, Agent Card or CloudEvent source is a claim, not authenticated identity. Bind
those claims to workload credentials at authenticated ingress before production.
The development echo worker trusts its configured local environment only.

Dapr-to-Dapr mTLS does not automatically encrypt/authenticate Dapr-to-NATS traffic.
Use broker TLS and per-workload accounts/subject permissions, network isolation,
Dapr scopes and API/app tokens. Topic filters in application code are defense in
depth, not a hard isolation boundary. A query tenant parameter is not authorization:
production query access must derive permitted tenants from a verified principal.

## Threats and controls

| Threat | Control | Initial status |
|---|---|---|
| Duplicate/replayed command | Tenant/consumer inbox, immutable hash, expiry | Phases 1/3 |
| Stale executor | Lease fencing at SQL commit | Phase 3 |
| Conflicting snapshot | Owner/context/version checks | Phase 3 |
| Malformed/large JSON | Byte/depth limits, strict schema, no executable deserialization | Phases 1/2 |
| Leaked prompts in logs | Identifier-only logs; no exception/payload dumps | Phase 2 |
| Forged source/tenant | Authenticated workload-to-policy mapping | Production phase 6 gate |
| Broker/operator reads content | Optional payload encryption | Phase 7, unsupported until then |
| Authorized endpoint compromise | Least privilege, isolation, key rotation | Residual risk |

No production manifest is claimed secure before negative authorization tests. Local
ports must bind loopback; container-internal listeners may bind all interfaces only
on the isolated compose network. Never put real credentials in local example traffic.

## Future envelope encryption

Use a vetted library: fresh 256-bit CEK per message, AES-256-GCM, fresh 96-bit nonce,
and a vetted KMS/Vault wrapping operation per authorized worker group. Do not invent
a cipher, deterministic nonce scheme or a global shared payload key. Freeze the
canonical AAD serialization with test vectors before implementation. Authenticate
version, tenant, message/operation, source/target, correlation/causation, task/context,
sequence, creation/expiry and content metadata. Do not authenticate mutable hop data.

Group AEAD proves possession of group key material, not an individual sender. Sender
authentication requires a transport identity binding or signatures. A broker cannot
hide route, timing and approximate sizes; ciphertext does not stop traffic analysis.
Encryption also does not prevent replay. Retain dedup and old decrypt keys through
the maximum supported replay window. Loss of an old key can make retained data
unrecoverable; revocation may intentionally trade availability for containment.

Key-policy failure is permanent; temporary KMS outage is retryable. Neither may
fall back to plaintext. Protect task DBs, backups, artifact URLs and DLQs too:
transport encryption does not encrypt those automatically. Authorized plaintext
endpoints and their model/tool providers remain inside the confidentiality boundary.

## Safe execution and release gate

The reference effect is local SQL. Future tool execution must not treat model output
as authority, expose arbitrary shell execution, or pass credentials in task text.
`AUTH_REQUIRED` uses scoped delegated references, not shared passwords. Do not
fetch arbitrary artifact URLs in the codec. SSRF and artifact authorization are
bridge/runtime controls when URL fetching is introduced.

## Production gate: broker TLS/auth, isolated subjects/accounts, token checks, query
principal mapping, secret store, restricted DLQ access, negative cross-tenant tests,
log audit, backup protection and bounded resource/retention settings. Optional
crypto additionally requires tamper, wrong-recipient, replay and rotation tests.
