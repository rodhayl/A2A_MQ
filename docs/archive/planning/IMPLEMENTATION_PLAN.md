# Implementation plan and gates

Phase numbering is preserved from the initial plan. A code milestone and an
integration exit gate are distinct; see `VALIDATION.md` for executed evidence.
Commits must be made after each phase, without hiding unrun gates as passed.

## Phase 0 - baseline and corrected architecture

Package, exact direct dependency pins, CI, schema fixtures, documentation checks,
role-specific deployment plan and ADRs. Pin the Dapr runtime separately from Python
SDK dependencies. Clean local install may reuse preinstalled dependencies offline;
a clean network-resolved installation is a different experiment.

## Phase 1 - pure domain

Immutable envelope codec, strict version/route/JSON rules, A2A 1.0 structural
validation, task transition/reachability functions, typed error taxonomy, expiry,
correlation and trace metadata. Exhaust all state pairs and fuzz malformed inputs.
Do not accidentally accept encryption, a legacy 0.3 payload or an ambiguous target.

## Phase 2 - transport

Dapr HTTP publisher, validated CloudEvent callback, explicit RETRY/permanent failure
policy, confirmed DLQ handoff, bounded handler concurrency, metadata-only logging,
trace propagation, health checks and model-free examples. Create a limits-retention
stream before publishing. Generate a separate component configuration per role and
filter; worker replicas share identities, independent observers do not.

Exit tests requiring real infrastructure: publish with no workers, start two workers,
verify competing behavior, start two independent observers, verify both see results,
and exercise broker retry/DLQ behavior. Mocks validate the HTTP contract only.

## Phase 3 - idempotency and projections

SQLite reference plus PostgreSQL transactional adapter. Namespace every key by
tenant and logical consumer. Persist immutable command hashes, reclaimable leases,
fencing tokens, reference effect, full Task outcome and transactional outbox.
Outbox publication may duplicate; the projection must converge under reordering.
Add internal get/list API with bounded keyset pagination and context/state filters.

Crash tests cover durable claim before execution, rollback after effect insertion,
commit before ACK, and publish before outbox marking. Test duplicate ID with changed
content, stale fencing tokens, cross-tenant IDs, same-version conflicts and terminal
rewrites. External effects require their own idempotency; no generic exactly-once claim.
Real PostgreSQL tests and multi-process broker tests remain separate exit gates.

## Phases 4-10 - retained roadmap

4. Dapr Workflow/Agents integration: durable long tasks, input/auth pauses, safe
   cancellation, continuation, fan-out/fan-in; choose a supported workflow state store.
5. Standard A2A bridge: SDK-backed send/get/list/cancel/subscribe, Agent Card and
   explicit capability/error mapping. Internal query endpoints are not this bridge.
6. Production security: workload identity, broker TLS/auth, Dapr scopes, secret
   provider, cross-tenant negative tests and authenticated query authorization.
7. Optional encrypted envelope: vetted CEK wrapping, canonical AAD, KMS provider,
   fail-closed behavior and rotation tests; no handwritten cryptography.
8. Operations: retention/cleanup, quotas, DLQ inspection/replay, telemetry, HA,
   backup/restore, graceful draining and measured performance.
9. RabbitMQ portability: same application tests, verified backend-specific consumer,
   retry, DLQ and ordering configuration. Portability is not proven by using Dapr.
10. Formal binding decision: mandatory operations, auth/discovery/error mapping,
    independent interoperability and current upstream custom-binding review.

## Release decision

Phases 0-3 are an experimental vertical slice, not the complete MVP. Do not expose
it to untrusted tenants or describe planned features as implemented. Full MVP still
requires the original bridge/workflow/security and live reliability gates in the
requirements matrix. A failed or unrun gate stays visible in validation records.
