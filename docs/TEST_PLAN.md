# Test Plan

## 1. Test layers

### Unit

- envelope validation/versioning;
- route normalization;
- task transition matrix;
- A2A serialization round trips;
- expiry;
- error mapping;
- AAD construction;
- crypto tamper detection;
- dedup state machine.

### Integration

Run with real Dapr + JetStream and a real state store where applicable.

- offline worker receives retained command;
- competing consumers;
- fan-out observers;
- broker reconnect;
- projection persistence;
- DLQ;
- trace propagation;
- ACL denial.

### Conformance

- upstream A2A client against bridge;
- canonical A2A task lifecycle;
- send/get/list/cancel/subscribe behaviors;
- interrupted task continuation;
- artifact chunk semantics;
- error mappings;
- Agent Card compatibility.

### Chaos/failure

Inject process kill and dependency faults at deterministic checkpoints.

## 2. Required scenarios

| ID | Scenario | Expected result |
|---|---|---|
| T-001 | Publish while all workers stopped | Command retained and later processed |
| T-002 | Two workers, one command | One logical execution |
| T-003 | Crash before command ack | Redelivery occurs |
| T-004 | Crash after durable side effect marker, before ack | Redelivery does not repeat reference side effect |
| T-005 | Duplicate event | Projection unchanged after first application |
| T-006 | Stale lower sequence/version event | Projection ignores it |
| T-007 | `INPUT_REQUIRED`, restart, continue | Workflow resumes and completes |
| T-008 | `AUTH_REQUIRED`, restart, continue | Workflow resumes without plaintext credential logging |
| T-009 | Cancel while working | Safe cancellation and terminal state |
| T-010 | Cancellation races completion | One deterministic terminal state; no terminal rewrite |
| T-011 | Poison schema-valid command repeatedly fails | Bounded retries then DLQ |
| T-012 | Malformed envelope | Rejected without agent execution |
| T-013 | Expired command | Not executed |
| T-014 | Unauthorized publisher | Publish denied/fails closed |
| T-015 | Unauthorized subscriber | Cannot consume route |
| T-016 | Encrypted payload captured at broker | No A2A plaintext content visible |
| T-017 | Ciphertext tampered | Authentication/decryption fails |
| T-018 | Old key during rotation window | Decrypts if policy allows |
| T-019 | Retired key after window | Fails predictably, audited |
| T-020 | Broker unavailable during publish | Caller receives transient failure, no false success |
| T-021 | Projection unavailable during state update | Unsafe ack does not occur |
| T-022 | Two orchestrators subscribe to same events | Both receive independent copy |
| T-023 | Large artifact over configured inline limit | Rejected or externalized according to policy |
| T-024 | Standard A2A client submits task | Canonical A2A result returned |
| T-025 | Bridge restarts during long task | Task survives; subsequent query/subscription recovers |

## 3. State transition tests

Tests MUST enumerate all state pairs, not only happy paths. For each source/target pair assert either allowed or rejected. Terminal states must reject later mutation.

## 4. Idempotency reference fixture

Create a fake irreversible side effect represented by an append-only database record keyed by logical operation ID. Test fault points:

1. before dedup claim;
2. after claim;
3. before side effect;
4. after side effect but before outcome persistence;
5. after outcome persistence but before broker ack.

The reference implementation must document which points are protected automatically and which require the application to use an idempotency key/transactional outbox. Never hide the classic side-effect/ack gap with an incorrect exactly-once claim.

## 5. Performance harness

Measure transport/runtime overhead separately from LLM inference.

Baseline dimensions:

- 1, 10, 100 concurrent workers;
- 1, 10, 50 observers;
- payload 1 KB, 32 KB, 256 KB;
- encrypted vs plaintext;
- durable workflow vs short task.

Record:

- publish-to-delivery latency p50/p95/p99;
- event latency;
- throughput;
- redelivery rate;
- CPU/memory;
- projection write latency;
- encryption overhead.

Performance numbers are environment-specific and must include hardware/configuration when published.

## 6. Compatibility matrix

Before v1.0 maintain CI/integration results for:

- pinned A2A SDK version;
- supported Python versions;
- pinned Dapr runtime/SDK versions;
- reference JetStream version;
- second broker backend version.

Upgrades are pull requests with the same conformance suite, not silent dependency drift.

## 7. Security tests

- topic scope negative tests;
- broker credential privilege tests;
- no secrets in logs snapshot test;
- ciphertext known-answer/round-trip tests;
- nonce uniqueness test strategy;
- modified AAD fails authentication;
- wrong target key fails decryption;
- malformed oversized payload rejected before expensive work;
- URL artifact policy blocks unsafe schemes/internal targets as configured.

## 8. Exit criteria

MVP: all mandatory requirements mapped to tests pass on JetStream reference stack.

v1.0: conformance + security + second-broker matrix passes, no open severity-1 correctness/security defects, and failure tests demonstrate documented recovery behavior.
