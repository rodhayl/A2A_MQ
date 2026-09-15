# Implementation Plan

The plan is intentionally incremental. Each phase should leave a runnable, tested system and avoid speculative infrastructure.

## Phase 0 — Repository baseline

Deliverables:

- packaging (`pyproject.toml`);
- formatting/lint/type-check/test tooling;
- CI workflow;
- pinned A2A and Dapr dependencies;
- local developer stack;
- protocol JSON schema;
- architecture decision records.

Exit gate:

- clean install in a fresh environment;
- schema validation tests run in CI;
- local broker + Dapr smoke test documented.

## Phase 1 — Envelope and pure domain core

Implement code with no network dependency:

- `Envelope` model;
- schema/version validation;
- route normalization;
- A2A payload codec;
- correlation/causation metadata;
- state-transition validator;
- error taxonomy;
- expiry checks.

Tests:

- exhaustive task transition table;
- property/fuzz tests for malformed envelopes;
- round-trip A2A representative payloads;
- route normalization collision/unsafe-character tests.

Exit gate: FR-001, FR-003, FR-009, FR-052 core behavior passes unit tests.

## Phase 2 — Dapr Pub/Sub transport

Implement:

- publisher adapter;
- subscriber/handler adapter;
- Dapr component configuration for JetStream;
- command and event topics;
- stable consumer identities;
- explicit retry/failure classification;
- structured logging and trace propagation.

Build two tiny examples:

- orchestrator publishes a command;
- worker consumes and returns task status/result.

Exit gate:

- worker can be offline at publish time and later receive the command;
- two workers compete correctly;
- two observers independently receive the same event.

## Phase 3 — Idempotency and task projection

Implement:

- dedup repository interface;
- task projection repository;
- atomic claim/outcome patterns appropriate to selected store;
- monotonic state version guard;
- duplicate event suppression;
- query API for `GetTask` and `ListTasks`.

Start with SQLite for local deterministic tests. Add a production state-store implementation using a Dapr-supported backend chosen from project needs.

Critical test: crash after durable claim but before broker ack, then redeliver to another replica; business side effect must not execute twice in the reference fixture.

Exit gate: FR-005/006/020/021/023/027 pass integration tests.

## Phase 4 — Durable workflows

Integrate Dapr Workflow/Dapr Agents for:

- long-running task execution;
- interrupted states;
- continuation messages;
- durable timers/retries;
- cancellation;
- parent/child fan-out and fan-in example.

Exit gate:

- kill/restart worker during workflow;
- `INPUT_REQUIRED` survives restart and resumes after continuation;
- cancellation race behavior is deterministic.

## Phase 5 — Standard A2A bridge

Implement the pinned upstream A2A server interface, initially HTTP+JSON unless SDK maturity clearly favors another binding.

Operations:

- send message;
- get task;
- list tasks;
- cancel task;
- task subscription/streaming where supported by upstream SDK;
- Agent Card;
- extended Agent Card if required by chosen conformance target;
- explicit standards-compliant handling for push notification configuration.

Bridge behavior:

- `returnImmediately=true`: return accepted task/message as upstream semantics require;
- blocking send: wait on correlated task state until terminal/interrupted condition;
- stream: translate task/artifact events to upstream stream objects.

Exit gate: official/upstream SDK client can interact with an MQ-backed sample agent without knowing about MQ.

## Phase 6 — Security baseline

Implement/configure:

- Dapr pub/sub scopes;
- separate workload identities;
- broker TLS/auth configuration for integration environment;
- secret-store integration;
- negative authorization tests;
- payload/log size bounds and sanitization.

Exit gate: unauthorized publish and subscribe tests fail closed.

## Phase 7 — Optional payload encryption

Implement behind feature flag:

- crypto provider interface;
- AES-256-GCM content encryption;
- canonical AAD profile;
- development wrapping provider;
- KMS/Vault wrapping adapter (choose one concrete reference backend first);
- key IDs/versions;
- rotation tests;
- decryption failure/DLQ policy.

Never build a custom cryptographic primitive.

Exit gate:

- broker payload capture contains no A2A plaintext content;
- tampering fails authentication;
- wrong workload/key cannot decrypt;
- rotation window test passes.

## Phase 8 — Reliability and operations

Add:

- DLQ helper/replay CLI;
- metrics;
- dashboards/runbook examples;
- chaos tests;
- retention sizing documentation;
- graceful shutdown/drain behavior;
- broker/state-store backup guidance;
- benchmark harness.

Exit gate: test matrix in `TEST_PLAN.md` passes on CI/integration infrastructure.

## Phase 9 — Portability proof

Add RabbitMQ as second Dapr Pub/Sub backend.

Rules:

- no domain-code branch based on broker type;
- backend-specific differences stay in component/deployment configuration or narrowly isolated adapter configuration;
- same conformance tests run against both backends.

Exit gate: FR-051 passes.

## Phase 10 — Custom A2A binding decision

Only after the system works:

1. compare implementation against current A2A custom-protocol-binding requirements;
2. close operation/auth/discovery gaps;
3. publish normative serialization/routing rules;
4. build an independent conformance client or second implementation;
5. test Agent Interface advertisement;
6. decide whether to propose/register the binding upstream.

Do not block the useful product on standardization.

## Suggested first implementation backlog

Order matters:

1. repo tooling and dependency pinning;
2. envelope schema + Pydantic/dataclass model;
3. A2A state transition tests;
4. Dapr/JetStream local stack;
5. publisher + worker subscriber;
6. event subscriber;
7. dedup + task projection;
8. crash/redelivery integration tests;
9. durable interrupted workflow;
10. standard A2A bridge;
11. ACL security;
12. payload encryption;
13. DLQ/replay/metrics;
14. RabbitMQ portability.

## Suggested Python interfaces

```python
class MessageBus(Protocol):
    async def publish(self, route: str, envelope: Envelope) -> None: ...

class DedupStore(Protocol):
    async def claim(self, message_id: str) -> ClaimResult: ...
    async def complete(self, message_id: str, outcome_ref: str | None) -> None: ...

class TaskProjection(Protocol):
    async def apply(self, event: TaskEvent) -> ApplyResult: ...
    async def get(self, task_id: str) -> Task | None: ...
    async def list(self, query: TaskQuery) -> Page[Task]: ...

class PayloadCrypto(Protocol):
    async def encrypt(self, payload: bytes, aad: bytes, target: KeyTarget) -> EncryptedPayload: ...
    async def decrypt(self, encrypted: EncryptedPayload, aad: bytes) -> bytes: ...
```

Interfaces should stay this small until production use cases prove more abstraction is needed.

## Definition of done for MVP

MVP is done when a demo can prove, automatically:

- standard A2A client -> bridge -> MQ -> offline/online worker -> canonical result;
- worker crash/redelivery without duplicate reference side effect;
- `INPUT_REQUIRED` durable pause/resume;
- cancellation;
- multiple worker replicas;
- multiple observing orchestrators;
- task queries after restarts;
- retries + DLQ;
- auth scopes;
- traces/metrics;
- one-command local developer startup.
