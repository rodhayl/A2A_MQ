# Requirements and traceability

IDs are stable. Phase assignment is scope, not evidence of a passed release gate.
See VALIDATION.md for executed results and TEST_PLAN.md for preserved scenarios.

| ID | Requirement | Phase | Acceptance / evidence |
|---|---|---|---|
| FR-001 | A2A semantic payload preservation | 1/5 | Codec tests; upstream bridge conformance separately |
| FR-002 | Submit while worker offline | 2 | Real retained-stream test |
| FR-003 | Task lifecycle | 1/4 | All state pairs; workflow transitions where enabled |
| FR-004 | Artifact results and deltas | 1/3/4 | Task artifacts now; streaming assembly separately |
| FR-005 | Get task | 3/5 | Durable tenant-scoped query |
| FR-006 | List tasks | 3/5 | Bounded keyset queries |
| FR-007 | Cancel task | 4 | Owner-coordinated cancel and race test |
| FR-008 | Continue interrupted task | 4 | Durable input/auth continuation |
| FR-009 | Correlation | 1 | ID preservation and mismatch tests |
| FR-010 | Agent/skill targeting | 1/2 | Exactly one canonical target |
| FR-011 | Worker pools | 2/3 | Real competition plus shared-store race |
| FR-012 | Independent observers | 2 | Distinct durable/group live fan-out |
| FR-013 | Standard A2A bridge | 5 | Upstream client interoperability |
| FR-014 | Agent discovery | 5 | Agent Card and capabilities |
| FR-020 | At-least-once delivery | 2/3 | Duplicate and redelivery tests |
| FR-021 | Idempotency | 3 | Namespaced immutable hash, fenced completion |
| FR-022 | Durable command retention | 2 | Real restart/retention test |
| FR-023 | Durable task projection | 3 | Restart and transactional outcome tests |
| FR-024 | Explicit retry policy | 2 | HTTP classification and live retries |
| FR-025 | Dead-letter handling | 2/8 | Confirmed handoff and replay |
| FR-026 | Back-pressure | 2 | Bounded concurrency and callback body |
| FR-027 | Ordering guard | 3 | Snapshot permutations and conflicts |
| FR-030 | Workload identity | 6 | Verified principal mapping |
| FR-031 | Topic authorization | 6 | Dapr/broker negative tests |
| FR-032 | Transport encryption | 6 | Broker TLS separately from sidecar mTLS |
| FR-033 | Optional payload encryption | 7 | Crypto gate |
| FR-034 | AEAD encryption | 7 | AES-GCM tamper tests |
| FR-035 | Key isolation | 7 | Per-message key and authorized receiver group |
| FR-036 | Key rotation | 7 | Read windows and retired keys |
| FR-037 | Metadata minimization | 1/6 | No task text in routes/logs |
| FR-038 | Secret management | 6 | No committed credentials |
| FR-040 | Trace context | 1/2/8 | Preserve valid W3C context |
| FR-041 | Metrics | 8 | Backlog/latency/duplicate/DLQ instruments |
| FR-042 | Structured logs | 2 | Identifier-only JSON logs |
| FR-043 | Health/readiness | 2/3 | Process versus store/outbound API |
| FR-044 | Replay tooling | 8 | Explicit identity policy and authorization |
| FR-050 | Dapr API boundary | 2 | Distributed/local-MQ transport uses Dapr rather than direct broker code in domain |
| FR-051 | Second broker | 9 | Same distributed tests with RabbitMQ |
| FR-052 | Versioned envelope | 0/1 | Strict schema and explicit extensions |

## Non-functional requirements

- NFR-001: one documented local startup after prerequisites; live smoke required.
- NFR-002: no custom broker, consensus or replication implementation.
- NFR-003: stateless adapters scale; SQLite is single-host only.
- NFR-004: no orchestrator process owns the only durable accepted-work record.
- NFR-005: abandoned work is recoverable, not ACKed merely because it was claimed.
- NFR-006: 50 ms median / 200 ms p95 dispatch remain unmeasured targets.
- NFR-007: bounded inline payloads; large artifacts use authorized external storage.
- NFR-008: bounded message/depth/concurrency/page sizes and retention/quota.
- NFR-009: upstream SDK/client compatibility tests before conformance claims.
- NFR-010: insecure local examples labeled; production security tested independently.
- NFR-011: every mutation has immutable event identity, authority and version.
- NFR-012: machine-testable schema and exhaustive transition matrix.
- **NFR-013: local economics is a release gate.** The default local bridge must have
  a measured lightweight profile. PostgreSQL, Redis, Docker/Kubernetes, Vault and
  external telemetry are not mandatory local prerequisites. Measure core footprint
  before broad functional testing, then measure every added daemon incrementally.
  A heavy acceptance topology cannot substitute for a viable local runtime.

## Deployment semantics

The complete role-separated stack validates distributed behavior; it is not the
default local cost target. Local tiers may expose fewer optional features, but must
advertise those limits explicitly and must not fake Workflow, broker parity, HA or
production security.

The preferred single-host persistence is SQLite. PostgreSQL remains the shared-store
gate for distributed replicas. A future local Workflow tier should prefer a
supported local Dapr state component and measure Placement/Scheduler overhead before
becoming default.

## Non-requirements

NR-001 global ordering; NR-002 arbitrary external exactly-once effects; NR-003 topic
per task; NR-004 proprietary discovery; NR-005 new LLM framework; NR-006 distributed
KMS; NR-007 broker federation in MVP; NR-008 standardization before useful tests.

## Release gates

Correctness and local economics are independent. A local release needs both a
passing lightweight correctness selection and a passing measured resource budget.
Distributed/production releases additionally require their real PostgreSQL,
Dapr/Workflow, broker, security and interoperability gates. Missing infrastructure
never becomes an implicit pass.
