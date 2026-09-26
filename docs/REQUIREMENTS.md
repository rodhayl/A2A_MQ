# Requirements and traceability — v0.1.0 pre-release

Requirement IDs are stable. Historical phase sequencing is archived; current evidence
is summarized in [IMPLEMENTATION_COVERAGE.md](IMPLEMENTATION_COVERAGE.md) and
[VALIDATION.md](VALIDATION.md).

| ID | Requirement | Current status |
| --- | --- | --- |
| FR-001 | A2A semantic payload preservation | implemented; SDK/full acceptance |
| FR-002 | submit while worker offline | implemented in distributed/live topology |
| FR-003 | task lifecycle | implemented; local + Workflow paths |
| FR-004 | artifact/status results | implemented in supported A2A lifecycle |
| FR-005 | get task | implemented |
| FR-006 | list tasks | implemented |
| FR-007 | cancel task | implemented; owner-coordinated |
| FR-008 | continue interrupted task | implemented in Workflow tier |
| FR-009 | correlation identity | implemented |
| FR-010 | agent/skill targeting | implemented |
| FR-011 | worker pools | implemented in distributed topology |
| FR-012 | independent observers | implemented in distributed topology |
| FR-013 | standard A2A bridge | implemented with pinned official SDK |
| FR-014 | agent discovery/Card | implemented |
| FR-020 | at-least-once delivery | implemented |
| FR-021 | idempotency/fencing | implemented |
| FR-022 | durable command retention | implemented |
| FR-023 | durable task projection | implemented |
| FR-024 | explicit retry policy | implemented |
| FR-025 | dead-letter handling | implemented and live-tested |
| FR-026 | back-pressure/bounds | implemented |
| FR-027 | ordering guard | implemented |
| FR-030 | workload identity | implemented production boundary |
| FR-031 | topic/route authorization | implemented; negative paths live-tested |
| FR-032 | transport encryption | broker TLS + Dapr mTLS boundary |
| FR-033 | optional payload encryption | implemented with Vault-backed profile |
| FR-034 | AEAD authenticated encryption | implemented |
| FR-035 | key isolation | implemented profile boundary |
| FR-036 | key rotation windows | implemented configuration contract |
| FR-037 | metadata minimization | implemented routing/logging contract |
| FR-038 | secret management | file/secret-reference boundaries implemented |
| FR-040 | W3C trace context | implemented |
| FR-041 | metrics | implemented modules; thresholds deployment-specific |
| FR-042 | structured logs | implemented |
| FR-043 | health/readiness | implemented |
| FR-044 | replay tooling | implemented and live-tested |
| FR-050 | Dapr API boundary | implemented |
| FR-051 | second broker | RabbitMQ live 12/12; Kubernetes parity not separately executed |
| FR-052 | versioned envelope | implemented strict schema/version contract |

## Non-functional requirements

- NFR-001: documented local startup with explicit prerequisites.
- NFR-002: no custom broker/consensus/replication implementation.
- NFR-003: stateless adapters scale; SQLite remains single-host.
- NFR-004: no orchestrator owns the only durable accepted-work record.
- NFR-005: abandoned work is recoverable and not ACKed merely because claimed.
- NFR-006: historical dispatch latency targets remain unmeasured; no SLA claim.
- NFR-007: bounded inline payloads; large artifacts belong in authorized external storage.
- NFR-008: bounded message/depth/concurrency/page sizes and retention/quota.
- NFR-009: official SDK compatibility required before interoperability claims.
- NFR-010: insecure local examples are not production security evidence.
- NFR-011: mutations carry immutable identity, authority and version.
- NFR-012: machine-testable schema and transition contracts.
- NFR-013: local economics remains a product gate; distributed topology is not a
  substitute for a viable lightweight local runtime.

## Non-requirements

No global ordering, arbitrary external exactly-once effects, topic-per-task,
proprietary discovery, new LLM framework, distributed KMS implementation or broker
federation is required for v0.1.0.
