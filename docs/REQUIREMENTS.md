# Requirements and traceability — v0.1.0-rc2

Requirement IDs are stable. Current implementation coverage is summarized in
[IMPLEMENTATION_COVERAGE.md](IMPLEMENTATION_COVERAGE.md) and exact evidence in
[VALIDATION.md](VALIDATION.md).

| ID | Requirement | Current status |
| --- | --- | --- |
| FR-001 | A2A semantic payload preservation | implemented |
| FR-002 | submit while worker/executor unavailable | implemented; Local-lite pause demonstrates queued submitted work, distributed topology covers independent workers |
| FR-003 | task lifecycle | implemented in Local-lite and Workflow |
| FR-004 | artifact/status results | implemented |
| FR-005 | get task | implemented |
| FR-006 | list tasks | implemented |
| FR-007 | cancel task | implemented; owner-coordinated |
| FR-008 | continue interrupted task | implemented in Workflow tier |
| FR-009 | correlation identity | implemented |
| FR-010 | agent/skill targeting | implemented; Local-lite uses configured agent route |
| FR-011 | worker pools | implemented in distributed topology |
| FR-012 | independent observers | implemented in distributed topology |
| FR-013 | standard A2A bridge | implemented with pinned official SDK |
| FR-014 | agent discovery/Card | implemented |
| FR-020 | at-least-once delivery | implemented |
| FR-021 | idempotency/fencing | implemented |
| FR-022 | durable command retention | implemented |
| FR-023 | durable task projection | implemented |
| FR-024 | explicit retry policy | implemented |
| FR-025 | dead-letter handling | implemented; RC2 local DLQ/replay exercised |
| FR-026 | back-pressure/bounds | implemented |
| FR-027 | ordering guard | implemented |
| FR-030 | workload identity | implemented production boundary |
| FR-031 | topic/route authorization | implemented; negative paths have live evidence |
| FR-032 | transport encryption | broker TLS + Dapr mTLS deployment boundary |
| FR-033 | optional payload encryption | implemented Vault-backed profile |
| FR-034 | AEAD authenticated encryption | implemented |
| FR-035 | key isolation | implemented profile boundary |
| FR-036 | key rotation windows | implemented configuration contract |
| FR-037 | metadata minimization | implemented routing/logging contract |
| FR-038 | secret management | file/secret-reference boundaries implemented |
| FR-040 | W3C trace context | implemented |
| FR-041 | metrics | implemented; thresholds deployment-specific |
| FR-042 | structured logs | implemented |
| FR-043 | health/readiness | implemented; RC2 local live/ready passed |
| FR-044 | replay tooling | implemented; RC2 local Inspect/Replay passed |
| FR-050 | Dapr API boundary | implemented |
| FR-051 | second broker | RabbitMQ live 12/12 in earlier full gate; Kubernetes parity not separately executed |
| FR-052 | versioned envelope | implemented strict schema/version contract |

## Non-functional requirements

- **NFR-001**: documented fresh-checkout startup with explicit prerequisites and
  reproducible pinned native-tool bootstrap.
- **NFR-002**: no custom broker, consensus or replication implementation.
- **NFR-003**: distributed adapters can scale separately; SQLite remains single-host.
- **NFR-004**: no orchestrator owns the only durable accepted-work record.
- **NFR-005**: abandoned work remains recoverable and is not ACKed merely because it
  was claimed.
- **NFR-006**: latency/throughput targets are not claimed without measurement.
- **NFR-007**: inline payloads are bounded; large artifacts belong in authorized
  external storage.
- **NFR-008**: message/body/depth/concurrency/page/retention/quota limits are bounded.
- **NFR-009**: official SDK compatibility is required for interoperability claims.
- **NFR-010**: loopback local examples are not production security evidence.
- **NFR-011**: mutations carry immutable identity, authority and version.
- **NFR-012**: schema and transition contracts are machine-testable.
- **NFR-013**: local footprint/economics is a product gate; the distributed topology
  is not a substitute for a viable local runtime.

## Non-requirements for v0.1.0

A2A-MQ does not require or claim:

- global ordering;
- arbitrary external exactly-once effects;
- a topic per task;
- proprietary agent discovery;
- a new LLM framework;
- a custom workflow engine;
- a distributed KMS;
- broker federation;
- a generic HA/RPO/RTO guarantee.
