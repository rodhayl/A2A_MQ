# Requirement coverage after lightweight-local implementation

Implementation presence, local evidence and distributed/native infrastructure
acceptance are separate. Historical test counts do not certify the current SHA.

| Scope | Implemented source | Evidence still required |
| --- | --- | --- |
| Local economics / NFR-013 | `scripts/local_resource_probe.py`, `scripts/local_stack_probe.py`, ADR 0011 | Execute both probes on the target low-resource machine |
| Local-lite A2A bridge | `local_lite.py`, official `bridge.py`, JWT identity, SQLite TaskLedger/TaskAPI | Native A2A client + real daprd/JetStream smoke on target |
| Local-lite transport | direct native launcher, one Dapr sidecar, bounded single-node JetStream, separate command/DLQ durables | Restart/redelivery and resource evidence with real native binaries |
| Local-lite state | SQLite jobs/tasks/actions/outbox/runtime-end, retention identities | Target-machine restart and backup/restore smoke |
| Local-lite execution | immediate deterministic echo/upper executor using existing durable actions | Race/restart/cancel regression plus real transport execution |
| Local-lite cancellation | existing owner-scoped TaskAPI/TaskLedger controls | Real A2A cancel race through sidecar |
| Local-lite DLQ/replay | existing bounded archive/replay, dedicated Dapr DLQ component | Poison delivery + replay through real JetStream |
| Local-lite MCP | optional official MCP SDK; submit/get/list/cancel only | Install `.[local,mcp]` and run native MCP HTTP client gate |
| Local Workflow | `local_workflow.py`, `local_workflow_runtime.py`, official Dapr Workflow, SQLite actor state, native Placement/Scheduler | Five-process resource gate + real wait/timer/approval/restart smoke on target |
| FR-001/009/010/037/040/050/052 | domain/schema/transport, stable routing/identity, A2A bridge | Whole-repository/current-SHA regression |
| FR-002/011/012/020/022/024/026 distributed semantics | role-separated admissions, brokers, workers/observers and dual-broker harness | Real JetStream/RabbitMQ distributed runs; local-lite does not claim independent offline worker pools/observer fan-out |
| FR-021/023/027 | SQLiteStore, PostgresStore, leases/fences/outbox/projection ordering | Current-SHA SQLite regression and real PostgreSQL gate |
| FR-003/007/008 Workflow tier | Dapr Workflow registration/actions/reconcile, input/auth/cancel/timers/fan-out | Native Workflow restart/race gate; intentionally outside local-lite |
| FR-004/005/006/013/014 | TaskAPI + official A2A SDK Agent Card/send/get/list/cancel/subscribe | Official SDK client/native streaming gate |
| FR-030/031/032/038 | JWT principal/owner scopes, Dapr scopes, TLS production profiles | Deployed workload/broker policy and cross-tenant negative tests |
| FR-033/034/035/036 | AEAD/key wrap/Vault Transit transport protection | Real Vault/encrypted deployment gate; not a local-lite prerequisite |
| FR-025/044 | bounded DLQ archive/operator/replay | Real broker poison/replay evidence |
| FR-041/042/043 | metrics/logging/readiness in distributed profile; lightweight health in local-lite | Operational tuning and native resource evidence |
| NFR-008 | bounded queues/concurrency/records/retirement | Capacity sizing and retention windows |
| Recovery | SQLite backup/restore, PostgreSQL wrappers, Dapr history purge guard | Real DR gates per deployment tier |
| FR-051 | JetStream/RabbitMQ distributed portability | Both native suites must pass; local-lite defaults to JetStream |
| MCP distributed | official MCP SDK and six Workflow-capable tools | Native MCP gate |
| Protocol binding | ADR 0010 / binding manifest | Independent conformance review |

## Local-lite feature boundary

Local-lite deliberately collapses API and immediate execution into one Python
process. This reduces process/memory cost and keeps SQLite as the only application
database. It supports durable persistence and restart/redelivery but does not claim
that a separate worker can remain offline while the API independently accepts new
work; that is a distributed-profile property.

`inputRequired`, `authRequired`, delays/timers, continuation/approval and
fan-out/fan-in are rejected with `WORKFLOW_FEATURE_UNAVAILABLE` in local-lite.
They remain implemented through official Dapr Workflow. The optional local Workflow
tier now exposes that implementation without PostgreSQL/Redis/Docker; its own native
resource and restart gates remain pending.

## Infrastructure boundary

The full Compose/Kubernetes topology remains useful for distributed acceptance and
production-like testing. PostgreSQL, Redis, Docker/Kubernetes, Vault, OTLP,
Placement and Scheduler are not default local requirements.

Generated configuration, mocks, source inspection or unavailable prerequisites do
not count as native acceptance. Resource viability must be measured on the actual
target hardware before the local product is declared viable.
