# Requirement coverage after lightweight-local implementation

Implementation presence, local evidence and distributed/native infrastructure
acceptance are separate. Historical test counts do not certify the current SHA.

| Scope | Implemented source | Evidence still required |
| --- | --- | --- |
| Local economics / NFR-013 | `scripts/local_resource_probe.py`, `scripts/local_stack_probe.py`, `scripts/local_soak_gate.py`, ADR 0011 | Measured and verified (`PASS_CORE_LIGHT`, `PASS_LOCAL_PYTHON`, `PASS_LOCAL_LITE`); 23.03-minute continuous soak gate verified (`PASS_LOCAL_SOAK`) with stable post-warmup RSS (+0.10 MiB delta), zero FD/thread leak, intact SQLite integrity |
| Local-lite A2A bridge | `local_lite.py`, official `bridge.py`, JWT identity, SQLite TaskLedger/TaskAPI | Functional roundtrip and official A2A SDK gate passed (`SDK_PASS`); formal resilience gate verified (`PASS_LOCAL_LITE_RESILIENCE`, 113.45s) |
| Local-lite transport | direct native launcher, one Dapr sidecar, bounded single-node JetStream, separate command/DLQ durables | Native 3-process figures verified; real broker/runtime fault-injection verified under live JetStream and Dapr: unacked redelivery (deliveryCount>=2), publish-before-bookkeeping crash (publishAttempts>=2), execution crash in genuine WORKING state, and deterministic commit boundary races |
| Local-lite state | SQLite jobs/tasks/actions/outbox/runtime-end, retention identities | Abrupt kill WAL crash consistency and backup/restore target-machine smoke verified |
| Local-lite execution | immediate deterministic echo/upper executor using existing durable actions | Immediate execution under crash/restart/cancel regressions verified |
| Local-lite cancellation | existing owner-scoped TaskAPI/TaskLedger controls | All 9 native cancellation race scenarios verified through real sidecar with official SDK |
| Local-lite DLQ/replay | existing bounded archive/replay, dedicated Dapr DLQ component | Real poison delivery quarantine + authorized replay verified through live JetStream |
| Local-lite MCP | optional official MCP SDK; submit/get/list/cancel only | Streamable HTTP client gate verified across 10 security/lifecycle scenarios |
| Local Workflow | `local_workflow.py`, `local_workflow_runtime.py`, official Dapr Workflow, SQLite actor state, native Placement/Scheduler | Five-process native stack passed separated verdicts: resource (`PASS_LOCAL_WORKFLOW_RESOURCE`, 370.69 MiB peak RSS <= 640 MiB, 4.23% idle CPU <= 8%, 61.26 MiB state <= 64 MiB), functional (`PASS_LOCAL_WORKFLOW_FUNCTIONAL`), and resilience (`PASS_LOCAL_WORKFLOW_RESILIENCE`) |
| FR-001/009/010/037/040/050/052 | domain/schema/transport, stable routing/identity, A2A bridge | 1100-test exact-current-SHA `LOCAL_PASS` and 1126-test broker-independent suite passed at `d7211264`; resilience, workflow and soak gates passed (`PASS_LOCAL_TIER_COMPLETE`); distributed multi-node gates remain |
| FR-002/011/012/020/022/024/026 distributed semantics | role-separated admissions, brokers, workers/observers and dual-broker harness | Real JetStream/RabbitMQ distributed runs; local-lite does not claim independent offline worker pools/observer fan-out |
| FR-021/023/027 | SQLiteStore, PostgresStore, leases/fences/outbox/projection ordering | SQLite/local evidence exists; exact-final-SHA regression passed; real verified-TLS PostgreSQL gate remains external/unavailable |
| FR-003/007/008 Workflow tier | Dapr Workflow registration/actions/reconcile, input/auth/cancel/timers/fan-out | Native Workflow functional & resilience gates passed on 5-process local stack (`PASS_LOCAL_WORKFLOW`) |
| FR-004/005/006/013/014 | TaskAPI + official A2A SDK Agent Card/send/get/list/cancel/subscribe | Official SDK client/native streaming gate passed (`SDK_PASS`) |
| FR-030/031/032/038 | JWT principal/owner scopes, Dapr scopes, TLS production profiles | Deployed workload/broker policy and cross-tenant negative tests |
| FR-033/034/035/036 | AEAD/key wrap/Vault Transit transport protection | Real Vault/encrypted deployment gate; not a local-lite prerequisite |
| FR-025/044 | bounded DLQ archive/operator/replay | Real broker poison/replay evidence |
| FR-041/042/043 | metrics/logging/readiness in distributed profile; lightweight health in local-lite | Operational tuning and native resource evidence |
| NFR-008 | bounded queues/concurrency/records/retirement | Capacity sizing and retention windows |
| Recovery | SQLite backup/restore, PostgreSQL wrappers, Dapr history purge guard | Real DR gates per deployment tier |
| FR-051 | JetStream/RabbitMQ distributed portability | Both native suites must pass; local-lite defaults to JetStream |
| MCP distributed | official MCP SDK and six Workflow-capable tools | Native MCP gate |
| Protocol binding | ADR 0010 / binding manifest | Independent conformance review |
| Production Deployment Gate | `scripts/production_deployment_gate.py`, `deploy/production/generate.py` | Manifest synthesis and schema verified; live cluster access absent, returning clean `INCOMPLETE_PREREQUISITE` (`productionCertified: false`) |

## Local-lite feature boundary

Local-lite deliberately collapses API and immediate execution into one Python
process. This reduces process/memory cost and keeps SQLite as the only application
database. It supports durable persistence and restart/redelivery but does not claim
that a separate worker can remain offline while the API independently accepts new
work; that is a distributed-profile property.

`inputRequired`, `authRequired`, delays/timers, continuation/approval and
fan-out/fan-in are rejected with `WORKFLOW_FEATURE_UNAVAILABLE` in local-lite.
They remain implemented through official Dapr Workflow. The optional local Workflow
tier exposes that implementation without PostgreSQL/Redis/Docker; its native resource,
functional, and restart/resilience gates have now all passed (`PASS_LOCAL_WORKFLOW`).

## Infrastructure boundary

The full Compose/Kubernetes topology remains useful for distributed acceptance and
production-like testing. PostgreSQL, Redis, Docker/Kubernetes, Vault, OTLP,
Placement and Scheduler are not default local requirements.

Generated configuration, mocks, source inspection or unavailable prerequisites do
not count as native acceptance. Resource viability must be measured on the actual
target hardware before the local product is declared viable.
