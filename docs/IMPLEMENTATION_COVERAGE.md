# Requirement coverage after integrated implementation

Implementation presence, local evidence and native infrastructure acceptance are
separate columns. Requirement IDs and phase numbering are preserved. No historical
snapshot was reconstructed; these are incremental implementations on the remote
base and do not inherit that snapshot's old acceptance claims.

| Requirements / phase | Implemented source | Remaining acceptance |
| --- | --- | --- |
| FR-001/009/010/037/040/050/052; phases 1-2 | domain/schema/transport, stable routing/identity, metadata and Dapr boundary | Existing whole-repository and native protocol regression |
| FR-002/011/012/020/022/024/026; phases 2/9 | durable SQL admissions, admission repair, broker provisioner, role components/queues, native dual-broker harness | Actual JetStream and RabbitMQ runs; do not infer parity from Dapr abstraction |
| FR-021/023/027; phase 3 | SQLiteStore, pooled PostgresStore, leases/fences/outbox, projection ordering | Full historical regression and shared PostgreSQL live gate |
| FR-003/007/008; phase 4 | TaskLedger, official Dapr Workflow registration/actions/reconcile, input/auth/cancel, timer, fan-out/fan-in | Native scheduling/restart/race gates |
| FR-004/005/006/013/014; phase 5 | TaskAPI plus official SDK Agent Card, send/get/list/cancel/subscribe and SSE/artifact conversion | Official SDK clients/native streaming; formal independent conformance |
| FR-030/031/032/038; phase 6 | pinned RS256 principal/owner scopes, per-call MCP verification, role-specific secret/config, Dapr scopes, TLS production profiles | Deployed workload/broker policies, certificate/credential rotation, cross-tenant native negative tests |
| FR-033/034/035/036; phase 7 | library AEAD and key wrap, Vault Transit, authenticated routing header, saved ciphertext, explicit read windows | Real Vault gate and encrypted live deployment/rotation acceptance |
| FR-025/044; phase 8 | bounded DLQ archive, operator API, strict original-identity replay, separate RabbitMQ parking route | Real broker DLQ and ciphertext replay; reviewed archive retention operations |
| FR-041/042/043; phase 8 | OTel counters/histograms/spans/backlog, metadata-only SDK handlers, liveness/readiness/drain | Export destination, capacity/failover tests and operational alert tuning |
| NFR-008; phase 8 | bounded admission/control/SQL lanes, task/projection quotas and terminal-only retirement, retained identity tombstones | Capacity sizing, archive/export strategy and retention windows |
| Phase 8 recovery | WAL-aware SQLite backup, hash-verified private restores, native pg_dump/pg_restore wrappers, Dapr history purge guard | Real PostgreSQL restore and coherent whole-system DR/HA drills |
| FR-051; phase 9 | same roles and native acceptance suite for JetStream/RabbitMQ, explicit provisioned backend settings | Both native suites must pass; configuration alone is not portability proof |
| Phase 10 | ADR 0010: official native HTTP bindings; experimental private MQ profile, no new standardization claim | Independent review, relevant upstream conformance/compatibility decisions |
| MCP integration | official MCP SDK adapter and six authorized tools sharing TaskAPI | Actual MCP HTTP client/server gate |

## Deliberate boundaries, not silent substitutes

The reference activities perform text transformations. General arbitrary external
exactly-once effects, a new LLM framework, global ordering, broker federation, a
handwritten crypto primitive and a replacement Workflow engine remain non-goals.
Full Task snapshots are canonical internally; arbitrary inbound delta reassembly
is not silently claimed. Optional push notifications are explicitly unsupported.
Production control planes, HA data services and credential issuance are managed
infrastructure prerequisites, not emulated by the local development profile.

NFR-006 latency values are unmeasured targets. The benchmark reports observations
when executed; no numerical SLA is claimed by implementation or unit-test counts.
Transitive hash-locking, vulnerability/container/SBOM auditing, full repository
lint/types and native deployed acceptance remain release gates. These are not
converted into passing statuses because their tooling/services are unavailable.
