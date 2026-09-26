# Implementation coverage — v0.1.0 pre-release

Requirement IDs are defined in [REQUIREMENTS.md](REQUIREMENTS.md); exact current
evidence is indexed in [VALIDATION.md](VALIDATION.md).

| Area | Current implementation / evidence |
| --- | --- |
| Local-lite | Python + native Dapr + native JetStream + SQLite. Implemented; current broker-independent acceptance passes. Historical resource/resilience/soak figures are archived and SHA-bound. |
| Optional local Workflow | Python + Dapr Workflow + Placement + Scheduler + local state. Implemented; historical five-process resource/functional/resilience evidence retained in archive. |
| Official A2A | Agent Card, admission/query/cancel/stream lifecycle using pinned official SDK. Current SDK gate passes. |
| MCP | Official streamable HTTP tools and auth boundary. Included in current SDK/full and Kubernetes evidence. |
| SQLite durability | tenant/consumer scoping, fencing, inbox/outbox, projections, replay identities. Implemented. |
| PostgreSQL | shared distributed store and migration/transaction paths. Real verified-TLS gate passes. |
| JetStream | live probe 12/12; Kubernetes production deployment 19/19 overall. |
| RabbitMQ | live probe 12/12; Kubernetes production parity not separately executed. |
| Dapr Workflow | waits, input/auth continuation, cancellation, timers, fan-out/fan-in. Implemented and exercised in live/full topology. |
| Authorization | JWT principal/owner/tenant scopes plus Dapr tokens/scopes. Kubernetes gate verifies authenticated clients and negative owner/tenant paths. |
| DLQ/replay | poison quarantine, bounded archive and authorized replay. Exercised live. |
| Vault protection | optional Vault Transit transport protection; real TLS Vault gate passes. |
| Packaging | isolated wheel build/install/import PASS. |
| Kubernetes | digest pinning, security context, PDB, Dapr state, rolling restart and broker outage; JetStream image PASS 19/19. |
| Backup/recovery | SQLite/PostgreSQL wrappers and guarded history operations; no blanket HA/RPO/RTO claim. |
| Observability | structured logging, metrics and telemetry modules; SLOs/alert thresholds remain deployment-specific. |
| Performance | optional benchmark exists; no unmeasured SLA claim. |
| Independent/upstream certification | tooling/binding present; `upstreamBindingCertified=false`. |
| Hosted CI | workflow exists; current platform runs receive no runner and execute zero steps. |

For the first JetStream deployment, use the validated image digest or rerun the
image-bound gate for any new build.
