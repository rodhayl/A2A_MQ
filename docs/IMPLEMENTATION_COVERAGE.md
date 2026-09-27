# Implementation coverage — v0.1.0-rc2

Requirement IDs are defined in [REQUIREMENTS.md](REQUIREMENTS.md). Exact evidence is
indexed in [VALIDATION.md](VALIDATION.md).

| Area | Current implementation / evidence |
| --- | --- |
| Local-lite | Python + native Dapr + JetStream + SQLite; RC2 exact native provenance and real browser/runtime smoke passed. |
| Queue Lab | Embedded local browser console using the real A2A/operations/Dapr/JetStream path; RC2 Chromium smoke passed pause/cancel/stream/subscribe/DLQ/raw/session scenarios. |
| Optional local Workflow | Python + Dapr Workflow + Placement + Scheduler + local state; RC2 exact native provenance plus input continuation, approve/reject and short-timer smoke passed. |
| Official A2A | Agent Card, send/query/list/cancel plus streaming/subscription using pinned official SDK; RC2 focused bridge selector passed. |
| MCP | Official streamable HTTP tools and auth boundary; covered by earlier full/Kubernetes evidence, not rerun in RC2 focused local smoke. |
| SQLite durability | tenant/consumer scoping, immutable identities, fencing, inbox/outbox, task projection and replay identities; implemented. |
| PostgreSQL | shared distributed store and migration/transaction paths; earlier verified-TLS gate passed. |
| JetStream | Local-lite RC2 exercised real JetStream; earlier live probe 12/12 and Kubernetes deployment gate 19/19. |
| RabbitMQ | earlier live probe 12/12; Kubernetes parity not separately executed. |
| Dapr Workflow | input/auth continuation, timers, cancellation and fan-out/fan-in implemented; focused RC2 local smoke plus earlier broader live/full evidence. |
| Authorization | JWT principal/tenant/scopes, task owner checks, Dapr tokens/scopes; RC2 invalid-JWT/session and local owner paths exercised; earlier Kubernetes negative owner/tenant evidence retained. |
| DLQ/replay | poison quarantine, bounded archive, authorization, exact replay identity and integrity checks; RC2 Demo DLQ/Inspect/Replay passed. |
| Vault protection | optional Vault Transit payload protection; earlier real TLS Vault gate passed; not rerun for RC2. |
| Packaging | package version 0.1.0; earlier isolated wheel build/install/import passed. |
| Kubernetes | digest-pinned production renderer and earlier live JetStream deployment PASS 19/19; new image builds require fresh image-bound evidence. |
| Backup/recovery | SQLite/PostgreSQL helpers and guarded operations implemented; no blanket HA/RPO/RTO claim. |
| Observability | structured logs, metrics and telemetry modules implemented; production thresholds/SLOs remain deployment-specific. |
| Performance | benchmark/probe tooling exists; no unmeasured SLA claim. |
| Hosted CI | workflows exist; RC2-related hosted runs received no runner and executed zero steps. |

## Evidence boundaries

RC2 is the current local/user-facing release-candidate checkpoint. It does not
replace the broader earlier acceptance evidence or extend that evidence to a new
container image.

For a production rebuild, rerun the image-bound deployment gate. For RabbitMQ
Kubernetes parity, run the equivalent deployment gate explicitly.
