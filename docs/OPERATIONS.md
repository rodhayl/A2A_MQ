# Operations and recovery

## Deployment boundary

Local fixtures under `deploy/` are not production infrastructure. Production
application resources are generated separately and assume administrator-owned
Kubernetes/Dapr, PostgreSQL, broker, TLS/identity and secret prerequisites.

SQLite is single-host. Verified-TLS PostgreSQL, Vault, both live brokers and the
JetStream Kubernetes gate have passed their current acceptance boundaries, but no
HA/RPO/RTO promise follows automatically.

## Health

Liveness answers whether the process can continue. Readiness verifies required
application/store/Dapr dependencies. Broker quorum/backlog and business SLOs require
separate operational signals.

Permanent malformed messages are quarantined before ACK; if DLQ handoff fails, retry
rather than silently ACK. Transient dependency failures remain retryable.

## Signals

Track accepted publishes, callback attempts, duplicates/conflicts, lease/fence
events, pending/oldest outbox, publish failures, projection lag, DLQ count/age,
dependency readiness and storage capacity.

Structured logs should contain identifiers/reason codes rather than prompts, tokens,
raw secrets or ciphertext keys. Telemetry modules exist; production thresholds and
SLOs are deployment-specific.

## Recovery

For a task, inspect tenant/consumer inbox state, active lease/fence, terminal outcome,
outbox, projection version, broker backlog and DLQ metadata.

Never manually mark a processing claim complete to clear a queue. Reclaimed work gets
a new fence. Outbox recovery republishes the stored immutable envelope. A publish
timeout may be ambiguous, so stable event identity is essential.

DLQ replay is authorized and revalidates archived content/route/identity before
republishing. Replays do not grant permission to mutate old identity/content.

## Backup/restore

SQLite backup/restore wrappers and PostgreSQL native-tool wrappers are implemented.
Dapr Workflow state, application SQL state and broker retention still require a
coordinated DR policy. No blanket application-consistent HA/RPO/RTO claim is made.

## Deploy/drain

Create broker resources before producers, preserve durable/group identities, and
bound SQL/callback/Dapr/ack/lease timeouts coherently.

For shutdown: stop intake, drain bounded in-flight work, let the relay finish its
current publish, then exit without blanket ACKs. Pending durable outbox rows survive
restart.

Schema migration behavior is exercised by the PostgreSQL acceptance gate; future
schema rollback remains an explicit deployment/DR concern.
