# Operations and recovery — v0.1.0-rc2

## Deployment boundary

The built-in Local-lite and local Workflow profiles are loopback single-host
runtimes. The Compose fixture under `deploy/` is disposable validation
infrastructure. Neither is a production trust/HA topology.

Production Kubernetes rendering assumes administrator-owned Dapr, PostgreSQL,
broker, TLS/identity, networking and secret prerequisites.

## Health and readiness

Local profiles expose:

- `/health/live` — process/lifecycle status;
- `/health/ready` — application storage + required Dapr/broker reachability;
- `/.well-known/agent-card.json` — A2A discovery.

Readiness is not a business SLO. Broker quorum, backlog age, storage capacity and
application latency need their own operational signals.

## Local operational dashboard

Queue Lab reads authenticated `/operations/status` and exposes values such as:

- active task count;
- task records;
- pending actions;
- pending publications;
- oldest pending publication age;
- visible queued/submitted tasks.

Queue Lab is for the loopback local profiles, not a production operator console.

## Delivery failures

Permanent invalid deliveries are quarantined before ACK when possible. If the DLQ
handoff itself fails, the callback remains retryable rather than silently ACKing.

Transient dependency failures return retry semantics.

Local command/DLQ components have bounded inbound retry policy. Poison-message
archival failures can terminate in the local parking topic instead of recursively
dead-lettering forever.

## DLQ and replay

DLQ records are authenticated operational data.

Replay:

1. authorizes the principal;
2. verifies archived record integrity;
3. reconstructs the original CloudEvent content;
4. revalidates route/identity/expiry;
5. stores a stable replay identity;
6. republishes through the configured Dapr path.

Replay does not invent new business identity and does not grant permission to mutate
an already committed task result.

RC2 exercised Demo DLQ -> archive -> Inspect -> Replay on real Local-lite and
confirmed the committed terminal task remained intact/non-duplicated.

## Recovery

For a stuck task, inspect:

- tenant/consumer scope;
- task and immutable request identity;
- current task state/history;
- active claim/lease/fence where applicable;
- pending actions;
- pending outbox;
- broker backlog/redelivery;
- DLQ metadata;
- dependency readiness.

Do not manually mark a processing claim complete to clear a queue. Reclaimed work
must obtain a new fence. A stale worker cannot commit through an old fence.

A publish timeout can be ambiguous. Recovery relies on stable message/event identity,
not on generating a new ID for the same logical attempt.

## Backup and restore

SQLite and PostgreSQL backup/restore helpers are implemented.

A complete disaster-recovery plan must also account for:

- Dapr Workflow state;
- broker retention/state;
- JWT/identity material;
- payload-protection key lifecycle where enabled.

No blanket application-consistent HA, RPO or RTO claim is made.

## Graceful shutdown

For a managed runtime:

1. stop new intake;
2. allow bounded in-flight work to settle;
3. preserve pending durable outbox/actions;
4. stop application/sidecars/control-plane processes cleanly;
5. confirm expected local ports are released.

For the built-in local profiles:

```bash
ps aux | grep -E 'a2amq|daprd|nats-server' | grep -v grep || true
ss -tulpn | grep -E ':(18000|3500|50001|50002|50005|50006|4222)\b' || true
```

RC2 validated zero leftover runtime processes/ports after Local-lite and local
Workflow shutdown.

## Signals to monitor in distributed deployments

Useful signals include:

- accepted publishes;
- callback attempts/retries;
- duplicate/conflict counts;
- lease/fence takeover;
- pending/oldest outbox;
- publication failures;
- task/projection lag;
- DLQ count/age;
- dependency readiness;
- storage capacity/latency.

Structured logs should contain identifiers/reason codes rather than prompts, bearer
tokens, private keys or raw credentials.

## Evidence boundary

Earlier verified-TLS PostgreSQL, Vault, live broker and Kubernetes evidence remains
recorded in [VALIDATION.md](VALIDATION.md). RC2 focused on the local user-facing
product and did not rerun unrelated distributed certification gates.
