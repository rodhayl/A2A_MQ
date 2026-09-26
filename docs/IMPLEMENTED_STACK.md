# Implemented stack — v0.1.0 pre-release

## Local-lite

Default steady state:

1. Python A2A-MQ process
2. native `daprd`
3. native `nats-server` with JetStream
4. SQLite files

Install:

```bash
python3 -m venv .venv
. .venv/bin/activate
python -m pip install -e '.[local]'
a2amq-local prepare .local-validation/local-lite
a2amq-local run .local-validation/local-lite
```

The launcher does not install host services. Repository validation tooling can
bootstrap pinned native binaries into a private ignored directory for disposable
validation.

## Optional local Workflow

```bash
python -m pip install -e '.[local,local-workflow]'
a2amq-local-workflow prepare .local-validation/local-workflow
a2amq-local-workflow run .local-validation/local-workflow
```

This adds Placement + Scheduler and Dapr Workflow semantics while remaining
single-host and container-free.

## Distributed application

The role-separated implementation supports API, Workflow workers, observers and
operations with:

- SQLite/PostgreSQL stores;
- JetStream or RabbitMQ Dapr Pub/Sub profiles;
- Dapr Workflow;
- official A2A SDK bridge;
- official MCP streamable HTTP tools;
- JWT owner/tenant authorization;
- DLQ/archive/replay;
- optional Vault Transit transport protection;
- metrics/telemetry;
- backup/restore/retention tooling.

## Disposable full-stack validation

```bash
python -m a2a_mq.deployment compose   --broker jetstream   --source "$PWD"   --output /tmp/a2amq-stack
```

RabbitMQ is also supported by the disposable/full acceptance topology. These
fixtures generate private secrets and are not production credentials.

## Production application rendering

Production Kubernetes resources require digest-pinned images and administrator-owned
secrets/infrastructure. The renderer produces application/Dapr resources; it does
not fabricate a production database, identity provider, CA or broker control plane.

Production prerequisites include secured Dapr control plane, Placement/Scheduler,
verified PostgreSQL state, verified broker TLS/auth, JWT identity material, required
Kubernetes Secrets/ConfigMaps and ingress/network policy.

## Formal acceptance

```bash
python -m a2a_mq.acceptance --mode full --root "$PWD"   --output /tmp/a2amq-full-evidence   --allow-disposable-containers --timeout 2400
```

Current source `45c459c` achieved FULL_PASS across local, SDK/MCP, verified-TLS
PostgreSQL, Vault, package/static and both live broker gates.

The JetStream Kubernetes production-deployment gate separately achieved 19/19 using
the digest-pinned image recorded in `VALIDATION.md`.

Archive only reviewed/sanitized evidence. Do not persist generated JWT private keys,
TLS private keys, database/broker passwords or Vault tokens.

## Evidence semantics

`FULL_PASS` is a repository acceptance verdict, not signed conformance or
production certification. `productionCertified` and `upstreamBindingCertified`
remain false. RabbitMQ Kubernetes deployment parity has not been separately executed.
