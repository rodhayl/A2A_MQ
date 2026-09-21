# Lightweight local runtimes

## Default: Local MQ Lite

The default local product is intentionally small. Its steady-state process budget is:

1. one Python A2A-MQ process;
2. one native `daprd`;
3. one native `nats-server` with JetStream.

Application state is a local SQLite database. PostgreSQL, Redis, Docker/Compose,
Kubernetes, Vault, OTLP, Placement and Scheduler are not default prerequisites.

Install in an isolated venv:

```bash
python3 -m venv .venv
. .venv/bin/activate
python -m pip install -e '.[local]'
python -m pip check
```

The host must already provide executable `daprd` and `nats-server`. The launcher
never installs host software.

Prepare and run:

```bash
a2amq-local prepare .local-validation/local-lite
a2amq-local run .local-validation/local-lite
```

The launcher itself becomes the ASGI process, so it does not remain as a fourth
supervisor. JetStream provisioning runs in a short-lived helper before steady state.

Local-lite reuses TaskLedger/TaskAPI, official A2A JSON-RPC and HTTP+JSON bindings,
JWT authorization, Dapr Pub/Sub, JetStream, SQLite outbox, cancellation, DLQ/replay
and operations. Immediate deterministic `echo` and `upper` tasks are supported.

Workflow-only semantics are explicit boundaries: `inputRequired`,
`authRequired`, non-zero delays/timers, continuation/approval and fan-out/fan-in
are rejected with `WORKFLOW_FEATURE_UNAVAILABLE`. Local-lite does not implement a
second scheduler.

The active Dapr resources are two JetStream pub/sub views with distinct durable
command and DLQ consumers plus a bounded inbound retry policy (5 x 500 ms) before
dead-lettering. No Dapr state store is loaded in this tier. The local JetStream
stream is file-backed and capped at 64 MiB / 50,000 retained messages with a
7-day maximum age; the server file-store ceiling is 96 MiB. Dapr-to-app delivery
and the application callback semaphore are both capped at two concurrent requests.

## Optional: Local Workflow

When durable waits, timers, approval/input or fan-out/fan-in are actually required,
the repository now provides an optional native Workflow tier. It deliberately keeps
API + Workflow worker in the same Python process and the same sidecar.

Steady-state target:

1. one Python A2A-MQ process;
2. one `daprd`;
3. one `nats-server`;
4. one Dapr `placement`;
5. one Dapr `scheduler`.

State remains local:

- `state/a2amq.db`: A2A-MQ inbox/task/action/outbox SQLite;
- `state/dapr-workflow.db`: Dapr `state.sqlite` actor/Workflow store;
- `state/scheduler/`: single-node Scheduler persistence;
- `nats/`: bounded JetStream files.

There is still no PostgreSQL, Redis, Docker, Vault or OTLP requirement.

Install the extra only in the venv when this tier is needed:

```bash
python -m pip install -e '.[local,local-workflow]'
python -m pip check
```

The host must already provide native `daprd`, `nats-server`, `placement` and
`scheduler`. No validation task is authorized to install them.

Prepare and run:

```bash
a2amq-local-workflow prepare .local-validation/local-workflow
a2amq-local-workflow run .local-validation/local-workflow
```

The Workflow app reuses the existing official Dapr Workflow registration,
`DaprActions`, TaskLedger, A2A bridge, outbox, controls, reconciliation and
DLQ/replay paths. It is not a new workflow engine.

The native Placement/Scheduler commands follow the loopback/single-node shape used
by Dapr 1.18 integration tests. The Workflow state component is `state.sqlite`
with `actorStateStore: true` and is scoped to the same local app id.

This tier is optional because its extra control-plane cost is real. It must not
silently become the default simply because it is available.

## Resource gates

Measure the pure core first:

```bash
python scripts/local_resource_probe.py \
  --state-dir .local-validation/core-probe \
  --json .local-validation/core-probe.json
```

Initial core budget:

- Python+SQLite RSS <= 192 MiB;
- idle CPU <= 3% of one core;
- imports <= 3 seconds;
- 100-task SQLite smoke <= 8 seconds;
- SQLite state <= 16 MiB;
- >= 512 MiB host memory headroom;
- no attributable swap growth.

After the core passes, measure a prepared native profile with the same probe:

```bash
python scripts/local_stack_probe.py \
  --profile .local-validation/local-lite \
  --json .local-validation/local-lite-resources.json \
  --log .local-validation/local-lite.log
```

For local-lite the initial budget is:

- exactly 3 steady processes;
- total RSS <= 384 MiB;
- idle CPU <= 5% of one core;
- readiness <= 15 seconds;
- runtime state <= 32 MiB.

If the optional Workflow tier is explicitly being evaluated:

```bash
python scripts/local_stack_probe.py \
  --profile .local-validation/local-workflow \
  --json .local-validation/local-workflow-resources.json \
  --log .local-validation/local-workflow.log
```

Its separate initial budget is intentionally higher and never substitutes for the
lite gate:

- exactly 5 steady processes;
- total RSS <= 640 MiB;
- idle CPU <= 8% of one core;
- readiness <= 25 seconds;
- runtime state <= 64 MiB.

These are product budgets, not production SLOs. A failed budget means redesign the
tier rather than silently increasing host requirements.

## Loopback and secrets

Both built-in launchers bind the application to `127.0.0.1:18000`, Dapr APIs to
loopback and NATS to `127.0.0.1:4222`. Generated profiles are owner-private and
contain random application/Dapr/NATS tokens plus a local JWT signing key. The
generated client JWTs have a one-hour maximum lifetime and are atomically refreshed
at startup and every 30 minutes while the launcher runs; clients should read the
token file when opening a new connection instead of treating its contents as a
permanent credential. The launcher refuses group/world-accessible profile roots or
a profile created for the other local tier.

The local HTTP A2A endpoint is deliberately loopback-only. Production/distributed
profiles retain HTTPS requirements.

## Distributed / production

The existing role-separated profile remains the place for independent API/workers,
observers, shared PostgreSQL, production broker/state backends, HA, Vault,
observability and Kubernetes. Those capabilities and their acceptance gates are
preserved; neither local tier silently claims them.
