# A2A-MQ

Experimental durable A2A task messaging with a **lightweight local bridge** and a
separate distributed/Workflow profile.

The local product is deliberately small: **one Python A2A-MQ process + one native
`daprd` + one native `nats-server`**, with SQLite application state. PostgreSQL,
Redis, Docker, Kubernetes, Vault, OTLP, Placement and Scheduler are not default local
prerequisites.

## Lightweight local bridge

Use `a2a_mq.local_lite:from_env` or the `a2amq-local` launcher.

```bash
python3 -m venv .venv
. .venv/bin/activate
python -m pip install -e '.[local]'
a2amq-local prepare .local-validation/local-lite
a2amq-local run .local-validation/local-lite
```

The host must already contain native `daprd` and `nats-server`; the launcher does
not install or modify host software and does not use containers.

Local-lite reuses the existing durable TaskLedger, TaskAPI, official A2A HTTP
bindings, JWT authorization, Dapr Pub/Sub, JetStream, SQLite outbox, cancellation,
DLQ/replay and operations. Immediate `echo` / `upper` tasks are supported.

Workflow-only semantics are explicit feature boundaries in this tier:
`inputRequired`, `authRequired`, timers/delays, continuation/approval and
fan-out/fan-in require the Workflow profile and are rejected rather than emulated.

See [lightweight local runtime](docs/LOCAL_RUNTIME.md) and
[ADR 0011](docs/adr/0011-local-footprint-first.md).

## Measure viability before broad testing

Local economics is a product gate.

```bash
python scripts/local_resource_probe.py \
  --state-dir .local-validation/core-probe \
  --json .local-validation/core-probe.json
```

After the core passes, and only when `daprd` and `nats-server` already exist:

```bash
python scripts/local_stack_probe.py \
  --profile .local-validation/local-lite \
  --json .local-validation/local-lite-resources.json \
  --log .local-validation/local-lite.log
```

The second probe measures the actual three steady-state processes. A failed resource
budget is a local-product redesign signal, not a reason to silently increase machine
requirements.

## Distributed / Workflow profile

Use `a2a_mq.application:from_env` for the role-separated implementation:
`api`, `workflow`, `observer` and `operator`.

That profile retains official Dapr Workflow, shared SQLite/PostgreSQL adapters,
multiple workers/observers, optional MCP, Vault-backed payload protection,
observability and production deployment support. The complete Compose/Kubernetes
topologies are acceptance/distributed profiles, not the default local runtime.

The disposable full-stack generator remains available for infrastructure testing:

```bash
python -m a2a_mq.deployment compose --broker jetstream \
  --source "$PWD" --output /path/to/fresh/stack
```

RabbitMQ remains the second distributed broker backend. Running those profiles may
require PostgreSQL, Workflow state, Dapr control services, containers and other
production-like dependencies; none of those are evidence about local-lite economics.

## Correctness boundaries

Each SQL key includes tenant and logical consumer. Stable message IDs, content
hashes, fencing, transactional task/outbox updates and monotonic snapshots protect
retries and stale writers. DLQ replay preserves original identity/content.

Local-lite collapses API and immediate worker execution into one process. It provides
durable restart/redelivery, but does **not** claim independently-offline worker-pool
or independent-observer semantics. Those remain distributed-profile gates.

Arbitrary external tool effects still require destination idempotency or explicit
reconciliation. No generic exactly-once claim is made.

## Validation status

Implementation and evidence are separate. Historical validation records remain under
`docs/validation/`; they do not automatically certify the current SHA.

The local-lite source, launcher and resource probes now exist, but the real native
three-process resource/interop gate must be run on the target machine. Real
PostgreSQL, Vault, Dapr Workflow, JetStream/RabbitMQ parity and production deployment
remain separate gates.

Missing prerequisites, skipped tests or generated manifests are never converted into
a production or conformance pass. Package version remains **0.1.0**.

See:

- [local runtime](docs/LOCAL_RUNTIME.md)
- [integrated stack](docs/IMPLEMENTED_STACK.md)
- [requirement coverage](docs/IMPLEMENTATION_COVERAGE.md)
- [validation record](docs/VALIDATION.md)
- [A2A binding decision](docs/adr/0010-native-bindings-and-operations.md)
