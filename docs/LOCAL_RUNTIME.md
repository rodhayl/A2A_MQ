# Lightweight local runtime

## Purpose

The default local product is a small single-host bridge, not the distributed
acceptance stack. Its steady-state process budget is:

1. one Python A2A-MQ process;
2. one native `daprd` sidecar;
3. one native `nats-server` process with JetStream.

Application state is one local SQLite database. There is no PostgreSQL, Redis,
Docker/Compose, Kubernetes, Vault, OTLP collector, Placement service or Scheduler
service in this tier.

The existing distributed/Workflow implementation remains available for features
that genuinely need those semantics.

## What local-lite implements

The profile in `a2a_mq.local_lite` reuses the existing durable implementation:

- SQLite TaskLedger admissions, ownership and stable IDs;
- official A2A JSON-RPC and HTTP+JSON bridge, Agent Card and streaming surface;
- JWT owner/tenant authorization;
- Dapr Pub/Sub transport over JetStream;
- durable command publication and SQL outbox;
- immediate deterministic `echo` / `upper` execution;
- get/list/cancel;
- DLQ archive and identity-preserving replay;
- operational status/retention routes;
- optional MCP with submit/get/list/cancel only.

It deliberately rejects `inputRequired`, `authRequired`, non-zero
`delaySeconds`, continuation messages and approval calls with
`WORKFLOW_FEATURE_UNAVAILABLE`. Durable waits, timers, approval/input and
fan-out/fan-in belong to the Workflow tier; local-lite does not recreate a second
workflow engine.

Local-lite also collapses API and worker into one process. It therefore provides
durable restart/redelivery, but it does **not** claim the distributed profile's
independently-offline worker-pool semantics or independent observer fan-out.

## Python environment

Create an isolated venv. The local extra intentionally does not install the Dapr
Python SDK, Workflow SDK, PostgreSQL driver, Redis client or container tooling.

```bash
python3 -m venv .venv
. .venv/bin/activate
python -m pip install -e '.[local]'
python -m pip check
```

For optional MCP:

```bash
python -m pip install -e '.[local,mcp]'
```

The host must already provide executable `daprd` and `nats-server`. The launcher
does not install or modify host software. It finds `daprd` on `PATH` or at the
standard `~/.dapr/bin/daprd` path.

## Prepare a private profile

Use a directory already excluded from Git:

```bash
a2amq-local prepare .local-validation/local-lite
```

The preparer creates owner-private application/Dapr/NATS tokens, a local JWT signing
key, bounded JetStream configuration, Dapr pub/sub components, SQLite state
directories and a one-hour local client JWT. The public listener is fixed to
`http://127.0.0.1:18000` (or `localhost:18000`) and is never bound to a non-loopback
address by the built-in launcher.

The generated active Dapr resources contain two JetStream pub/sub component
views: one command publisher/subscriber and one read-only DLQ subscriber, with
separate durable consumer identities. A scoped Dapr resiliency resource retries
transient inbound command-delivery failures five times at 500 ms before the configured
dead-letter topic. No Dapr state store is loaded by local-lite.

An example `state.sqlite` actor state component is generated under `examples/`
for a future/optional local Workflow tier. It is not in the active resources path.

## Run

```bash
a2amq-local run .local-validation/local-lite
```

The Python launcher itself becomes the ASGI application process, so it does not
remain as a fourth supervisor process. It starts NATS, performs JetStream
provisioning in a short-lived helper process, starts `daprd`, and then serves the
A2A application in the current Python process. If `daprd` or NATS exits, the app
shuts down rather than remaining falsely healthy.

The sidecar is loopback-only, uses an API token, disables metrics, and is started
with Placement and Scheduler addresses explicitly empty because local-lite uses
neither Actors nor Workflow.

## Resource gates

First measure the pure Python+SQLite core:

```bash
python scripts/local_resource_probe.py \
  --state-dir .local-validation/core-probe \
  --json .local-validation/core-probe.json
```

If that passes and the native `daprd` and `nats-server` binaries already exist,
measure the actual three-process runtime:

```bash
python scripts/local_stack_probe.py \
  --profile .local-validation/local-lite \
  --json .local-validation/local-lite-resources.json \
  --log .local-validation/local-lite.log
```

The initial local-lite budget is intentionally conservative and must be validated
on the target machine:

- exactly 3 steady-state product processes;
- total RSS <= 384 MiB;
- aggregate idle CPU <= 5% of one core;
- ready in <= 15 seconds;
- runtime SQLite + JetStream state <= 32 MiB immediately after startup.

These are product budget gates, not production SLOs. Failing them means redesign
the local composition; it is not permission to silently raise machine requirements.

## Optional local Workflow tier

Workflow is not part of local-lite. If the product later needs durable waits,
timers, approval/input or fan-out/fan-in locally, validate a separate tier using
Dapr Workflow. The repository prepares a SQLite actor-state example so Redis is not
introduced merely for convenience. Placement and Scheduler remain explicit extra
processes whose incremental RSS/CPU must be measured before that tier is accepted.

## Distributed / production

The existing role-separated profiles remain the place for shared PostgreSQL,
multiple workers/observers, Dapr Workflow, HA broker/state infrastructure, Vault,
OTLP and Kubernetes. Those capabilities and their acceptance gates are not removed
or silently claimed by local-lite.
