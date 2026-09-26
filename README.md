# A2A-MQ

A2A-MQ is an experimental durable A2A task transport with a deliberately lightweight
local product and a separate distributed/Workflow profile.

Package version is **0.1.0 pre-release**. The current source-bearing
release-cleanliness SHA is
`45c459c632977143b8681c055847775e88d7ad97`.

## Local-first product

The default local runtime is intentionally small:

- one Python A2A-MQ process;
- one native `daprd`;
- one native `nats-server` with JetStream;
- SQLite application state.

PostgreSQL, Redis, Docker, Kubernetes, Vault, OTLP, Placement and Scheduler are not
default local-lite prerequisites.

```bash
python3 -m venv .venv
. .venv/bin/activate
python -m pip install -e '.[local]'
a2amq-local prepare .local-validation/local-lite
a2amq-local run .local-validation/local-lite
```

Local-lite supports durable task admission/query, official A2A bindings, JWT
authorization, Dapr Pub/Sub, JetStream, SQLite inbox/outbox state, cancellation,
DLQ/replay and operations. Workflow-only waits, input/auth continuation, timers and
fan-out/fan-in are rejected explicitly rather than emulated.

## Optional local Workflow tier

When Workflow semantics are needed locally:

```bash
python -m pip install -e '.[local,local-workflow]'
a2amq-local-workflow prepare .local-validation/local-workflow
a2amq-local-workflow run .local-validation/local-workflow
```

This adds native Placement + Scheduler while remaining single-host and container-free.
It does not replace the cheaper local-lite default.

## Distributed profile

The distributed implementation supports role separation, PostgreSQL, Dapr Workflow,
JetStream or RabbitMQ, official A2A/MCP surfaces, owner/tenant authorization,
DLQ/replay, optional Vault-backed payload protection, operational tooling and
Kubernetes deployment generation.

No generic exactly-once claim is made for arbitrary external side effects. Callers
still need destination idempotency or reconciliation.

## Current validation

Formal acceptance on exact source `45c459c` achieved **FULL_PASS**:

- local: 1,150 passed;
- official SDK/MCP: 26 passed;
- verified-TLS PostgreSQL 16: 4 passed;
- TLS Vault Transit: 1 passed;
- repository/dependency/Ruff/Mypy/package gates: PASS;
- JetStream live probe: 12/12;
- RabbitMQ live probe: 12/12.

A digest-pinned image built from source `6458195` achieved
**PRODUCTION_DEPLOYMENT_PASS (19/19)** on live three-node Kubernetes v1.31.0 +
Dapr 1.18.3 + JetStream. No deployed runtime module changed between that image
source and `45c459c`, but a newly rebuilt image is a new artifact and must obtain
fresh image-bound deployment evidence.

RabbitMQ Kubernetes parity has not been separately executed. Hosted GitHub Actions
currently fail before runner assignment (zero executed steps). The explicit
`productionCertified` and `upstreamBindingCertified` booleans remain false.

## Documentation

Start at [docs/README.md](docs/README.md). Current release status is in
[docs/STATUS.md](docs/STATUS.md) and current evidence in
[docs/VALIDATION.md](docs/VALIDATION.md).

Superseded plans, prompts and historical validation reports are preserved under
[docs/archive/](docs/archive/).
