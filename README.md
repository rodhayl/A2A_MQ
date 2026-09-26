# A2A-MQ

A2A-MQ is an experimental durable A2A task transport with a deliberately lightweight
local product and a separate distributed/Workflow profile.

Package version is **0.1.0 pre-release**. The current source-bearing
release-cleanliness SHA is `45c459c632977143b8681c055847775e88d7ad97`.

## Navigation

- **Try it now** → [Getting Started Guide](docs/GETTING_STARTED.md) (run Local-lite in minutes)
- **Architecture** → [Architecture & Boundaries](docs/ARCHITECTURE.md)
- **Local runtime** → [Lightweight Local Runtimes](docs/LOCAL_RUNTIME.md)
- **Distributed stack** → [Implemented Stack](docs/IMPLEMENTED_STACK.md)
- **Validation & Status** → [Current Status](docs/STATUS.md) | [Evidence](docs/VALIDATION.md)
- **Documentation Map** → [docs/README.md](docs/README.md)

## Runtime tiers

### 1. Default: Local-lite (recommended)

A container-free single-host profile with exactly three steady-state processes:
- 1 Python A2A-MQ process (ASGI server, task ledger, outbox relay);
- 1 native `daprd` sidecar;
- 1 native `nats-server` with JetStream;
- SQLite application state.

PostgreSQL, Redis, Docker, Kubernetes, Vault, Placement, and Scheduler are not required.
Follow the [Getting Started Guide](docs/GETTING_STARTED.md) to install, prepare, and run.

### 2. Optional: Local Workflow

When durable waits, timers, input/approval continuation, or fan-out/fan-in are needed locally,
the optional local Workflow tier adds native Dapr Placement + Scheduler while remaining
single-host and container-free (5 processes). See [docs/LOCAL_RUNTIME.md](docs/LOCAL_RUNTIME.md).

### 3. Distributed / Production

The role-separated implementation supports independent API, Workflow workers, observers,
PostgreSQL, clustered JetStream or RabbitMQ, Vault transit encryption, operational tooling,
and Kubernetes deployment generation. See [docs/IMPLEMENTED_STACK.md](docs/IMPLEMENTED_STACK.md).

## Current validation

Formal acceptance on exact source `45c459c` achieved **FULL_PASS**:
- local: 1,150 passed;
- official SDK/MCP: 26 passed;
- verified-TLS PostgreSQL 16: 4 passed;
- TLS Vault Transit: 1 passed;
- repository/dependency/Ruff/Mypy/package gates: PASS;
- JetStream live probe: 12/12;
- RabbitMQ live probe: 12/12.

A digest-pinned image built from source `6458195` achieved **PRODUCTION_DEPLOYMENT_PASS (19/19)**
on live three-node Kubernetes v1.31.0 + Dapr 1.18.3 + JetStream.

RabbitMQ Kubernetes parity has not been separately executed. Hosted GitHub Actions
currently fail before runner assignment (zero executed steps). The explicit
`productionCertified` and `upstreamBindingCertified` booleans remain false.
Historical records are preserved in [docs/archive/](docs/archive/).
