# Current status — v0.1.0 pre-release

## Release identity

- Package version: **0.1.0**; no release/tag has been made.
- Working branch: `implementation/production-hardening-20260916`.
- Protected `main`: `66c17153d7ec75b9ad85808ef141b9dba43a9d91`.
- Last source-bearing release-cleanliness commit:
  `45c459c632977143b8681c055847775e88d7ad97`
  (tree `23fdbcdf30adedf984c631b8a060442cc97b426a`).
- Later commits are documentation/evidence-only unless explicitly stated otherwise.

## Product shape

Default local-lite:

- Python A2A-MQ;
- native Dapr sidecar;
- native NATS JetStream;
- SQLite.

Optional local Workflow adds Placement + Scheduler. The distributed profile adds
role separation, PostgreSQL, Dapr Workflow, JetStream/RabbitMQ, MCP, optional Vault
transport protection and Kubernetes deployment support.

## Current formal acceptance

Exact source `45c459c` achieved **FULL_PASS** across all 11 required sub-gates:

- local: 1,150 passed, 0 failures/errors/skips;
- official SDK/MCP: 26 passed;
- verified-TLS PostgreSQL 16: 4 passed;
- TLS Vault Transit: 1 passed;
- repository/dependency/Ruff/Mypy/package: PASS;
- JetStream live: 12/12;
- RabbitMQ live: 12/12.

## Kubernetes deployment acceptance

Image:

`a2amq-prod:6458195-ca@sha256:a54c969183a0c314c77df93c7761db53de19dea8da9e9b20ddebccec425958af`

built from source `6458195b171020ece1911e68bc6d14998d216164` achieved
**PRODUCTION_DEPLOYMENT_PASS (19/19)** on live three-node Kubernetes v1.31.0 with
Dapr 1.18.3 and three-node JetStream.

No deployed application runtime module changed between `6458195` and `45c459c`;
the later source changes affect warning policy, acceptance harness/tests and docs.
A newly rebuilt image is still a new artifact and needs its own deployment evidence.

RabbitMQ Kubernetes deployment was not separately executed; RabbitMQ live/full-stack
acceptance is covered by the 12-check FULL_PASS probe.

## Evidence boundaries

- `productionCertified = false`
- `upstreamBindingCertified = false`
- Hosted GitHub Actions remain pre-runner/unexecuted: `runner_id=0`, zero steps.
- Historical local resource/soak measurements remain bound to their recorded SHAs.
- No generic exactly-once, HA SLA, RPO/RTO or unmeasured latency guarantee is implied.

## First deployment

There is no known repository gate blocker for a first JetStream deployment using the
already Kubernetes-validated image and matching prerequisites. Rebuilds require a
fresh image-bound deployment gate. Claiming RabbitMQ Kubernetes parity requires one
separate execution of the same production-deployment gate.

Historical construction material is under [archive](archive/).
