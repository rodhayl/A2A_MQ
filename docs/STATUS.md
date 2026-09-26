# Current status — v0.1.0 pre-release

## Release identity

- Package version: **0.1.0**; release candidate checkpoint `v0.1.0-rc1`.
- Working branch: `implementation/production-hardening-20260916`.
- Protected `main`: `66c17153d7ec75b9ad85808ef141b9dba43a9d91`.
- Formal FULL_PASS source remains:
  `45c459c632977143b8681c055847775e88d7ad97`
  (tree `23fdbcdf30adedf984c631b8a060442cc97b426a`).
- `v0.1.0-rc1` points to the previously smoke-tested user-facing checkpoint.
- The working branch now contains **post-RC1 Queue Lab source changes**. Those changes
  are intentionally not relabeled as covered by the earlier FULL_PASS/RC1 smoke.

## Release candidate 1 (v0.1.0-rc1)

- **Release candidate tag**: `v0.1.0-rc1` (the annotated git tag marks this test checkpoint).
- **Targeted commit**: Canonical commit SHA is bound by annotated tag `v0.1.0-rc1`.
- **Package version**: `0.1.0` (unchanged pre-release contract).
- **Date**: 2026-09-26.
- **Smoke result**: `PASS`.
- **What was actually tested**:
  - Canonical user onboarding path documented in [GETTING_STARTED.md](GETTING_STARTED.md).
  - Pinned native tool bootstrapping and version verification (`check_local_native_versions.py`: `PASS_NATIVE_VERSION`).
  - Disposable profile preparation (`a2amq-local prepare`) and startup of the 3-process Local-lite stack (`a2amq-local run`).
  - Health checks: `/health/live` (200 alive), `/health/ready` (200 ready), and Agent Card discovery (`/.well-known/agent-card.json`).
  - End-to-end task execution: task submission via official A2A JSON-RPC `SendMessage`, task ID acquisition, asynchronous completion over JetStream, query via `GetTask` and REST `GET /tasks/{id}`, and operational backlog verification via `/operations/status`.
  - Clean shutdown: SIGTERM shutdown of ASGI server and child processes (`daprd`, `nats-server`), with zero orphan processes and all ports released.
  - Targeted regressions: 38 passed (`tests/test_local_launcher_regressions.py`, `tests/test_local_runtime.py`).
  - Static checks: `python tools/check.py` (syntax, schema copies, and markdown links passed); `python -m pip check` (no broken requirements).
  - CLI help validation: `a2amq-local --help`, `a2amq-local prepare --help`, `a2amq-local run --help`, `a2amq-local-workflow --help`, `python -m a2a_mq --help`.
- **What was deliberately NOT rerun**:
  - Full acceptance suite (`a2a_mq.acceptance --mode full`).
  - Kubernetes cluster redeployment or live node testing.
  - Multi-node distributed stacks, PostgreSQL, Vault, or RabbitMQ live infrastructure gates.
  - 23-minute soak or load/performance campaigns.
  - Dapr Workflow 5-process resource recertification.

## Post-RC1 Queue Lab

The working branch now adds a local browser-based manual test console under
`/lab/` for both local-lite and local Workflow.

Implemented surfaces include:

- real A2A `SendMessage`, `GetTask`, `ListTasks`, `CancelTask` calls;
- task follow and official `SubscribeToTask` attempt with polling fallback;
- operational backlog and DLQ/replay panels;
- Workflow-only input/approval/delay controls when running the Workflow profile;
- raw JSON-RPC console;
- local-lite **Pause simulated agent** control so producers can enqueue durable work
  while the model-free executor is intentionally held, then resume and drain it;
- no CDN/frontend dependency and no server-side JWT bypass.

This is **post-RC1 development**, not part of the already-tagged RC1 evidence. Before
creating `v0.1.0-rc2`, run the focused Queue Lab tests plus one real local-lite
browser smoke (pause → enqueue burst → inspect pending work → resume → completion).

Windows note: Queue Lab itself is browser/Python platform-neutral, but the repository's
pinned native-tool bootstrap remains Linux-only; Windows runtime provisioning is not
yet a repository-certified path.

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
