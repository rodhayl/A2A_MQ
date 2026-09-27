# A2A-MQ

A2A-MQ is an experimental durable A2A task transport. It provides a small local
runtime for development and manual testing, plus a separate distributed/Workflow
profile for deployments that need shared infrastructure and independently scalable roles.

## Release status

- Package version: **0.1.0**.
- Latest tested release candidate: **`v0.1.0-rc2`**.
- `v0.1.0-rc2` is an annotated tag on commit
  `4b1fc34065e37b68258f1944fbcb1e9a15f5fe6d`.
- The tag was validated on Ubuntu 24.04.5 LTS / x86_64 with Python 3.12.3 using
  the exact repository-pinned Dapr 1.18.3 and NATS Server 2.15.0 release archives.
- The RC2 tag remains immutable. The post-RC2 usability/onboarding work was
  independently validated on Linux at commit
  `401471b05da55ba8311b9bc6e5019a24a4250b9b` and merged into `main` via
  `84097baa57da4919c1672a0392397e05c0404882`. It is not part of the RC2 tag.

See [Current Status](docs/STATUS.md) and [Validation](docs/VALIDATION.md) for the
exact evidence boundaries.

## Quick start: Local-lite

Local-lite is the recommended first experience. It runs on one Linux host with three
steady-state processes: the Python A2A-MQ app, `daprd`, and `nats-server`. Application
state is SQLite. Docker, Kubernetes, PostgreSQL, Redis, Vault, Placement and Scheduler
are not required.

```bash
git clone https://github.com/rodhayl/A2A_MQ.git
cd A2A_MQ

python3 -m venv .venv
. .venv/bin/activate
python -m pip install -U pip
python -m pip install -e '.[local]'
python -m pip check

# Download the exact repository-pinned native tools into a private local directory.
python scripts/bootstrap_local_tools.py --output .local-validation/tools

# Verify versions and archive/binary provenance.
python scripts/check_local_native_versions.py \
  --daprd .local-validation/tools/bin/daprd \
  --nats-server .local-validation/tools/bin/nats-server \
  --bootstrap-manifest .local-validation/tools/manifest.json \
  --json .local-validation/native-versions.json

export PATH="$PWD/.local-validation/tools/bin:$PATH"

a2amq-local prepare .local-validation/local-lite
a2amq-local doctor .local-validation/local-lite
a2amq-local run .local-validation/local-lite
```

The native check should print `PASS_NATIVE_VERSION`. When the runtime starts, open:

```text
http://127.0.0.1:18000/lab/
```

Load `.local-validation/local-lite/client.jwt` in Queue Lab. From there you can run
Echo/Uppercase tasks, inspect task state and backlog, pause/resume the simulated local
agent, cancel queued work, use official A2A streaming/subscription flows, and exercise
the real local DLQ archive/replay path.

With the runtime running, a small non-browser happy-path check is also available:

```bash
python scripts/smoke_local.py .local-validation/local-lite
```

For the complete walkthrough, including raw JSON-RPC examples and shutdown checks, use
[Getting Started](docs/GETTING_STARTED.md).

## Runtime tiers

### 1. Local-lite — recommended default

- 1 Python A2A-MQ process;
- 1 native `daprd`;
- 1 native `nats-server` with JetStream;
- SQLite application state.

It supports immediate deterministic tasks and the real A2A/Dapr/JetStream delivery
path. Workflow-only waits, approvals, continuation and timers fail closed instead of
being reimplemented by a second local scheduler.

### 2. Local Workflow — optional

When durable waits, timers, input continuation, approval/rejection, or Workflow
fan-out/fan-in are needed locally, install `.[local,local-workflow]`. The runtime adds
native Dapr Placement and Scheduler, for five steady-state processes, while remaining
single-host and container-free. See [Local Runtime](docs/LOCAL_RUNTIME.md).

### 3. Distributed / production profile

The role-separated implementation supports API, Workflow workers, DLQ/operations roles,
shared PostgreSQL, JetStream or RabbitMQ Dapr Pub/Sub profiles, optional Vault Transit
payload protection, MCP, telemetry, backup/recovery helpers and Kubernetes manifest
generation. See [Implemented Stack](docs/IMPLEMENTED_STACK.md).

## What RC2 validated

The RC2 Linux validation exercised the user-facing local product end to end:

- `python tools/check.py` — PASS;
- Queue Lab tests — 11 passed;
- focused A2A bridge selector — 13 passed;
- focused local-lite pause selector — 1 passed;
- exact native provenance — `PASS_NATIVE_VERSION` for local-lite and Workflow;
- real Chromium Queue Lab smoke — health/readiness, Echo, Uppercase, pause/queue/
  cancel/resume, `SendStreamingMessage`, `SubscribeToTask`, DLQ inspect/replay, raw
  JSON-RPC, invalid auth/session recovery;
- Workflow smoke — input continuation, approve/reject and a short durable timer;
- clean shutdown — no remaining A2A-MQ/Dapr/NATS processes or bound local ports.

Detailed RC2 evidence is recorded in
[docs/validation/queue-lab-rc2-20260927.md](docs/validation/queue-lab-rc2-20260927.md).

Separate historical acceptance remains SHA-bound: source `45c459c...` achieved the
repository `FULL_PASS`, and a digest-pinned image from `6458195...` achieved the
JetStream Kubernetes `PRODUCTION_DEPLOYMENT_PASS (19/19)`. Those results are not
silently relabeled as tests of another image or source tree.

Hosted GitHub Actions currently fail before runner assignment (`runner_id=0`, zero
executed steps). That is an infrastructure state, not a source-test result.

## Documentation

- [Getting Started](docs/GETTING_STARTED.md) — first run from a fresh checkout.
- [Configuration](docs/CONFIGURATION.md) — local options and distributed environment variables.
- [Troubleshooting](docs/TROUBLESHOOTING.md) — actionable recovery for real error codes.
- [API Examples](docs/API_EXAMPLES.md) — copyable A2A JSON-RPC requests.
- [Distributed Deployment](docs/DEPLOYMENT.md) — operator/application responsibility split.
- [Changelog](CHANGELOG.md) — user-visible release and unreleased changes.
- [Queue Lab](docs/QUEUE_LAB.md) — graphical/manual queue and protocol testing.
- [Architecture](docs/ARCHITECTURE.md) — runtime tiers, boundaries and durability model.
- [Local Runtime](docs/LOCAL_RUNTIME.md) — local-lite and optional Workflow details.
- [Implemented Stack](docs/IMPLEMENTED_STACK.md) — distributed capabilities and deployment.
- [Operations](docs/OPERATIONS.md) — recovery, DLQ, retention and operational boundaries.
- [Security](docs/SECURITY.md) — trust boundaries and controls.
- [Protocol](docs/PROTOCOL.md) — envelope, routing and delivery semantics.
- [Current Status](docs/STATUS.md) — release identity and known boundaries.
- [Validation](docs/VALIDATION.md) — SHA-bound evidence index.
- [Documentation Map](docs/README.md) — all maintained documentation.

## Evidence and certification boundary

A2A-MQ uses explicit repository acceptance verdicts. `FULL_PASS`,
`PASS_NATIVE_VERSION` and `PRODUCTION_DEPLOYMENT_PASS` describe their recorded test
gates; they are not third-party certification. The repository still reports
`productionCertified=false` and `upstreamBindingCertified=false`. No generic
exactly-once guarantee, HA SLA, RPO/RTO target or unmeasured performance promise is
implied.
