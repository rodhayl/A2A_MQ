# Lightweight local runtimes — v0.1.0-rc2

## Queue Lab

Both local profiles serve the local-only manual test console at
`http://127.0.0.1:18000/lab/`; the launcher also prints this as `labUrl`.

Queue Lab uses the existing A2A/operations surfaces rather than a second backend. Local-lite
can hold command delivery before `ledger.accept` with **Pause simulated agent**, allowing
durably admitted `SUBMITTED` tasks to be inspected/canceled before resuming delivery.
The normal callback timeout and Dapr/JetStream retry/DLQ policy remain authoritative.

The local profile has one command route derived from its configured `--source`; Queue Lab
does not manufacture arbitrary agent/skill routes outside that Dapr component scope.
It exposes official `SendStreamingMessage` and `SubscribeToTask` SSE flows in addition to
ordinary Send/Get/List/Cancel operations. See [QUEUE_LAB.md](QUEUE_LAB.md).

### RC2 validation status

The `v0.1.0-rc2` Linux gate validated the exact pinned Local-lite native tools,
real Chromium Queue Lab flows, pause/queue/cancel/resume behavior, official A2A
streaming/subscription, DLQ inspect/replay, clean shutdown, and the focused optional
Workflow input/approval/timer scenarios. See
[validation/queue-lab-rc2-20260927.md](validation/queue-lab-rc2-20260927.md).

Historical resource/resilience/soak measurements remain bound to their recorded
SHAs; RC2 did not relabel those older measurements as newly rerun evidence.

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

Before starting a prepared profile, `a2amq-local doctor <profile>` provides a
non-mutating prerequisite/profile/port check. It does not replace the exact native
provenance gate; when it detects a repository bootstrap directory it reuses that
checker directly.

The launcher never installs host software. Existing `daprd` and `nats-server`
executables may be used directly. For a disposable validation run on Linux
amd64/arm64, `scripts/bootstrap_local_tools.py` can instead download the
repository-pinned Dapr 1.18.3 all-components/default archive and NATS Server 2.15.0
archive into an ignored private directory. JetStream is compiled behind Dapr's
`allcomponents` build tag, so the bootstrap intentionally uses the default/all-components
`daprd` release asset rather than the smaller `stablecomponents` flavor. It verifies
fixed SHA-256 digests and exact official archive sizes, retains those private
release archives, extracts only the expected regular binaries, and records
compressed/extracted byte totals in the private manifest. The native-version checker
reopens the retained archives and proves that each installed executable is the exact
pinned archive member. On amd64 the pinned downloads are 63,716,263 bytes for Dapr and 7,236,657
bytes for NATS (70,952,920 bytes / 67.67 MiB total compressed); on arm64 they total
63,342,655 bytes / 60.41 MiB compressed. This is tooling download/disk cost, not
runtime RSS. Prepend that run-local `bin/` to `PATH`. This is a test bootstrap,
not a host installation or production updater. For the reproducible local economics
gate, prefer this pinned private asset even when a same-version host daprd exists:
Dapr's version output does not identify whether that binary was built with
`allcomponents`, which JetStream requires.

Prepare and run:

```bash
a2amq-local prepare .local-validation/local-lite
a2amq-local run .local-validation/local-lite
```

The launcher fails closed before rotating client credentials or starting children if
one of its fixed loopback ports is already occupied. This prevents accidentally
connecting Dapr/NATS to another local project's service and avoids exposing the
generated app token to an unrelated listener on the configured app port.

The launcher itself becomes the ASGI process, so it does not remain as a fourth
supervisor. JetStream provisioning runs in a short-lived helper before steady state.

Local-lite uses one combined durable action relay for command publication and
immediate start/cancel actions, rather than two independent 100 ms SQLite pollers.
The outbox relay remains separate because it operates on the transactional outbox.

Local-lite reuses TaskLedger/TaskAPI, official A2A JSON-RPC and HTTP+JSON bindings,
JWT authorization, Dapr Pub/Sub, JetStream, SQLite outbox, cancellation, DLQ/replay
and operations. Immediate deterministic `echo` and `upper` tasks are supported.

Workflow-only semantics are explicit boundaries: `inputRequired`,
`authRequired`, non-zero delays/timers, continuation/approval and fan-out/fan-in
are rejected with `WORKFLOW_FEATURE_UNAVAILABLE`. Local-lite does not implement a
second scheduler.

The active Dapr resources are two JetStream pub/sub views with distinct durable
command and DLQ consumers plus a bounded inbound retry policy (5 x 500 ms) before
dead-lettering. No Dapr state store is loaded in this tier. The pinned Dapr 1.18.3
runtime defines empty placement/scheduler addresses as the disabled state.
Local-lite passes `--placement-host-address=` and `--scheduler-host-address=`
explicitly, and its least-privilege child environment also strips inherited Dapr
control-plane address variables. Native version validation must still pass before
the three-process economics gate is accepted. The local JetStream
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

For a reproducible validation run, bootstrap the optional control plane into the
same private tools directory rather than relying on host Placement/Scheduler:

```bash
python scripts/bootstrap_local_tools.py --workflow --output .local-validation/workflow-tools
export PATH="$PWD/.local-validation/workflow-tools/bin:$PATH"
python scripts/check_local_native_versions.py \
  --daprd .local-validation/workflow-tools/bin/daprd \
  --nats-server .local-validation/workflow-tools/bin/nats-server \
  --placement .local-validation/workflow-tools/bin/placement \
  --scheduler .local-validation/workflow-tools/bin/scheduler \
  --bootstrap-manifest .local-validation/workflow-tools/manifest.json \
  --json .local-validation/workflow-native-versions.json
```

The Workflow bootstrap uses the separately published official Dapr 1.18.3
Placement/Scheduler release archives and records their archive and extracted-binary
SHA-256 provenance. It does not install anything into the host. Local-lite omits
those assets unless `--workflow` is explicitly selected.

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

Measure the pure core first in a fresh venv containing only
`requirements/core-probe.txt` (currently the schema-validation runtime dependency).
Do not install the A2A/HTTP/local extras before this first measurement:

```bash
python -m pip install --only-binary=:all: -r requirements/core-probe.txt
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

After the core passes, measure the Python product process independently:

```bash
python scripts/local_python_probe.py \
  --state-dir .local-validation/python-app \
  --json .local-validation/python-app-resources.json
```

This builds the real local-lite A2A/HTTP/crypto/SQLite app and enters its lifespan
without starting daprd or NATS. The default 60-second idle window covers the
periodic admission-repair cadence instead of sampling only the first seconds.
Initial budget: <=192 MiB RSS, <=3% one-core idle
CPU, <=5 seconds build/start and <=4 MiB SQLite state. This isolates our own process
cost from the sidecar and broker.

After the core passes, measure a prepared native profile with the same probe:

```bash
python scripts/local_stack_probe.py \
  --profile .local-validation/local-lite \
  --tools-dir .local-validation/tools \
  --json .local-validation/local-lite-resources.json \
  --log .local-validation/local-lite.log
```

The native stack probe also observes a 60-second idle window by default so
periodic maintenance contributes to the CPU result. Its JSON records the bootstrap
manifest SHA-256 and per-binary SHA-256 values. For an accepted measurement these
must exactly equal the values emitted by `check_local_native_versions.py` immediately
before the probe; `nativeToolsBound` must also be true. This binds provenance and
resource measurement across the two evidence files.

For local-lite the initial budget is:

- exactly 3 steady processes;
- peak aggregate RSS <= 384 MiB across startup and idle;
- idle CPU <= 5% of one core;
- readiness <= 15 seconds;
- runtime state <= 32 MiB both while running and after clean shutdown.

If the optional Workflow tier is explicitly being evaluated:

```bash
python scripts/local_stack_probe.py \
  --profile .local-validation/local-workflow \
  --tools-dir .local-validation/workflow-tools \
  --json .local-validation/local-workflow-resources.json \
  --log .local-validation/local-workflow.log
```

Its separate initial budget is intentionally higher and never substitutes for the
lite gate:

- exactly 5 steady processes;
- peak aggregate RSS <= 640 MiB across startup and idle;
- idle CPU <= 8% of one core;
- readiness <= 25 seconds;
- runtime state <= 64 MiB both while running and after clean shutdown.

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


### Dapr internal gRPC port

Both native local tiers pin Dapr's internal gRPC listener to
`127.0.0.1:50002` and include that port in the fail-closed preflight. Dapr 1.18.3
otherwise derives a stable internal port when none is supplied; making it explicit
keeps the local topology deterministic and prevents a hidden port collision from
appearing only after NATS/Dapr startup.


### Poison-message terminal parking

Both local pub/sub components use the same bounded Dapr inbound retry policy
(5 retries at 500 ms). Command failures dead-letter to the local DLQ; failures
while archiving that DLQ are then forwarded to `a2a.v1.<tenant>.parking`.
The parking topic intentionally has no local application consumer, preventing a
corrupt DLQ delivery from redelivering forever or recursively dead-lettering.
