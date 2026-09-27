# Implemented stack — v0.1.0-rc2

This document describes what is implemented now. Validation strength differs by
profile; see [VALIDATION.md](VALIDATION.md) for SHA-bound evidence.

## Local-lite — recommended default

Steady state:

1. Python A2A-MQ process;
2. native `daprd`;
3. native `nats-server` with JetStream;
4. SQLite application state.

Recommended reproducible Linux bootstrap:

```bash
python3 -m venv .venv
. .venv/bin/activate
python -m pip install -e '.[local]'
python -m pip check

python scripts/bootstrap_local_tools.py --output .local-validation/tools
python scripts/check_local_native_versions.py \
  --daprd .local-validation/tools/bin/daprd \
  --nats-server .local-validation/tools/bin/nats-server \
  --bootstrap-manifest .local-validation/tools/manifest.json \
  --json .local-validation/native-versions.json

export PATH="$PWD/.local-validation/tools/bin:$PATH"

a2amq-local prepare .local-validation/local-lite
a2amq-local run .local-validation/local-lite
```

The pinned bootstrap uses Dapr 1.18.3 all-components/default and NATS Server 2.15.0.
The provenance checker binds each installed binary to the exact retained release
archive member.

Local-lite includes:

- official A2A bridge and Agent Card;
- Send/Get/List/Cancel;
- `SendStreamingMessage` and `SubscribeToTask`;
- SQLite TaskLedger, inbox/idempotency and transactional outbox;
- real Dapr Pub/Sub + JetStream command delivery;
- authorized DLQ archive/replay;
- browser Queue Lab at `/lab/`;
- local pause/resume test control at the delivery boundary;
- deterministic Echo/Uppercase execution.

Workflow-only waits, approvals, continuation and timers are deliberately rejected.

## Optional local Workflow

Install only when Workflow semantics are required:

```bash
python -m pip install -e '.[local,local-workflow]'

python scripts/bootstrap_local_tools.py \
  --output .local-validation/workflow-tools \
  --workflow
```

This adds:

- Dapr Placement 1.18.3;
- Dapr Scheduler 1.18.3;
- official Dapr Workflow runtime;
- separate local Workflow/actor state;
- input/auth continuation;
- durable delay/timer behavior;
- Workflow fan-out/fan-in paths.

Steady state is five processes: A2A-MQ, `daprd`, `nats-server`, Placement and
Scheduler.

## Distributed application

The role-separated distributed implementation supports:

- API role;
- Workflow worker role;
- DLQ/operations role;
- SQLite or shared PostgreSQL application stores;
- JetStream or RabbitMQ Dapr Pub/Sub profiles;
- Dapr Workflow;
- official A2A SDK bridge;
- official MCP streamable HTTP tools;
- JWT subject/tenant/scope authorization;
- task owner isolation;
- DLQ/archive/replay;
- optional Vault Transit payload protection;
- metrics/telemetry;
- backup/restore/retention helpers.

## Kubernetes rendering

`deploy/production/generate.py` renders application/Dapr Kubernetes resources for
administrator-managed infrastructure.

The renderer requires:

- an immutable image reference ending in `@sha256:<digest>`;
- HTTPS public URL and issuer;
- a JWT public key;
- selected JetStream or RabbitMQ profile;
- external PostgreSQL/Dapr Workflow state secrets;
- administrator-owned broker, TLS, Dapr control plane and network/security setup.

It does not fabricate a production database, identity provider, CA, broker cluster or
secret material.

## Disposable Compose fixture

`deploy/generate.py` remains a separate loopback-only Compose validation fixture.
It is **not** the recommended Local-lite runtime and intentionally has its own
container versions/configuration. See [../deploy/README.md](../deploy/README.md).

Do not infer RC2 native-tool provenance from that Compose fixture.

## Current RC2 validation

RC2 commit `4b1fc34065e37b68258f1944fbcb1e9a15f5fe6d` passed the focused real-Linux
local validation:

- Local-lite native provenance: `PASS_NATIVE_VERSION`;
- Queue Lab tests: 11 passed;
- A2A subscribe/stream/cancel selector: 13 passed;
- Local-lite queue/pause selector: 1 passed;
- real Chromium Queue Lab smoke;
- Workflow native provenance: `PASS_NATIVE_VERSION`;
- Workflow input/approve/reject/timer smoke;
- clean shutdown.

See
[validation/queue-lab-rc2-20260927.md](validation/queue-lab-rc2-20260927.md).

## Earlier broader acceptance

Exact source `45c459c632977143b8681c055847775e88d7ad97` achieved the repository
`FULL_PASS`, including real JetStream, RabbitMQ, PostgreSQL and Vault gates.

A digest-pinned image built from `6458195b171020ece1911e68bc6d14998d216164`
achieved `PRODUCTION_DEPLOYMENT_PASS (19/19)` on a live three-node Kubernetes
JetStream deployment.

Those results remain bound to their recorded source/image and are not silently
relabelled as full acceptance of RC2.

## Certification boundary

Repository verdicts are engineering acceptance gates, not third-party certification.

- `productionCertified=false`
- `upstreamBindingCertified=false`
- RabbitMQ Kubernetes parity has not been separately executed.
- No generic exactly-once, HA SLA, RPO/RTO or unmeasured performance claim is made.
