# Implementation guidance

Read docs/IMPLEMENTATION_PLAN.md and docs/VALIDATION.md before changing scope.
Preserve requirement/scenario IDs and document contract changes with an ADR.
Run python tools/check.py and pytest; run ruff/mypy when installed. Never describe
skipped infrastructure tests as passing. Commit a tested phase separately.

Do not add a broker, workflow engine, crypto primitive or model dependency to the
core. Never ACK an unfinished claim or execute arbitrary external side effects under
reference-fixture exactly-once assumptions. Preserve tenant/consumer namespacing,
immutable message IDs, fencing, transactional outbox and terminal guards.
Keep spec/envelope.schema.json and packaged src/a2a_mq/schema.json byte-identical.

## Local-product priority

For the owner's resource-constrained server, local economics is a correctness gate.
Follow ADR 0011 and docs/LOCAL_RUNTIME.md.

The default local product is already implemented as `a2a_mq.local_lite` plus the
`a2amq-local` launcher. Its intended steady state is exactly:

- one Python A2A-MQ process;
- one native daprd;
- one native nats-server;
- SQLite application state.

Do not reintroduce PostgreSQL, Redis, Docker/Kubernetes, Vault, OTLP, Placement,
Scheduler, multiple application roles or the Dapr Workflow Python runtime into the
default local tier merely for convenience.

Local-lite deliberately rejects Workflow-only waits/timers/approval/input/fan-out.
The optional `a2a_mq.local_workflow` / `a2amq-local-workflow` tier already
reuses official Dapr Workflow with SQLite state plus Placement/Scheduler. Extend
that tier rather than adding a second scheduler to local-lite. Do not make the
five-process Workflow tier the default without separate resource evidence.

Before broad local testing, create the isolated venv and run the core resource probe
from PROMPT_AGENT_LOCAL_TEST.md. If it passes and native daprd/nats-server already
exist, run the real three-process local stack probe. Do not install host binaries or
services as part of validation.

## Low-resource validation

Use an isolated project-local venv. Keep generated state under ignored
`.local-validation/`. Start with `requirements/local-validation.txt`; only after
the core resource gate passes may the agent install the project's `.[local]` extra
inside that same venv for real local-lite measurement.

Run one inspected test file at a time. Do not run Docker/full distributed stacks,
parallel load tests or host package installation on this machine. Native
PostgreSQL/Vault/Workflow/RabbitMQ/full-stack gates remain separate and unexecuted,
not implicitly passed.
