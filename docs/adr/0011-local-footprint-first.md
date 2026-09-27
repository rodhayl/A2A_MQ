# ADR 0011: local footprint is a product gate

## Status

Accepted and implemented in source. Native resource/interop evidence on the target
machine is still required.

## Context

The repository has a full distributed acceptance topology with PostgreSQL, Workflow
state, brokers, Dapr control services, multiple roles, gateway and observability.
That topology is useful for distributed testing but is not an acceptable default for
a low-resource machine whose primary purpose is a local A2A bridge.

Dapr can run self-hosted without Docker. Placement is unnecessary for applications
that do not use Actors, and Scheduler can be omitted when the local tier does not use
Workflow/Jobs/reminders. Dapr also provides a SQLite state component suitable for a
separate single-host Actor/Workflow tier, so Redis is not a mandatory local choice.

## Decision

1. Resource viability is the first local-product acceptance gate.
2. The full Compose/Kubernetes topology is an infrastructure/distributed acceptance
   profile, not the default local runtime.
3. PostgreSQL, Redis, Vault, external OTLP and container orchestration are not
   prerequisites for the default local bridge.
4. The default local implementation is `a2a_mq.local_lite`:
   - one Python application process;
   - one native `daprd`;
   - one native single-node `nats-server`/JetStream;
   - SQLite application state.
5. The launcher does not retain a separate supervisor process. It becomes the ASGI
   process after starting NATS and daprd. Broker provisioning is short-lived.
6. Local-lite uses Dapr Pub/Sub but does not use Dapr Workflow, Actors, Placement or
   Scheduler. It supports immediate deterministic tasks and rejects Workflow-only
   waits/timers/approval/input/fan-out.
7. API and immediate execution are collapsed into one process. Therefore local-lite
   does not claim independently-offline worker-pool or independent-observer
   semantics; those remain distributed-profile requirements.
8. The optional local Workflow tier is implemented with official Dapr Workflow,
   a local SQLite actor/Workflow state component, native Placement and native
   Scheduler. API + Workflow share the Python process and sidecar. Its five-process
   footprint is an explicit incremental cost and it never becomes default without
   its own measured gate.
9. The distributed/production profile remains role-separated and may use shared
   PostgreSQL, production state/broker backends, HA/security/observability.
10. A tier advertises only capabilities it actually runs. Missing infrastructure
    never becomes a passing gate.

## Resource gates

The pure core probe uses conservative initial targets:

- Python+SQLite RSS <= 192 MiB;
- idle CPU <= 3% of one core;
- imports <= 3 seconds;
- 100-task SQLite smoke <= 8 seconds;
- SQLite state <= 16 MiB;
- >= 512 MiB host memory headroom;
- no attributable swap growth.

After that passes, `scripts/local_stack_probe.py` measures the actual local-lite
runtime with initial targets:

- exactly 3 steady-state processes;
- total RSS <= 384 MiB;
- aggregate idle CPU <= 5% of one core;
- readiness <= 15 seconds;
- immediate runtime state <= 32 MiB.

The optional local Workflow tier has a separate initial budget: exactly 5 steady
processes, total RSS <= 640 MiB, idle CPU <= 8% of one core, readiness <= 25 seconds
and runtime state <= 64 MiB.

These are initial product budgets, not production SLOs. Failure means redesign the
tier; it is not permission to silently increase machine requirements.

## Consequences

Local deployment now has an implementation independent of the heavy acceptance
stack. The existing distributed/Workflow code is preserved rather than deleted.

The lightweight tier intentionally has fewer capabilities than the Workflow tier.
That feature boundary is a product trade-off for low idle cost, not a test shortcut.

Real target-machine execution is still required before claiming the local product is
economically viable.
