# ADR 0011: local footprint is a product gate, not an afterthought

## Status

Accepted for the next local-product iteration.

## Context

The integrated implementation can render a full disposable stack with PostgreSQL,
a Workflow state backend, a broker, multiple Dapr-enabled roles, gateway and
observability pieces. That stack is useful for distributed acceptance, but it is
not an appropriate default for a resource-constrained machine whose primary job is
to act as a local A2A bridge.

The current role-based `a2a_mq.application` also assumes a colocated Dapr sidecar
for readiness and publication. Therefore the current integrated profile must not be
described as a proven lightweight local runtime merely because SQLite is available.

Current Dapr documentation supports self-hosted native execution without Docker.
Dapr's SQLite state component supports Actors and Workflow, so PostgreSQL/Redis are
not inherently required to exercise a single-host Workflow design. Workflow still
uses Actors/reminders and therefore carries Placement/Scheduler/runtime overhead.
That overhead must be measured rather than hidden inside a convenience stack.

## Decision

1. Resource viability is the first local-product acceptance gate.
2. The full Compose/Kubernetes topology is an infrastructure acceptance profile,
   not the default local runtime.
3. PostgreSQL, Redis, Vault, an OTLP collector and container orchestration are not
   prerequisites for the default local bridge.
4. Preserve Dapr + broker portability as the distributed architecture, but develop
   and measure lighter local tiers before adding more infrastructure.
5. The target tiers are:

   - **Core probe**: Python + SQLite only. It validates that our own code is cheap.
     It is diagnostic and does not claim MQ/Dapr parity.
   - **Local MQ Lite target**: one application process, SQLite application state,
     one native Dapr sidecar and one native single-node JetStream broker. Advanced
     Workflow features may be disabled unless the next tier is selected.
   - **Local Workflow target**: Local MQ Lite plus the minimum self-hosted
     Dapr actor/workflow services, using a local SQLite Dapr state store when
     compatible. Measure Placement/Scheduler separately. No PostgreSQL/Redis/Docker
     merely for convenience.
   - **Distributed/production**: role-separated services, shared PostgreSQL,
     production broker/state backends, HA/security/observability infrastructure.

6. A tier may advertise only the features it actually runs. A lightweight tier
   does not fake Workflow, broker parity, HA or production security.
7. If the core probe fails the initial resource budget, stop feature validation
   and treat local economics as a product blocker. If the core passes but the
   current integrated topology is too heavy, redesign the local composition rather
   than raising the host requirements silently.

## Initial measurement budget

The repository's probe uses intentionally conservative starting targets, not
production SLOs: 192 MiB maximum observed RSS for the Python core process, 3% of
one CPU core while idle, 3 seconds for core imports, 8 seconds for a 100-task
SQLite smoke, 16 MiB state footprint and 512 MiB host memory headroom before the
probe starts. The owner may tighten these after first measurements.

These thresholds apply to the core probe only. Each additional local tier must
report its incremental RSS/CPU/process/disk cost independently.

## Consequences

The current full local deployment remains valuable for acceptance, but it no longer
defines product viability. The next implementation decision should be based on
measured local footprint and should remove unnecessary daemon/process boundaries
from the default local composition before optimizing throughput or adding features.
