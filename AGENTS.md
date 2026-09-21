# Implementation guidance

Read docs/IMPLEMENTATION_PLAN.md and docs/VALIDATION.md before changing scope.
Preserve requirement/scenario IDs and document contract changes with an ADR.
Run python tools/check.py and pytest; run ruff/mypy when installed. Never describe
skipped infrastructure tests as passing. Commit a tested phase separately.

Do not add a broker, workflow engine, crypto primitive or model dependency to the
core. Never ACK an unfinished claim or execute arbitrary external side effects
under the reference fixture's exactly-once assumptions. Preserve tenant/consumer
namespacing, immutable message IDs, fencing, transactional outbox and terminal guards.
Keep spec/envelope.schema.json and packaged src/a2a_mq/schema.json byte-identical.
The package is not a certified A2A binding or production service.

## Local-product priority

For the owner's resource-constrained server, local economics is a correctness gate.
Before broad testing, create the isolated venv and run the resource viability probe
defined in PROMPT_AGENT_LOCAL_TEST.md. If the Python+SQLite core exceeds its budget,
stop expensive validation and report the resource blocker. If the core passes,
review the mandatory process topology before starting any additional component.

Treat the full Docker/PostgreSQL/Redis/multi-role deployment as an acceptance fixture,
not the default local product. Follow ADR 0011. Do not make PostgreSQL, Redis, Vault,
OTLP collectors, Docker or Kubernetes prerequisites for the default local bridge.
A future local Dapr/broker tier must be measured incrementally and should prefer
native single-host components and SQLite where supported.

## Low-resource local validation

Use an isolated project-local venv and requirements/local-validation.txt, with one
inspected test file at a time. Keep generated state in ignored .local-validation/.
Do not install host packages, start containers/services, install every optional
extra, or launch a full-stack/load test on this machine. These limits override the
general test-command examples for this local task; full infrastructure gates remain
intact and unexecuted, not implicitly passed.
