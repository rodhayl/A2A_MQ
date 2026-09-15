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
