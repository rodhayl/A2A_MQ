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

## Low-resource local validation

For the owner's resource-constrained server, follow `PROMPT_AGENT_LOCAL_TEST.md`.
Use an isolated project-local venv and `requirements/local-validation.txt`, with
one inspected test file at a time. Keep generated state in ignored
`.local-validation/`. Do not install host packages, start containers/services,
install every optional extra, or launch a full-stack/load test on this machine.
These limits override the general test-command examples for this local task;
full infrastructure gates remain intact and unexecuted, not implicitly passed.
