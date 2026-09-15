# Phase 2 validation

Executed `python tools/check.py`, `python deploy/generate.py` and full local
`python -m pytest -q`: **156 passed, 3 skipped**. Two skips require the optional
A2A SDK; one requires the real disposable Dapr/JetStream Docker fixture.

The transport tests exercise status classification, stable CloudEvent IDs/bytes,
API tokens, malformed/wrong-route callbacks, confirmed DLQ handoff and failed-DLQ
retry, metadata-only logs, request limits, readiness and bounded concurrency.
A real loopback HTTP server plus subprocess CLI test verifies save-before-send,
byte-identical retries and refusal to overwrite the saved command. This server is
an HTTP contract fixture, NOT Dapr or JetStream.

Deployment tests inspect generated role-specific durable/group identities, shared
worker state volume, independent observers, retained-stream policy, retry policy
and loopback host bindings. `docker compose` validation and live startup could not
run because Docker/images are unavailable. Image tags are pinned baselines, not
locally pulled or certified images. Broker competition/fan-out/offline gates remain
unexecuted. At this checkpoint the sample worker/observer is deliberately volatile;
phase 3 replaces it with transactional SQL and outbox processing.

No completed GitHub CI status was returned when checked. Local Ruff/mypy remain
unavailable; syntax/import/tests have run, not a substitute lint/type claim.
