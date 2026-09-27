# Durable service validation - 2026-09-17

## Source provenance

Starting commit: `a2ffbf787150563652085de934a2d67b7ae13b28` (consumer-scoped
SQLite reference). Recovery hardening: `9dcd38e0b0aee8936d9089490c2d51e4a18a1479`.
Runtime integration: `4e68deeb9d0c2c06722cd070dbd07a736533386a`.
Service code and tests: `64694be6f69f0033cb20cd3a32d762d37b901c24`.
Documentation-only follow-ups do not change those tested source bytes.

Files were materialized from authorized connector reads and checked against Git
blob hashes. All 11 files in the package directory match the service commit's
source inventory. This was a selected-file execution workspace, not a full Git
checkout or the independent frozen-SHA review. The whole repository suite and
`tools/check.py` were not executed in this continuation.

## Executed gates

| Gate | Actual result |
| --- | --- |
| Reproduce original SQLite suite with real domain/schema | 103 passed |
| New recovery checks before fix | 18 failed: 12 integrity regressions and 6 new pagination/API checks |
| SQLite plus recovery after fix | 121 passed |
| SQLite, recovery and asynchronous runtime | 158 passed |
| Service contract tests alone | 25 passed |
| Real HTTP/process restart scenario alone | 1 passed |
| Final combined selected suite | **184 passed, 0 failures, 0 errors, 0 skips; process exit 0** |
| Existing strict JUnit checker on final report | 184/0/0/0 accepted |
| Offline wheel build and target installation | Passed |
| Installed wheel smoke outside source directory | Passed: 11 package files, imports, resources, auth, durable callback, duplicate, query and restart |

Counts are overlapping stages, not additive. The final 184 comprises 103 storage,
18 recovery, 37 runtime, 25 service-contract and 1 real HTTP scenario.

The final supervised command was:

```sh
PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 PYTHONDONTWRITEBYTECODE=1 A2AMQ_NO_LLM_TOKENS=1 \
python -m pytest -p pytest_asyncio.plugin \
  tests/test_storage.py tests/test_storage_recovery.py tests/test_runtime.py \
  tests/test_service.py tests/test_service_http.py -q -W error \
  --junitxml=/tmp/a2amq-phase3.junit.xml
python scripts/assert_junit_complete.py /tmp/a2amq-phase3.junit.xml
```

Only pytest-asyncio was explicitly loaded, excluding unrelated ambient pytest
plugins. Python was 3.13.5, pytest 9.0.2, pytest-asyncio 1.3.0, jsonschema 4.26.0,
httpx 0.28.1, FastAPI 0.128.2, Uvicorn 0.48.0 and setuptools 82.0.1. Test duration
was 30.54 seconds; the supervised process returned 0 after 33.22 seconds.
Earlier combined attempts were interrupted by the execution tool and are not
counted as completed passes; their diagnostic output remains in the audit bundle.

## What the HTTP scenario proves

The scenario starts real Uvicorn child processes and uses real loopback sockets,
the actual DaprPublisher and a real SQLite database. A local HTTP sidecar fixture
first returns 503, then 204. The worker commits while publication is unavailable,
restarts, drains the saved outbox, and accepts a duplicate without repeating the
SQL effect. Failed and successful publication bodies are byte-identical. Two
observer processes receive the captured event via explicit callback requests and
retain independent projections. Final SQL counts are one effect, three tasks and
three inbox rows across those consumer scopes.

The sidecar is a fixture, **not Dapr or NATS**. Observer deliveries are injected by
the test, not a demonstrated broker fan-out. Actual broker retention, competition,
redelivery, DLQ routing and sidecar authentication remain separate live gates.

## Package experiment

Built with `pip wheel --no-index --no-deps --no-build-isolation .` and installed
with `pip install --no-index --no-deps --target ...`. Smoke ran using `python -I`
outside the source directory; package imports resolved to the installation target,
and packaged files matched source bytes. Third-party dependencies came from the
preinstalled environment. This is not a fresh network-resolved dependency install,
a full-MVP wheel/migration test or an isolated-build dependency test.

Wheel: `a2a_mq-0.1.0-py3-none-any.whl`.
SHA-256: `6e0a9f227545594ed8c2a1abbeab1fc194dcf7e305f0e4f2a204e70fd3ff7b43`.

## Hosted and remaining gates

Hosted Actions run `35194462365` for service commit `64694be6...` concluded failure.
Both Python 3.12/3.13 jobs returned no step summaries or log URL. The checks expose
an annotation count, but the connector does not support fetching those annotations.
The underlying hosted failure cause is therefore not established. No hosted pass
is claimed, and a local run does not replace that gate.

Ruff/mypy, the complete existing regression suite, Python 3.12 execution, fresh
locked dependencies, PostgreSQL, actual Dapr/brokers, Workflow, A2A interoperability,
MCP, Vault/KMS and production acceptance were not established here. The original
later snapshot remains unrecovered; this is incremental implementation against the
available baseline. `mvpStatus=INCOMPLETE`; `productionCertified=false`.
