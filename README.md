# A2A-MQ

Experimental A2A task messaging over **Dapr Pub/Sub + NATS JetStream**. This is a
reference implementation, not a new broker, certified A2A binding, or production
agent platform.

## Current source status

The branch now includes a consumer-scoped **SQLite inbox/effect/task/outbox**, a
bounded asynchronous worker and relay, and an authenticated durable reference
service. These are incremental implementations on the accessible phase-2 baseline;
the original later MVP snapshot has **not** been recovered. Version remains
**0.1.0**, not the intended unrecovered 0.3.1 handoff.

Read [current status](docs/STATUS.md), [durable service usage](docs/DURABLE_SERVICE.md),
[executed evidence](docs/validation/durable-service-20260917.md) and
[independent review](TEST_MVP.md). The selected combined phase-3 suite passed
**184 tests**, including real SQLite, cancellation and a real loopback HTTP/process
restart scenario with a sidecar fixture. This is not actual broker validation or
full-MVP/production acceptance.

## Contract and implementation boundary

Stable agent/skill routes identify logical worker pools; independent observers use
separate consumer namespaces. SQLite keys include tenant and logical consumer.
The durable echo worker commits its reference SQL effect, complete terminal Task,
result outbox and completed inbox in one transaction before callback success.
A busy claim stays retryable; a completed duplicate reuses its verified result.

The outbox relay publishes at least once. An unsuccessful/ambiguous publication or
failed local mark keeps the event pending, allowing identical bytes to be replayed
after restart. Independent snapshot projections converge under reordered events.
External effects still require provider idempotency or reconciliation; the SQL
echo fixture is not a generic exactly-once execution guarantee.

The service provides authenticated, bounded internal get/list queries for one
fixed tenant/consumer scope. These are not the public A2A HTTP bridge. PostgreSQL,
Workflow orchestration, production identity/TLS, encrypted envelopes and live broker
acceptance remain separate roadmap gates. The app token is not tenant identity.

## Develop and test

```sh
python -m venv .venv
. .venv/bin/activate
python -m pip install -e '.[dev]'
make check
```

`make check` is a developer convenience and may report missing optional tests as
skips. It is not the acceptance gate. [TEST_MVP.md](TEST_MVP.md) describes strict
local review; [phase-3 evidence](docs/validation/durable-service-20260917.md) includes
the exact selected-suite command. CI additionally selects upstream SDK tests.
A workflow file is not proof that hosted execution succeeded.

Direct dependencies are pinned, not transitively hash-locked. Python 3.12/3.13 are
CI targets; this continuation executed Python 3.13.5. An offline wheel build and
installation smoke passed using preinstalled third-party dependencies. Fresh
network dependency resolution, Ruff/mypy and hosted CI remain separate checks.

## Choose the correct entry point

For the durable service use `a2a_mq.service:from_env` with explicit
`A2A_SQLITE_PATH` and `APP_API_TOKEN`; follow [the startup guide](docs/DURABLE_SERVICE.md).
It starts the app, not the sidecar or broker. Keep this reference on a trusted local
host and preserve its database on restart.

The existing `make up` / `a2a_mq.demo:create_app` remains the **legacy volatile
transport demo** with a disposable Docker deployment. It has not been silently
converted into a production or durable-service stack. Consult [deployment](deploy/README.md)
before its destructive opt-in live test. The publisher CLI preserves command IDs:
`python -m a2a_mq --text 'hello' --save /tmp/a2a-command.json`, then
`--retry-file /tmp/a2a-command.json` for an ambiguous retry.

## Design and roadmap

[Whitepaper](docs/WHITEPAPER.md), [foundation evidence](docs/FOUNDATION_RESEARCH.md),
[architecture](docs/ARCHITECTURE.md), [protocol](docs/PROTOCOL.md),
[workflows](docs/WORKFLOWS.md), [requirements](docs/REQUIREMENTS.md),
[implementation plan](docs/IMPLEMENTATION_PLAN.md), [test plan](docs/TEST_PLAN.md),
[security](docs/SECURITY.md) and [operations](docs/OPERATIONS.md) describe the target.
Historical [phase records](docs/VALIDATION.md) are not final-SHA certificates.
No paid model or model API key is needed for the deterministic fixtures.
