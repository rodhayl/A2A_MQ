# A2A-MQ

Experimental A2A task messaging over **Dapr Pub/Sub + NATS JetStream**. This is a
reference implementation, not a new broker, certified A2A binding, or production
agent platform.

## Current source status

This branch contains the phase-2 source plus focused transport, JSON and validation
hardening. The later MVP snapshot has **not** been recovered. Package version is
still **0.1.0**; the intended 0.3.1 handoff is not a published implementation.
See [current status](docs/STATUS.md), [executed evidence](docs/validation/hardening-20260916.md)
and [independent review](TEST_MVP.md). A successful local test run is not full-MVP
acceptance, broker validation, or production certification.

## Contract and implementation boundary

The target architecture uses stable agent/skill routes: replicas of one worker
pool compete; independent observers receive separate event copies. Administrative
queries should use a durable projection, not a broker scan. Durable SQL processing,
the ordinary A2A HTTP bridge, workflow orchestration and their integration gates
remain absent from this source tree.

Delivery is **at least once**. The current demo is volatile and does not provide a
durable inbox/outbox. The planned transaction must commit a reference local effect,
task outcome and outbox together before acknowledgement. External side effects
require destination idempotency or reconciliation; no exactly-once claim is made.

Implemented hardening makes deterministic echo event bytes stable across retries,
bounds cooperative HTTP/callback lifecycles, validates CloudEvent source/type
consistency, rejects duplicate app-token headers and checks JUnit evidence strictly.
The app token is not a substitute for tenant authorization or workload identity.

## Develop

```sh
python -m venv .venv
. .venv/bin/activate
python -m pip install -e '.[dev]'
make check
```

`make check` is a developer convenience and may report missing optional tests as
skips. It is not the acceptance gate. Use [TEST_MVP.md](TEST_MVP.md) for strict
local review. The CI contract includes upstream SDK tests and rejects skipped
selected tests; live/PostgreSQL scenarios remain separate unverified gates.

Direct dependencies are pinned, not transitively hash-locked. Python 3.12/3.13
are CI targets. Hosted runner success, clean dependency resolution, lint/types
and package installation still require executed evidence.

## Read in order

1. [Current status](docs/STATUS.md) and [independent review](TEST_MVP.md).
2. [Whitepaper](docs/WHITEPAPER.md) and [foundation evidence](docs/FOUNDATION_RESEARCH.md).
3. [Architecture](docs/ARCHITECTURE.md), [protocol](docs/PROTOCOL.md), [workflows](docs/WORKFLOWS.md).
4. [Requirements](docs/REQUIREMENTS.md), [implementation plan](docs/IMPLEMENTATION_PLAN.md), [tests](docs/TEST_PLAN.md).
5. [Security](docs/SECURITY.md), [operations](docs/OPERATIONS.md), [deployment](deploy/README.md).

Architecture/requirements documents describe the target; phase records in
[validation](docs/VALIDATION.md) are historical and are not final-SHA certificates.
Current implementation and evidence limits are listed in the status document.
No paid model or model API key is needed for the deterministic fixtures.

## Local transport demo

With Docker Compose installed, `make up` generates and starts the disposable demo:
a retained stream, producer sidecar, two workers and two independent observers.
Publish with `python -m a2a_mq --text 'hello' --save /tmp/a2a-command.json`; retry an
ambiguous send with `--retry-file /tmp/a2a-command.json`. Read deployment instructions
before the destructive, opt-in live test. This fixture is not a production profile.
