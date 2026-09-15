# A2A-MQ

Experimental A2A task messaging over **Dapr Pub/Sub + NATS JetStream**. This is a
reference implementation, not a new broker, certified A2A binding, or production
agent platform. Documentation distinguishes executable phases from future work.

## Contract

Multiple orchestrators publish commands to stable agent/skill routes. Replicas of
one worker pool compete; independent observers receive separate event copies.
All agent commands and events travel through the bus. Administrative task queries
use a durable projection, not a broker scan. An ordinary A2A HTTP bridge is phase 5.

Delivery is **at least once**. A durable claim alone does not mean the task ran.
The reference transaction commits an idempotent local effect, task outcome and
outbox together; only then may the command be acknowledged. External side effects
need a destination idempotency key or reconciliation, not just our inbox table.

## Develop

```sh
python -m venv .venv
. .venv/bin/activate
python -m pip install -e '.[dev]'
make check
```

The repository pins direct dependencies; it does not claim a transitive hash lock.
Python 3.12/3.13 are CI targets. Runtime integration prerequisites and actual test
results are recorded separately in [validation](docs/VALIDATION.md).

## Read in order

1. [Whitepaper](docs/WHITEPAPER.md) and [foundation evidence](docs/FOUNDATION_RESEARCH.md).
2. [Architecture](docs/ARCHITECTURE.md), [protocol](docs/PROTOCOL.md), [workflows](docs/WORKFLOWS.md).
3. [Requirements](docs/REQUIREMENTS.md), [implementation plan](docs/IMPLEMENTATION_PLAN.md), [tests](docs/TEST_PLAN.md).
4. [Security](docs/SECURITY.md), [operations](docs/OPERATIONS.md), [deployment](deploy/README.md).

The first slice uses a deterministic echo worker: no paid model or API key is
needed. Dapr Workflow/Agents integration, cancellation, continuation, authentication
hardening and payload encryption remain explicit later gates. Unsupported
features must fail closed rather than silently pretending to work.
