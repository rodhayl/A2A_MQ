# A2A-MQ

Experimental durable A2A task messaging over **Dapr Pub/Sub + Dapr Workflow**,
with SQLite and PostgreSQL adapters, native A2A HTTP interfaces, optional MCP,
JWT authorization, optional Vault-backed transport encryption, and operations.

## Implemented entry point

Use **`a2a_mq.application:from_env`** for the integrated application. Its roles are
`api`, `workflow`, `observer` and `operator`. The API uses the official A2A SDK for
JSON-RPC, HTTP+JSON, Agent Cards and streaming. Dapr owns durable orchestration;
this project does not implement a second broker or workflow engine.

Existing `production.py` / `production_env.py` profiles are preserved; the new
application profile is explicit and opt-in. The older `service:from_env`,
`demo:create_app` and `make up` remain separate
historical/reference entry points. They do not start the integrated stack.

See [installation and operation](docs/IMPLEMENTED_STACK.md),
[requirement coverage](docs/IMPLEMENTATION_COVERAGE.md),
[binding decision](docs/adr/0010-native-bindings-and-operations.md) and
[execution evidence](docs/validation/integrated-implementation-20260917.md).
Older status and phase records describe their historical commits, not this source.

## Start a fresh disposable stack

```sh
python3 -m venv .venv
. .venv/bin/activate
python -m pip install -e '.[dev,bridge,workflow,postgres,security,mcp,observability,deployment]'
python -m pip check
ROOT="$(mktemp -d /tmp/a2amq.XXXXXXXX)"
python -m a2a_mq.deployment compose --broker jetstream \
  --source "$PWD" --output "$ROOT/stack"
docker compose -f "$ROOT/stack/compose.yaml" up --build -d
```

Select `--broker rabbitmq` in a different fresh directory for the other backend.
The profiles bind host ports to loopback and generate fresh local credentials and
TLS material. They are **disposable development profiles**, not production HA.
Keep their private directory; never commit it or include it in an evidence ZIP.
The external API is `https://localhost:18443` with the generated trust certificate.
The guide explains authentication, lifecycle, infrastructure prerequisites and
safe cleanup. Docker and network access to pinned packages/images are required.

## Correctness boundaries

Each SQL key includes tenant and logical consumer. Stable message IDs, hashes,
leases/fences, atomic outcomes/outbox and monotonic snapshots protect retries.
Observers keep compact retirement identities so old deliveries cannot recreate
removed tasks. Replay retains the original identity and content; it is not a way
to manufacture new business intent. An encrypted route rejects plaintext replay.

Task pauses, continuation, approval, cancellation, timers and fan-out/fan-in use
Dapr Workflow. The supplied activities are model-free text transformations.
Arbitrary external effects require destination idempotency/reconciliation; there
is no general exactly-once promise. Payload encryption protects transport, not
SQL or workflow history at rest.

## Acceptance is separate from implementation

```sh
python -m a2a_mq.acceptance --mode local --root "$PWD" \
  --output "$(mktemp -d /tmp/a2amq-review.XXXXXXXX)/local"
```

Additional explicit modes are `sdk`, `postgres`, `vault`, and `full`. Full mode
requires configured real PostgreSQL/Vault and opt-in disposable tests on **both**
brokers, plus dependency/lint/type checks. Missing prerequisites or skipped tests
cannot produce `FULL_PASS`. Read the guide before enabling containers.

This continuation executed **170 new local tests**, not the complete historical
repository suite. Native A2A/MCP/Dapr clients, real PostgreSQL/Vault/broker tests,
container builds and production deployment still require execution in a suitable
environment. Direct dependencies are pinned; transitive hash locking and supply-
chain audits remain release work. No full-MVP, production or upstream binding
certification is claimed. Package version remains **0.1.0**.
