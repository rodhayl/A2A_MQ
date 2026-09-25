# Single-host durable reference service

The entry point is `a2a_mq.service:from_env`, not the legacy volatile
`a2a_mq.demo:create_app`. The existing `make up` deployment still launches that
legacy transport demo; it does not silently enable this new service.

## Implemented behavior

A worker accepts one configured tenant and agent/skill route. It claims a command,
computes the deterministic echo fixture, and atomically commits the reference SQL
effect, complete terminal Task snapshot, result outbox and completed inbox record.
A verified completed redelivery reuses the stored result without recomputing the
effect. A busy claim returns RETRY. Only the terminal snapshot (sequence 3) is
published; intermediate fixture states are not durable workflow progress.

The relay runs during the application lifespan. An unsuccessful or ambiguous
publish remains pending; restarting the service resumes the outbox. Publication
followed by a failed local mark can publish identical bytes again. Snapshot
observers independently deduplicate and project those events.

All four stores are keyed by tenant and logical consumer. Replicas share a
consumer; independent observers use distinct consumers. Physical worker UUIDs
own leases but do not change logical event identities. The asynchronous SQLite
lane remains bounded even after a caller times out while a transaction is running.

## Local startup

Use the development dependencies from the repository on a trusted local host.
Keep the database on a local filesystem. Create its parent directory explicitly.
The token below is newly generated, not a hard-coded shared credential:

```sh
umask 077
mkdir -p "$HOME/.local/state/a2amq"
export APP_API_TOKEN="$(python -c 'import secrets; print(secrets.token_hex(32))')"
export A2A_SQLITE_PATH="$HOME/.local/state/a2amq/reference.sqlite3"
export A2A_TENANT=demo
export A2A_CONSUMER=worker-echo
export A2A_ROLE=worker
export A2A_TARGET_KIND=skill
export A2A_TARGET=echo
export DAPR_HTTP_ENDPOINT=http://127.0.0.1:3500
python -m uvicorn a2a_mq.service:from_env --factory \
  --host 127.0.0.1 --port 8080 --no-access-log
```

This starts the application, not a Dapr sidecar or broker. Live publishing requires
a separately configured Dapr `bus` component and broker. The application checks
callbacks' topic, component, tenant and envelope metadata. `/health/live` works
without the sidecar; `/health/ready` also checks sidecar and database availability.

The sidecar must send the same application credential in `dapr-api-token` when
calling the app. The app reads it from `APP_API_TOKEN`; Dapr's application-token
configuration is a separate sidecar deployment setting. `DAPR_API_TOKEN`, when
set, is the credential this application sends to the sidecar, not the app token.
Live sidecar token wiring is not established by the local HTTP fixture tests.

For another process on the same host, keep the same tenant/database/consumer to
join the same logical worker pool. Use a distinct app port and appropriate
sidecar subscription identity. Changing the consumer creates a different inbox
namespace and is not a way to rename existing persisted work.

For an observer, set `A2A_ROLE=observer` and a distinct `A2A_CONSUMER`, for example
`observer-a`. Its topic is `a2a.v1.demo.event.task`. A second independent observer
uses another consumer such as `observer-b`.

## Environment and programmatic settings

`APP_API_TOKEN` and `A2A_SQLITE_PATH` are required. Tokens must contain 16-256
visible ASCII characters. Default tenant is `demo`; default consumer is
`worker-echo`. `A2A_ROLE` supports `worker` and `observer`. Worker targets use
`A2A_TARGET_KIND=skill|agent`, `A2A_TARGET`, and optional `A2A_SOURCE`. Agent targets
must have a matching logical source. Skill targets default the source to consumer.

`create_app(store, bus, settings=ServiceSettings(...))` allows explicit settings:
concurrency 4, database capacity 8, callback/query timeout 30 seconds, relay interval
1 second and relay batch 50 by default. The public constructor validates their
bounds. These tunables are programmatic; the environment factory does not pretend
to parse additional undocumented variables.

## Internal queries

The application token protects callbacks, subscription discovery, readiness and
internal queries. These are trusted internal endpoints, not public A2A operations
or a multi-tenant identity/authorization system. Bind to loopback or a protected
local network; do not expose the service as a production tenant API.

From a separate shell using the same token:

```sh
curl --fail --silent --show-error \
  --header "dapr-api-token: $APP_API_TOKEN" \
  'http://127.0.0.1:8080/internal/tasks?limit=20&state=COMPLETED'
```

`GET /internal/tasks` accepts `context_id`, `state`, `after` and `limit` (1-200).
The response contains `tasks` and `nextCursor`. Pass `nextCursor` back as `after`.
The cursor is keyset pagination, not a transactional snapshot across pages.
`GET /internal/tasks/{task_id}` returns the full Task or 404. URL-encode opaque
identifiers. Responses are non-cacheable. Invalid filters return 422, unavailable
storage/query deadlines return 503, and invalid credentials return 401. Tenant
and consumer are fixed at app construction, never request-controlled query fields.

## Recovery and scope limits

Keep original envelopes unchanged for ambiguous retries. Completed result evidence
requires its corresponding outbox record; manually deleting outbox rows is not a
supported retention policy. Legacy unscoped schemas are rejected rather than
silently assigned an invented owner. Normal shutdown stops relay admission and
drains started SQL operations. Caller cancellation is not proof of rollback.

SQLite is a single-host reference backend. The SQL effect is a narrow echo fixture,
not exactly-once execution of external tools. PostgreSQL, live Dapr/JetStream,
RabbitMQ parity, long-running Workflow operations, the public A2A bridge, production
identity/TLS and coordinated retention still require separate implementation or
acceptance gates. See [executed evidence](validation/durable-service-20260917.md)
and [current status](STATUS.md).
