# Integrated application: installation, deployment and operations

## Lightweight local product

The default local product is now implemented separately as
`a2a_mq.local_lite:from_env` plus the `a2amq-local` launcher. Its steady-state
runtime is one Python application process, one native `daprd`, one native
`nats-server`, and SQLite files. It does not require Docker, PostgreSQL, Redis,
Placement, Scheduler, Vault or OTLP.

Install in an isolated venv with `python -m pip install -e '.[local]'`. The host
must already provide `daprd` and `nats-server`; the launcher never installs host
software. See `docs/LOCAL_RUNTIME.md` for the exact prepare/run and resource-gate
workflow.

Local-lite reuses TaskLedger/TaskAPI, official A2A bindings, JWT authorization,
Dapr Pub/Sub, JetStream, outbox, cancellation, DLQ/replay and operations. It
supports immediate deterministic echo/upper execution. Workflow-only waits, timers,
approval/input and fan-out/fan-in are rejected explicitly and remain in the
role-separated Workflow profile.

The built-in local-lite launcher binds only loopback, disables Dapr metrics, leaves
Placement/Scheduler unconfigured, uses bounded single-node JetStream storage and
generates a separate durable DLQ consumer. A short-lived provisioning process may
run at startup; steady state remains three processes.

The optional local Workflow tier is also implemented as
`a2a_mq.local_workflow:from_env` plus `a2amq-local-workflow`. It combines API +
official Dapr Workflow in the same Python process/sidecar, adds native Placement and
Scheduler, and uses a separate Dapr `state.sqlite` actor store. Its steady target is
five processes and it still uses no PostgreSQL, Redis or Docker. Install it only
when needed with `.[local,local-workflow]`; see `docs/LOCAL_RUNTIME.md`.

## Scope and entry points

`a2a_mq.application:from_env` assembles the existing SQL, Workflow, A2A and security
modules with new MCP, operator, recovery, telemetry and deployment modules.

| Role | Responsibilities | Durable scope |
| --- | --- | --- |
| api | Official A2A HTTP interfaces, Agent Card, optional MCP, operator API, command-outbox dispatch and abandoned-admission repair | Same tenant/consumer as its workflow workers |
| workflow | Official Dapr runtime, bounded reference activities, control dispatch, outcome relay and terminal reconciliation | Replicas share the logical consumer; physical workers do not change task IDs |
| observer | Independent snapshot projection, query, quota and retirement policy | Different logical consumers for separate observers |
| operator | Durable DLQ intake and original-identity replay relay, operator API | Same task scope when inspecting/replaying that pool |

The configured workflow target is an agent route. The existing transport/domain
still support agent or skill routes. The integrated workflow is a model-free
reference implementation, not an arbitrary user-code or LLM execution service.

## Distributed acceptance dependencies and Compose

Use Python 3.12 or 3.13 and install the direct pins:

```sh
python -m pip install -e '.[dev,bridge,workflow,postgres,security,mcp,observability,deployment]'
python -m pip check
```

The new deployment files are `deploy/Dockerfile.full` and
`deploy/Dockerfile.sidecar`. Generate a NEW private directory, outside the repo:

```sh
ROOT="$(mktemp -d /tmp/a2amq.XXXXXXXX)"
python -m a2a_mq.deployment compose --broker jetstream --source "$PWD" --output "$ROOT/stack"
docker compose -f "$ROOT/stack/compose.yaml" up --build -d
```

RabbitMQ uses the same application command with `--broker rabbitmq`. Its bootstrap
predeclares durable quorum queues/exchanges and policy bindings before publication.
JetStream bootstrap creates a limits-retention stream and verifies existing
configuration rather than overwriting a conflict. Consumers/role scopes are distinct.
RabbitMQ's archive queue dead-letters to a separate parking queue, not back to itself.

The **distributed disposable acceptance profile** includes PostgreSQL, Redis workflow state, broker,
provisioner, Dapr placement/scheduler, API, two workflow workers, two observers,
operator and HTTPS gateway. It is intentionally not the lightweight local product. External address: `https://localhost:18443`. Trust
`$ROOT/stack/secrets/tls.crt`. Host application ports are loopback-only; internal
broker/database traffic in this disposable profile is not a production TLS claim.

Fresh JWT signing keys, passwords and distinct app/API tokens are generated.
The directory includes `.gitignore` and private files. The generated development
issuer is `https://local-idp.example`; API audience is `https://localhost:18443`,
and MCP audience is `https://localhost:18443/mcp`. Local client token issuance for
acceptance uses `a2a_mq.live_probe.token`; it is **not a production identity provider**.
Production tokens must come from the configured, authorized issuer.

To inspect the public card without a bearer token:

```sh
curl --fail --cacert "$ROOT/stack/secrets/tls.crt" \
  https://localhost:18443/.well-known/agent-card.json
```

Use an official A2A client, rather than guessing JSON-RPC or protobuf field shapes.
The live harness and `tests/test_native_adapters_new.py` contain exact client examples.
The legacy publisher CLI is for its original transport demo; its unregistered
commands are not automatically workflow admissions.

Stop this particular profile with `docker compose -f "$ROOT/stack/compose.yaml" down`.
Add `--volumes` only when intentionally discarding **this disposable profile's** data.
Keep production resources separate. Each live acceptance run uses a unique project.

## Environment contract

Required for every role: `A2A_ROLE`, `APP_API_TOKEN_FILE`, `DAPR_API_TOKEN_FILE`,
and exactly one of `A2A_SQLITE_PATH` / `A2A_POSTGRES_DSN_FILE`.

Defaults: tenant `demo`, logical consumer `tasks`, workflow source `workflow-worker`,
Dapr component `bus`, colocated HTTP endpoint `http://127.0.0.1:3500`, gRPC port 50001.
Overrides use `A2A_TENANT`, `A2A_CONSUMER`, `A2A_SOURCE`, `A2A_COMPONENT`,
`DAPR_HTTP_ENDPOINT` and `DAPR_GRPC_PORT`.

API/operator roles require `A2A_PUBLIC_URL`, `A2A_JWT_ISSUER`,
`A2A_JWT_KEYS_FILE`. The key file maps trusted `kid` values to PEM public keys.
The issuer's exact HTTPS path/trailing slash is preserved. JWT algorithms and keys
are pinned; token-provided URLs/keys are not trusted. Permissions are rechecked
against authenticated subject/tenant, not payload fields.

`A2A_ENABLE_MCP=1` enables six native SDK tools on `/mcp`: submit, get, list,
continue, cancel and approve. The MCP audience differs from the A2A audience.
Tools verify the current token for every call rather than caching a session principal.

`A2A_DEVELOPMENT=1` explicitly permits single-host SQLite or insecure local
PostgreSQL. Otherwise PostgreSQL with `sslmode=verify-full` and `sslrootcert` is
required. SQLite files belong on a local filesystem, never as shared multi-node state.
App/token/DSN files are bounded, regular, non-symlink files with owner-only secret
permissions. Kubernetes manifests copy projected credentials to private files;
rotation of those snapshots and Dapr tokens requires a coordinated pod rollout.

Bounds: `A2A_MAX_ACTIVE` (default 1000), `A2A_MAX_RECORDS` (100000),
`A2A_CONCURRENCY` (8), `A2A_DATABASE_CAPACITY` (16). Other timeout/relay tunables are
explicit `Settings` constructor options, not undocumented environment variables.

Optional: `A2A_CRYPTO_CONFIG`, `A2A_OTLP_ENDPOINT`, `A2A_OTLP_CA_FILE`.
For RabbitMQ the generator sets `A2A_SUBSCRIPTION_QUEUE`; it installs quorum queue
metadata at the subscription, not merely an unrelated component default.

## Optional Vault transport protection

Set the same route/receiver contract on senders and receivers. Example config:

```json
{
  "vaultUrl": "https://vault.internal.example",
  "tokenFile": "/run/private/vault-token",
  "caFile": "/run/trust/vault-ca.pem",
  "mount": "transit",
  "namespace": "",
  "command": {
    "receiver": "workflow-workers", "active": "commands",
    "keys": [{"id": "commands", "name": "a2amq-commands", "writeVersion": 2, "minReadVersion": 1}]
  },
  "event": {
    "receiver": "task-observers", "active": "events",
    "keys": [{"id": "events", "name": "a2amq-events", "writeVersion": 2, "minReadVersion": 1}]
  }
}
```

Vault owns authentication, Transit keys and per-role encrypt/decrypt policy.
Provision them with least privilege; use dedicated tenant/receiver key names.
Keep read versions long enough to decrypt queued/replayed messages during rotation.
Raising `minReadVersion` or removing a key is an intentional rejection of old data.
Retain correct unwrap rights for authorized replay operators; do not grant root tokens.

Library AES-256-GCM uses fresh data keys/nonces and authenticated routing metadata.
The wrapped/sealed publication bytes are stored before sending, so retries reuse
ciphertext. Plaintext on an encrypted route is rejected. SQL and native Workflow
history are **not** encrypted by this transport feature; use infrastructure disk/
backup encryption and access control for at-rest requirements.

## Operations, replay and retirement

Operator JWTs require appropriate `a2a:operate`, `a2a:replay`, `a2a:approve` or
admin permissions; ordinary send/read permissions do not grant these operations.
Operator HTTP responses are non-cacheable; diagnostics omit task/secret text.

```sh
python -m a2a_mq.admin status --url https://api.example \
  --token-file /run/private/operator.jwt --ca-file /run/trust/ca.pem
python -m a2a_mq.admin retire --url https://api.example \
  --token-file /run/private/operator.jwt --retention-seconds 604800
python -m a2a_mq.admin deadletters --url https://api.example \
  --token-file /run/private/operator.jwt
python -m a2a_mq.admin replay --url https://api.example \
  --token-file /run/private/operator.jwt --record-id RECORD --request-id STABLE_REQUEST_ID
```

Retirement is a dry run by default; `--execute` opts into eligible task deletion.
A task must be terminal, its native Workflow terminal-confirmed, controls delivered
and all result publications verified/confirmed. Compact admission identities remain.
`projection-retire` operates from an observer role's environment and preserves
projection tombstones and event identities. Quotas count retained identities too:
cleanup is not permission to erase deduplication history. Capacity exhaustion is
explicit back-pressure, not silent forgetting. DLQ archives are bounded and are
not automatically discarded when a replay succeeds.

DLQ replay rejects truncated, invalid, expired, wrong-route or changed-content
records. It retains the original message ID and bytes. A new task is submitted
through the task API, not by modifying archived identity. The relay verifies the
archive again before publication. A failed local mark can yield identical repeats.

`recovery.purge_history` is an administrator API for native Dapr history removal
**after SQL retirement**. Call it against the workflow application's sidecar; it
checks exact Workflow name/input digest and terminal runtime state. SQL retirement
identities are preserved. It does not terminate active work or cancel activities.

## Backup and restore

SQLite commands operate on the entire file (all scopes), not one tenant:

```sh
python -m a2a_mq.admin backup /state/a2amq.db /backups/new.db
python -m a2a_mq.admin restore /backups/new.db /state/restored-new.db --sha256 EXPECTED_SHA256
```

WAL-aware online backup produces a temporary checked file, fsyncs it, and installs
it with no-overwrite semantics. Restore verifies the separately stored SHA-256 and
schema before installing a new destination. Stop writers before replacing a live
DB file. Atomic installation, not application-level consistency across external
broker/Workflow history, is the guarantee.

PostgreSQL uses native client tools through a service file (private, no inline
password argument):

```sh
python -m a2a_mq.admin pg-backup /backups/new.dump --service-file /run/private/pg-service.conf
python -m a2a_mq.admin pg-restore /backups/new.dump --service-file /run/private/pg-service.conf \
  --sha256 EXPECTED_SHA256 --confirm-database a2amq_restore
```

The restore target must be a distinct, empty, disposable database; the wrapper
checks its exact database name and refuses an unexpected/non-empty target. It does
not provide application-consistent Workflow/broker backup by itself. Production DR
requires coordinated database, Workflow state and broker retention/restore policy.

## Production rendering

Render Kubernetes resources to a new file:

```sh
python -m a2a_mq.deployment kubernetes --config /path/production.json --output /path/new-manifests.json
```

Required config keys: `namespace`, digest-pinned `image`, `broker`, verified-TLS
`brokerEndpoint`, HTTPS `publicUrl`, exact HTTPS `issuer`, TLS `redisHost`, and
explicit non-global `egressCIDRs`. Optional: `cryptoConfigMap`, `otlpEndpoint`.
Namespace identifies tenant in this renderer. Generate broker bootstrap/ACL plans
for a dedicated tenant account/vhost before starting applications.

Prerequisites remain administrator-owned: secured Dapr control plane with mTLS,
placement/scheduler, ingress controller, existing HA PostgreSQL and Workflow state,
verified broker certificates/authentication, role-scoped broker credentials/ACLs,
CA ConfigMaps, identity keys and Kubernetes Secrets. Inspect generated Secret/key
references to match your infrastructure. No certificates/passwords are fabricated
for a production manifest. Validate it against the target cluster before applying.

Resources include per-role scopes, workload service accounts/least-privilege secret
reads, image digest enforcement, resource limits, topology spreading, selected
replicas/disruption budgets, read-only root filesystems and network restrictions.
These are application resources, not a newly implemented HA database/control plane.
A rendered manifest is not proof of server-side ACLs, TLS rotation or live failover.

## Tests and full gate

Use a new external evidence directory for each run. Required native test selection
fails when imports/services are absent; the runner records unavailable prerequisites.

```sh
python -m a2a_mq.acceptance --mode local --root "$PWD" --output /tmp/new-local-evidence
python -m a2a_mq.acceptance --mode sdk --root "$PWD" --output /tmp/new-sdk-evidence
```

For the real PostgreSQL gate set `A2AMQ_POSTGRES_DSN_FILE` to a private verified-TLS
disposable test DSN. The new test uses a fresh random tenant and deletes only its rows. The runner
also supplies the legacy `A2A_TEST_POSTGRES_DSN` variable from that same file
when the concurrent gate is present; conflicting preexisting DSNs fail closed.
The older tests may retain their isolated random consumers in this disposable database.
For Vault set `A2AMQ_VAULT_TEST_CONFIG_FILE` to a JSON object containing `url`,
`tokenFile`, `caFile`, `keyName`, `tenant`, `receiver`, optional `mount`/`namespace`.
The dedicated Transit test key must already have versions 1 and 2. The test does
not rotate/delete production keys or alter Vault policy.

```sh
python -m a2a_mq.acceptance --mode postgres --root "$PWD" --output /tmp/new-pg-evidence
python -m a2a_mq.acceptance --mode vault --root "$PWD" --output /tmp/new-vault-evidence
python -m a2a_mq.acceptance --mode full --root "$PWD" --output /tmp/new-full-evidence \
  --allow-disposable-containers --timeout 1200
```

Full mode includes local, official SDK, real PostgreSQL/Vault, dependency checks,
Ruff, mypy and two isolated real-broker runs. The live harness exercises offline
submission, actual publication, competition/fan-out, process restart, input/auth,
fan-in, cancellation, standard clients, streaming, MCP, DLQ/replay and denials.
Connection failures cannot satisfy negative authorization checks. Native/live
scenarios were written but not executed in this continuation's environment.

The full harness creates private fixture directories containing generated secrets;
archive only verdicts and reviewed/sanitized logs, never blindly ZIP the full output.
`FULL_PASS` is an implementation gate, not signed independent conformance or
production certification. The existing frozen-SHA independent reviewer is a
separate, narrower procedure; this runner records provenance but does not replace it.

`python -m a2a_mq.performance --help` describes an opt-in official-client benchmark
against a disposable reference target. It measures admission latency, not business
completion, and does not assert the unmeasured historical latency/SLA targets.


## Concurrent branch compatibility

During this implementation the remote branch advanced from `617ecbab...` to
`79b4de69f2460479e691be2eaa67a41dfdee2e4f` with additional production/operations
modules. The integration patch preserves those additions and its CI/binding work.
Our stricter retirement and archive helpers use `maintenance.py` and
`sqlite_archive.py`; they do not replace the remote `operations.py` / `backup.py`.
New DLQ/replay tables are `a2amq_delivery_dlq` / `a2amq_delivery_replays`, avoiding
collision with the concurrently introduced, differently shaped `a2amq_dlq` table.
There is no inferred archive migration between the two formats.

Existing `production.py`, `production_env.py`, their deploy profiles, MCP adapter
and operator interfaces remain available and unchanged. The new `application`
profile is explicit and opt-in. Choose one orchestration/deployment profile for
a logical pool; do not independently start both against the same broker/workflow
identities. Reusing helper code does not authorize mixed-profile duplicate workers.

This continuation tested the new profile's selected contracts, not the concurrently
added production profile. Integration compatibility is guarded by immutable file
preimages and non-overlapping additions, not claimed as a full regression pass.
