# Getting started — v0.1.0-rc2

This guide takes a new user from a fresh checkout to a working **Local-lite**
A2A-MQ instance and the browser Queue Lab.

The repository-tested bootstrap path is Linux amd64/arm64. The Queue Lab web UI is
platform-neutral, but Windows native runtime provisioning is not currently
repository-certified.

## 1. What you are starting

Local-lite is the recommended default runtime:

1. one Python A2A-MQ process;
2. one native Dapr sidecar (`daprd`);
3. one native NATS Server with JetStream;
4. SQLite application state.

It does **not** require Docker, Kubernetes, PostgreSQL, Redis, Vault, Dapr Placement,
or Dapr Scheduler.

Local-lite is intentionally narrow: immediate deterministic Echo/Uppercase work is
supported. Durable waits, input continuation, approvals and timers belong to the
optional Workflow profile.

## 2. Prerequisites

You need:

- Linux x86_64/amd64 or arm64/aarch64 for the repository-pinned native bootstrap;
- Python **3.12 or newer**;
- Git;
- Internet access to GitHub release assets for the first native-tool bootstrap.

The RC2 validation used Ubuntu 24.04.5 LTS, x86_64 and Python 3.12.3.

## 3. Clone and install

```bash
git clone https://github.com/rodhayl/A2A_MQ.git
cd A2A_MQ

python3 -m venv .venv
. .venv/bin/activate

python -m pip install -U pip
python -m pip install -e '.[local]'
python -m pip check
```

Expected dependency result:

```text
No broken requirements found.
```

## 4. Bootstrap the exact native tools

For a reproducible local run, use the repository helper instead of installing system
packages:

```bash
python scripts/bootstrap_local_tools.py --output .local-validation/tools
```

This downloads the exact pinned Linux release archives:

- Dapr `daprd` **1.18.3**, all-components/default flavor;
- NATS Server **2.15.0**.

The helper verifies the expected archive size and SHA-256 before extracting the
expected executable.

Now verify that the extracted binaries are exactly the pinned archive members:

```bash
python scripts/check_local_native_versions.py \
  --daprd .local-validation/tools/bin/daprd \
  --nats-server .local-validation/tools/bin/nats-server \
  --bootstrap-manifest .local-validation/tools/manifest.json \
  --json .local-validation/native-versions.json
```

The command must print:

```text
PASS_NATIVE_VERSION
```

Add the private tools directory to your current shell:

```bash
export PATH="$PWD/.local-validation/tools/bin:$PATH"

daprd --version
nats-server --version
```

The bootstrap directory is local to the repository checkout and is not a system
installation.

## 5. Prepare a Local-lite profile

```bash
a2amq-local prepare .local-validation/local-lite
```

The profile contains owner-private local credentials and state:

- `profile.json` — profile mode, ports and runtime metadata;
- `app-api-token`, `dapr-api-token`, `nats-token` — local loopback credentials;
- `jwt-private.pem`, `jwt-keys.json` — local signing/verification material;
- `client.jwt` — generated local owner token;
- `components/` — Dapr Pub/Sub resources;
- `nats.conf` — local NATS configuration;
- `state/` — SQLite application state;
- `nats/` — JetStream file state.

Do not commit this profile directory. It contains secrets.

Before starting processes, run the read-only preflight:

```bash
a2amq-local doctor .local-validation/local-lite
```

It checks Python/extras, profile safety, required native binaries and pinned versions,
bootstrap provenance when detectable, startup ports, state writability and the
generated client identity. It does not install, repair or modify anything.

## 6. Run Local-lite

```bash
a2amq-local run .local-validation/local-lite
```

The launcher checks that its fixed loopback ports are free before starting children.
At steady state there are exactly three product processes:

- application: `127.0.0.1:18000`;
- Dapr HTTP: `127.0.0.1:3500`;
- Dapr gRPC: `127.0.0.1:50001`;
- Dapr internal gRPC: `127.0.0.1:50002`;
- NATS: `127.0.0.1:4222`.

The startup JSON includes a `labUrl`, normally:

```text
http://127.0.0.1:18000/lab/
```

Leave this terminal running while you use the service.

## 7. Confirm health

From another terminal:

```bash
curl -s http://127.0.0.1:18000/health/live
curl -s http://127.0.0.1:18000/health/ready
curl -s http://127.0.0.1:18000/.well-known/agent-card.json
```

Expected readiness:

```json
{"status":"ready"}
```

For one small automated happy-path check while Local-lite is running:

```bash
python scripts/smoke_local.py .local-validation/local-lite
```

The smoke intentionally checks only health/readiness, Agent Card, SendMessage,
task completion/GetTask and operations status. Queue Lab and pytest remain the
deeper diagnostic/validation surfaces.

## 8. Use Queue Lab

Open:

```text
http://127.0.0.1:18000/lab/
```

Click **Load file** and choose:

```text
.local-validation/local-lite/client.jwt
```

A useful first sequence is:

1. send an **Echo** task and inspect the completed Task;
2. send an **Uppercase** task and inspect the transformed artifact;
3. click **Pause simulated agent**;
4. choose **Burst / queue view**;
5. confirm the new tasks remain `SUBMITTED` and queued/submitted increases;
6. cancel one while paused;
7. click **Resume simulated agent**;
8. confirm the remaining tasks drain to completion and the canceled task stays
   `CANCELED`;
9. try **Enqueue + A2A stream**;
10. inspect **Dead letters** with **Demo DLQ**, then Inspect and Replay.

Queue Lab uses the real local A2A bridge, Dapr Pub/Sub, JetStream, SQLite ledger and
DLQ/replay implementation. It does not start a fake second queue.

See [QUEUE_LAB.md](QUEUE_LAB.md) for all controls.

## 9. Submit a task without the browser

Load the generated owner token:

```bash
TOKEN=$(cat .local-validation/local-lite/client.jwt)
```

Send a task through the official JSON-RPC surface:

```bash
curl -s -X POST http://127.0.0.1:18000/rpc \
  -H "Authorization: Bearer $TOKEN" \
  -H "A2A-Version: 1.0" \
  -H "Content-Type: application/json" \
  -d '{
    "jsonrpc": "2.0",
    "id": 1,
    "method": "SendMessage",
    "params": {
      "message": {
        "messageId": "msg-001",
        "role": "ROLE_USER",
        "parts": [{"text": "Hello A2A-MQ!"}]
      },
      "configuration": {"returnImmediately": true}
    }
  }'
```

Copy the returned task ID and query it:

```bash
TASK_ID="<returned-task-id>"

curl -s -X POST http://127.0.0.1:18000/rpc \
  -H "Authorization: Bearer $TOKEN" \
  -H "A2A-Version: 1.0" \
  -H "Content-Type: application/json" \
  -d "{
    \"jsonrpc\": \"2.0\",
    \"id\": 2,
    \"method\": \"GetTask\",
    \"params\": {\"id\": \"$TASK_ID\"}
  }"
```

Operational backlog is available at:

```bash
curl -s http://127.0.0.1:18000/operations/status \
  -H "Authorization: Bearer $TOKEN"
```

## 10. Stop cleanly

Press **Ctrl+C** in the runtime terminal, or send SIGTERM to the launcher.

Verify that the local runtime released its processes/ports:

```bash
ps aux | grep -E 'a2amq|daprd|nats-server' | grep -v grep || true
ss -tulpn | grep -E ':(18000|3500|50001|50002|4222)\b' || true
```

No process or listener from the stopped run should remain.

To discard all local test state after shutdown:

```bash
rm -rf .local-validation/local-lite
```

## 11. Optional local Workflow profile

Use this tier only when you need durable input/approval waits or timers.

```bash
python -m pip install -e '.[local,local-workflow]'
python -m pip check

python scripts/bootstrap_local_tools.py \
  --output .local-validation/workflow-tools \
  --workflow

python scripts/check_local_native_versions.py \
  --daprd .local-validation/workflow-tools/bin/daprd \
  --nats-server .local-validation/workflow-tools/bin/nats-server \
  --placement .local-validation/workflow-tools/bin/placement \
  --scheduler .local-validation/workflow-tools/bin/scheduler \
  --bootstrap-manifest .local-validation/workflow-tools/manifest.json \
  --json .local-validation/workflow-native-versions.json

export PATH="$PWD/.local-validation/workflow-tools/bin:$PATH"

a2amq-local-workflow prepare .local-validation/local-workflow
a2amq-local-workflow doctor .local-validation/local-workflow
a2amq-local-workflow run .local-validation/local-workflow
```

The Workflow provenance command must also print `PASS_NATIVE_VERSION`.

This runtime has five steady-state processes: the A2A-MQ app, `daprd`,
`nats-server`, Placement and Scheduler.

## 12. Where to go next

- [Configuration](CONFIGURATION.md) — supported local options and production environment variables.
- [Troubleshooting](TROUBLESHOOTING.md) — error-code and startup recovery.
- [API Examples](API_EXAMPLES.md) — copyable JSON-RPC requests.
- [Distributed Deployment](DEPLOYMENT.md) — operator prerequisites and manifest rendering.
- [Queue Lab](QUEUE_LAB.md) — every browser/manual test control.
- [Architecture](ARCHITECTURE.md) — durability and runtime boundaries.
- [Local Runtime](LOCAL_RUNTIME.md) — detailed Local-lite/Workflow implementation.
- [Operations](OPERATIONS.md) — retries, DLQ, replay and recovery.
- [Security](SECURITY.md) — trust boundaries and local-vs-production security.
- [Current Status](STATUS.md) — exact RC2 state and limitations.
- [Validation](VALIDATION.md) — SHA-bound evidence and earlier acceptance records.
