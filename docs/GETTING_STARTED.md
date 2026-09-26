# Getting started with A2A-MQ

This guide walks through running the lightweight **Local-lite** profile of A2A-MQ,
verifying its health, and completing your first end-to-end task interaction.

---

## 1. What am I starting?

A2A-MQ supports three runtime tiers:

1. **Local-lite (recommended default)**: A single-host, container-free 3-process stack:
   - 1 Python A2A-MQ application process (serving HTTP, JSON-RPC, and outbox relay);
   - 1 native `daprd` sidecar process (managing JetStream pub/sub);
   - 1 native `nats-server` process with JetStream enabled;
   - SQLite files for durable state.
2. **Optional local Workflow**: A 5-process local stack adding native Dapr `placement`
   and `scheduler` when workflow-specific semantics (durable timers, input continuation,
   approval pauses, fan-out/fan-in) are required locally.
3. **Distributed / Production profile**: Role-separated services (API, workers,
   observers) with external PostgreSQL, clustered JetStream or RabbitMQ, optional Vault
   transit encryption, and Kubernetes deployments.

**Local-lite is the default.** It requires **no** PostgreSQL, Redis, Docker,
Kubernetes, Vault, Placement, or Scheduler.

---

## 2. Prerequisites

### Python
- Python **>= 3.12**
- A project-local virtual environment (`.venv`)

### Native binaries
Local-lite requires two standalone native executables:
- **`daprd`**: Dapr 1.18.3 (the default/`allcomponents` build, required for JetStream pub/sub).
- **`nats-server`**: NATS Server >= 2.10 (tested with v2.15.0).

If these binaries are already in your `PATH` (or under `~/.dapr/bin`), `a2amq-local`
finds them automatically.

If you do not have them installed, you can bootstrap the exact repository-pinned standalone
binaries into an isolated, git-ignored directory without modifying system packages:

```bash
python scripts/bootstrap_local_tools.py --output .local-validation/tools
export PATH="$PWD/.local-validation/tools/bin:$PATH"
```

Verify that the binaries are available:

```bash
daprd --version
nats-server --version
```

---

## 3. Installation

Create and activate an isolated virtual environment, then install the package with the
`local` extra:

```bash
python3 -m venv .venv
. .venv/bin/activate
python -m pip install -e '.[local]'
python -m pip check
```

The `local` extra installs the official A2A SDK, cryptographic identity libraries (`PyJWT`,
`cryptography`), and the NATS client for initial stream setup.

---

## 4. Prepare a local profile

Prepare an isolated, owner-private profile directory (for example under `.local-validation/local-lite`):

```bash
a2amq-local prepare .local-validation/local-lite
```

This command generates:
- `profile.json`: Runtime configuration and port bindings;
- `app-api-token`, `dapr-api-token`, `nats-token`: Private random loopback credentials;
- `jwt-private.pem`, `jwt-keys.json`: Local RS256 token signing and verification keys;
- `client.jwt`: A pre-signed owner JWT with full task scopes (`a2a:send`, `a2a:read`, `a2a:cancel`, etc.);
- `components/`: Dapr Pub/Sub component manifests for JetStream;
- `nats.conf`: Loopback NATS server configuration;
- `state/`: Directory for the application SQLite database.

---

## 5. Start the service

Ensure the tools directory is in `PATH`, then start the runtime:

```bash
a2amq-local run .local-validation/local-lite
```

Expected startup output:

```json
{"processes": {"app": 1018277, "daprd": 1018316, "nats": 1018286}, "profile": "local-lite", "status": "starting", "url": "http://127.0.0.1:18000"}
```

At steady state, exactly three processes run:
1. Python A2A-MQ (serving port `18000`);
2. Native `daprd` (HTTP port `3500`, gRPC `50001`, internal gRPC `50002`);
3. Native `nats-server` (port `4222`).

The launcher continuously monitors child processes and automatically refreshes client tokens
every 30 minutes.

---

## 6. Confirm health

In a separate terminal, verify that the service is running and ready:

```bash
# Liveness
curl -s http://127.0.0.1:18000/health/live

# Readiness (verifies SQLite access and Dapr Pub/Sub reachability)
curl -s http://127.0.0.1:18000/health/ready
```

Expected output for readiness:

```json
{"status":"ready"}
```

You can also fetch the official A2A Agent Card:

```bash
curl -s http://127.0.0.1:18000/.well-known/agent-card.json
```

---

## 7. First useful operation: Submit and query a task

Local-lite implements deterministic immediate text transformation (echo and uppercase).

### Step 7.1: Load your client credentials

Read the owner JWT created during preparation:

```bash
TOKEN=$(cat .local-validation/local-lite/client.jwt)
```

### Step 7.2: Submit a task (JSON-RPC)

Submit a task using the official A2A `SendMessage` method:

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
      "configuration": {
        "returnImmediately": true
      }
    }
  }'
```

Expected response:

```json
{
  "jsonrpc": "2.0",
  "id": 1,
  "result": {
    "task": {
      "id": "c53fc357-903d-5f31-9744-1dea467d6098",
      "contextId": "56ac5a6a-22ec-58a8-a0ed-828902e082c6",
      "status": {
        "state": "TASK_STATE_SUBMITTED",
        "timestamp": "2026-09-26T08:44:13.380917Z"
      },
      "history": [...]
    }
  }
}
```

Copy the returned `task.id` (e.g. `c53fc357-903d-5f31-9744-1dea467d6098`).

### Step 7.3: Query the completed task

Local-lite asynchronously accepts the command over JetStream, executes the action,
and records the result in SQLite. Query the task status via `GetTask`:

```bash
curl -s -X POST http://127.0.0.1:18000/rpc \
  -H "Authorization: Bearer $TOKEN" \
  -H "A2A-Version: 1.0" \
  -H "Content-Type: application/json" \
  -d '{
    "jsonrpc": "2.0",
    "id": 2,
    "method": "GetTask",
    "params": {
      "id": "c53fc357-903d-5f31-9744-1dea467d6098"
    }
  }'
```

Or via REST:

```bash
curl -s http://127.0.0.1:18000/tasks/c53fc357-903d-5f31-9744-1dea467d6098 \
  -H "Authorization: Bearer $TOKEN" \
  -H "A2A-Version: 1.0"
```

Expected completed response:

```json
{
  "jsonrpc": "2.0",
  "id": 2,
  "result": {
    "id": "c53fc357-903d-5f31-9744-1dea467d6098",
    "contextId": "56ac5a6a-22ec-58a8-a0ed-828902e082c6",
    "status": {
      "state": "TASK_STATE_COMPLETED",
      "timestamp": "2026-09-26T08:44:16.453746Z"
    },
    "artifacts": [
      {
        "artifactId": "result",
        "parts": [
          {
            "text": "Hello A2A-MQ!"
          }
        ]
      }
    ],
    "history": [...]
  }
}
```

### Step 7.4: Check operational backlog

You can inspect the operational health and backlog metrics:

```bash
curl -s http://127.0.0.1:18000/operations/status \
  -H "Authorization: Bearer $TOKEN"
```

Expected response:

```json
{
  "activeTasks": 0,
  "taskRecords": 1,
  "retiredIdentities": 0,
  "pendingPublications": 0,
  "pendingActions": 0,
  "oldestPublicationAgeSeconds": 0
}
```

---

## 8. Stopping the service

In the terminal where `a2amq-local run` is active, press **Ctrl+C** (or send `SIGTERM`).

The launcher will:
1. Signal the ASGI HTTP server to drain active requests;
2. Send `SIGTERM` to `daprd` and `nats-server`;
3. Wait for clean shutdown of all children and terminate cleanly with code 0.

Confirm no orphan processes remain on loopback ports:

```bash
ss -tulpn | grep -E ':(18000|3500|50001|50002|4222)\b'
```

---

## 9. Where is state stored?

All local state is stored inside the directory you provided to `prepare`:

- `state/a2amq.db`: SQLite database holding task ledgers, jobs, inbox idempotency, and outbox tables;
- `nats/`: JetStream message store directory on disk;
- `client.jwt`: The active client access token (atomically refreshed every 30 minutes);
- `jwt-private.pem` & `jwt-keys.json`: Local RS256 key pair;
- `components/`: Dapr component configuration files;
- `profile.json`: Metadata for profile mode and ports.

To discard a test run and return to a clean slate, stop the service and delete the directory:

```bash
rm -rf .local-validation/local-lite
```

---

## 10. What next?

- **[Local Runtime Architecture](LOCAL_RUNTIME.md)**: Deep dive into the 3-process Local-lite budget and the optional 5-process Local Workflow tier.
- **[Architecture & Boundaries](ARCHITECTURE.md)**: Process boundaries, storage contracts, and outbox delivery guarantees.
- **[Operations & Recovery](OPERATIONS.md)**: Retries, DLQ inspection, replay service, and retention management.
- **[Distributed Stack](IMPLEMENTED_STACK.md)**: Role separation, PostgreSQL, RabbitMQ, Vault transit encryption, and Kubernetes deployments.
- **[Status & Evidence](STATUS.md)**: Current v0.1.0 pre-release verification, formal acceptance gates, and known boundaries.
