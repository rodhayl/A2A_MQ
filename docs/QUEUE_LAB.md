# Queue Lab — graphical manual queue testing

Queue Lab is a browser-based manual test console embedded in the existing local
A2A-MQ process. It does **not** start a second queue, mock broker or alternate task
API. Every task still flows through the real A2A bridge, Dapr Pub/Sub, JetStream,
SQLite ledger/outbox and the selected local executor/Workflow profile.

## Start it

Queue Lab is automatically available whenever either local profile is running:

- Local-lite: `http://127.0.0.1:18000/lab/`
- Local Workflow: `http://127.0.0.1:18000/lab/`

The launcher prints a `labUrl` field at startup.

Linux/Ubuntu example:

```bash
python3 -m venv .venv
. .venv/bin/activate
python -m pip install -e '.[local]'

python scripts/bootstrap_local_tools.py --output .local-validation/tools
export PATH="$PWD/.local-validation/tools/bin:$PATH"

a2amq-local prepare .local-validation/local-lite
a2amq-local run .local-validation/local-lite
```

Then open the printed `labUrl`.

## Authentication

Queue Lab never receives a server-side bypass credential.

Load the generated local owner token from the profile directory:

```text
.local-validation/local-lite/client.jwt
```

Use **Load file** in the browser, or paste the token manually.

The JWT is kept only in browser `sessionStorage`. All task, operations and replay
requests continue through the normal bearer-authenticated A2A-MQ HTTP surfaces.

## What you can test

### Producer

Create tasks with:

- custom text;
- echo or uppercase transformation;
- custom message ID;
- optional context ID;
- immediate return or wait-for-state behavior;
- batches of up to 25 tasks.

The **Burst / queue view** preset is useful for creating several tasks quickly and
watching task/backlog values change.

### Queue / consumer view

The task table uses the real A2A `ListTasks` API and can filter by state:

- SUBMITTED
- WORKING
- INPUT_REQUIRED
- AUTH_REQUIRED
- COMPLETED
- CANCELED
- FAILED
- REJECTED

Selecting a task uses the real `GetTask` API and shows the complete current task
document, artifacts and history.

### Follow and subscribe

**Follow** polls the normal `GetTask` surface and records each state change.

**Subscribe (SSE)** attempts the official A2A `SubscribeToTask` stream. If the
client/browser path cannot keep the official event stream open, Queue Lab falls back
to Follow rather than inventing a private streaming protocol.

### Cancellation

Cancel sends the official A2A `CancelTask` request and then refreshes the real task
state.

### Workflow controls

When Queue Lab runs under `a2amq-local-workflow`, the composer enables the real
Workflow options:

- input required;
- approval required;
- delay/timer;
- continuation input;
- approve/reject.

Under local-lite these controls are visibly disabled because local-lite intentionally
does not implement Workflow-only semantics.

### Operations and backlog

The dashboard reads `/operations/status` and displays:

- active tasks;
- total task records;
- pending actions;
- pending publications;
- oldest pending publication age.

### Dead letters

The DLQ panel lists real archived dead letters through
`/operations/deadletters`. Replay uses the authenticated replay endpoint and a
fresh replay request ID.

### Raw JSON-RPC

The advanced panel lets you edit and send JSON-RPC manually. Presets are provided for
`ListTasks`, `GetTask`, `CancelTask` and `SubscribeToTask`.

This is useful for deliberately changing parameters and observing protocol errors
without writing a separate client.

## Local-lite versus Workflow

Local-lite is the recommended first test because it is only three steady-state
processes:

1. Python A2A-MQ;
2. `daprd`;
3. `nats-server`.

It executes immediate model-free echo/uppercase work, so no external agent or LLM is
required.

For timers/input/approval, prepare the Workflow profile instead:

```bash
python scripts/bootstrap_local_tools.py   --output .local-validation/workflow-tools   --workflow
export PATH="$PWD/.local-validation/workflow-tools/bin:$PATH"

python -m pip install -e '.[local,local-workflow]'
a2amq-local-workflow prepare .local-validation/local-workflow
a2amq-local-workflow run .local-validation/local-workflow
```

## Windows status

The **Queue Lab web application itself is platform-neutral** and contains no Linux
frontend dependency.

However, the repository's pinned native-tool bootstrap currently supports Linux
amd64/arm64 only. Native Windows local-runtime provisioning has not yet been
certified by this repository. Do not interpret the web UI as Windows runtime
certification.

If the A2A-MQ local service is running successfully on a Windows host with compatible
native Dapr/NATS binaries, Queue Lab is served by the same Python ASGI application at
`/lab/`. The documented, repository-verified bootstrap path remains Linux/Ubuntu.

## Security boundary

Queue Lab is intended for the loopback local profiles.

It ships no CDN assets and uses a restrictive Content-Security-Policy. HTML, CSS and
JavaScript are served from the A2A-MQ package itself. It does not expose private JWT
keys, app tokens, Dapr tokens, NATS credentials or raw SQLite access.

For distributed/production environments, use the normal authenticated APIs and
operational tooling instead of exposing this local manual console.
