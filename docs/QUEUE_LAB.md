# Queue Lab — graphical manual queue testing

Queue Lab is a browser-based manual test console embedded in the existing local
A2A-MQ process. It does **not** start a second queue, mock broker or alternate task
API. Every task still flows through the real A2A bridge, Dapr Pub/Sub, JetStream,
SQLite ledger/outbox and the selected local executor/Workflow profile.

> **Status:** Queue Lab is post-`v0.1.0-rc1` work on the production-hardening branch.
> Its focused tests and one real browser smoke should pass before tagging `v0.1.0-rc2`.

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

The JWT is kept only in browser `sessionStorage`. Task, operations, replay and the
local pause/resume control continue through bearer-authenticated A2A-MQ HTTP
surfaces; Queue Lab does not persist the token server-side. Local client JWTs are
short-lived and refreshed on disk by the launcher, so a long-open tab may ask you
to reload the current `client.jwt` and reconnect.

## What you can test

### Producer

Create tasks with:

- custom text;
- echo or uppercase transformation;
- custom message ID;
- optional context ID;
- immediate return or wait-for-state behavior;
- batches of up to 25 tasks;
- `SendStreamingMessage` over the official A2A SSE binding.

The **Burst / queue view** preset is useful for creating several tasks quickly and
watching task/backlog values change.

### Pause the simulated local agent

Under **local-lite**, Queue Lab exposes **Pause simulated agent**.

This does not stop A2A-MQ, Dapr, JetStream or durable admission. New tasks still
enter through the real A2A API, are durably admitted in SQLite and are published to
JetStream. While paused, the local command delivery is held before `ledger.accept`,
so the task remains `SUBMITTED` and no `start` action is created until delivery is
accepted.

A useful manual scenario is:

1. click **Pause simulated agent**;
2. enqueue one task or choose **Burst / queue view**;
3. watch **Queued / submitted**, Active tasks and task states;
4. inspect or cancel queued tasks;
5. click **Resume simulated agent**;
6. watch the same durable tasks drain and complete.

Cancellation remains available while execution is paused.

The pause is intended for short manual experiments. The callback waits for resume
within its normal timeout; if the pause outlives that timeout, A2A-MQ returns the
normal retry result and Dapr/JetStream redelivery policy applies. An indefinitely
paused consumer can therefore eventually exercise the configured DLQ path rather
than creating an artificial infinite hold.

This control exists only in local-lite. The Workflow profile already has a real Dapr
Workflow runtime, so Queue Lab does not fake a Workflow-worker pause switch.

### Routing target

The running local profile has one real subscribed command route:

`a2a.v1.<tenant>.command.agent.<source>`

Queue Lab shows that configured agent in the header/route label. It deliberately
does not invent arbitrary agent/skill routes that the Dapr component is not scoped
to consume.

Choose a different simulated agent identity when preparing a fresh profile:

```bash
a2amq-local prepare .local-validation/reviewer --source reviewer
```

That produces a real `...command.agent.reviewer` route. Skill-target routing and
multiple independent consumers belong to a multi-service composition rather than
this single local-lite process.

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

### Streaming, follow and subscribe

**Enqueue + A2A stream** sends the task using the official
`SendStreamingMessage` JSON-RPC method and consumes its SSE responses.

**Follow** polls the normal `GetTask` surface and records each state change.

**Subscribe (SSE)** uses the official A2A `SubscribeToTask` stream for an existing
non-terminal task. Terminal tasks are not subscribable and the UI disables that
action. If an ordinary UI subscription cannot keep the event stream open, Queue Lab
falls back to Follow rather than inventing a private streaming protocol. The raw
JSON-RPC console does **not** hide streaming errors behind that fallback.

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
- queued/submitted tasks visible in the current task page;
- total task records;
- pending actions;
- pending publications;
- oldest pending publication age.

### Dead letters

The DLQ panel lists real archived dead letters through
`/operations/deadletters`. Each record can be inspected through the authenticated
record endpoint or replayed with a fresh replay request ID.

For a deterministic manual DLQ test, select any real task and click **Demo DLQ**.
The control requires `a2a:replay`; the generated local owner token already has that
scope. Queue Lab does not insert a row directly. The server loads that task's
persisted command envelope and publishes it through the real
`DaprPublisher.quarantine()` path to the configured DLQ topic. The normal Dapr
subscription then archives it, after which **Inspect** and **Replay** exercise the
ordinary operations/replay path. Replaying the duplicate command remains subject to
the normal task/message idempotency rules.


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
python scripts/bootstrap_local_tools.py \
  --output .local-validation/workflow-tools \
  --workflow
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

## Focused Linux smoke before v0.1.0-rc2

Do not replace this with a full certification run. For the Queue Lab handoff, verify
these user-visible paths on a fresh Linux local profile:

1. **Connect** — load the generated `client.jwt`; health/readiness and queue metrics
   populate.
2. **Ordinary task** — Echo and Uppercase each admit and complete; Inspect shows the
   real A2A Task.
3. **Pause / queue / resume** — pause the simulated agent, enqueue a Burst, confirm
   the tasks remain `SUBMITTED`, cancel one while paused, resume, and confirm the
   remaining tasks complete.
4. **Streaming send** — `Enqueue + A2A stream` produces official
   `SendStreamingMessage` SSE events and reaches a terminal task.
5. **Subscribe** — subscribe to a non-terminal task and observe official
   `SubscribeToTask` SSE. Confirm the UI refuses subscription on a terminal task.
6. **DLQ / replay** — select a task, click **Demo DLQ**, wait for it to appear in
   Dead letters, Inspect it, Replay it, and confirm replay does not corrupt or
   duplicate the committed task result.
7. **Raw console** — List/Get work; an intentionally invalid request returns the
   protocol error without breaking the UI.
8. **Session lifecycle** — Clear removes task/raw/activity data. If practical, replace
   or invalidate the JWT and confirm a 401 clears authenticated data and allows
   reconnect with the current token.
9. **Workflow profile** — only targeted smoke: input-required continuation,
   auth-required approve/reject, and a short delay. Do not rerun the historical
   Workflow resource/soak certification.
10. **Shutdown** — stop the profile and confirm no app/daprd/nats processes or local
    ports from this run remain.

The targeted automated checks for this handoff are documented in the agent prompt;
full acceptance, Kubernetes, PostgreSQL, Vault, RabbitMQ and long soak runs are out
of scope unless a focused failure proves they are directly affected.
