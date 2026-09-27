# Queue Lab — graphical manual queue testing

Queue Lab is the browser-based local test console included in **v0.1.0-rc2**. It is
served by the existing A2A-MQ process and uses the real local A2A/Dapr/JetStream/
SQLite path; it does not create a second queue or mock task backend.

RC2 includes a completed real-Chromium smoke covering the controls described below.
See [validation/queue-lab-rc2-20260927.md](validation/queue-lab-rc2-20260927.md).

## Start it

Follow [GETTING_STARTED.md](GETTING_STARTED.md) to prepare the exact pinned native
tools and Local-lite profile, then run:

```bash
a2amq-local run .local-validation/local-lite
```

Open:

```text
http://127.0.0.1:18000/lab/
```

The same path is available when the optional local Workflow profile is running.

## Authentication

Queue Lab has no server-side authentication bypass.

Load the generated local owner token:

```text
.local-validation/local-lite/client.jwt
```

Use **Load file** or paste the token. The browser keeps the JWT in
`sessionStorage`; the private signing key and Dapr/NATS credentials are never
exposed through Queue Lab.

Local client JWTs are short-lived and refreshed on disk by the launcher. If a long
session becomes unauthorized, load the current `client.jwt` and reconnect.

## Producer controls

You can create tasks with:

- custom text;
- Echo or Uppercase transformation;
- a custom message ID;
- optional context ID;
- immediate return or wait-for-state behavior;
- batches of up to 25 tasks;
- official A2A `SendStreamingMessage` over SSE.

**Burst / queue view** is useful for observing several queued tasks at once.

## Pause simulated agent — Local-lite only

**Pause simulated agent** holds command delivery before `ledger.accept`.

It does not stop the application, Dapr, JetStream or durable task admission. New
tasks can still be admitted and published while execution is held. Their visible
state remains `SUBMITTED` until a delivery is accepted.

Recommended scenario:

1. click **Pause simulated agent**;
2. enqueue a task or **Burst / queue view**;
3. confirm queued/submitted and active-task values change;
4. inspect or cancel one queued task;
5. click **Resume simulated agent**;
6. confirm remaining tasks drain and complete;
7. confirm the canceled task remains canceled.

Pause is bounded. An in-flight delivery is held for at most five seconds and always
for less than the configured callback timeout. If pause remains active, the callback
returns the normal retry result so broker/Dapr redelivery and DLQ policy remain
authoritative.

The Workflow profile does not expose a fake worker-pause switch.

## Routing

A local profile subscribes to one configured command route:

```text
a2a.v1.<tenant>.command.agent.<source>
```

Queue Lab displays the configured route and does not invent arbitrary agent/skill
routes.

To test a different local agent identity, prepare another profile:

```bash
a2amq-local prepare .local-validation/reviewer --source reviewer
```

## Task table

The table uses the real A2A `ListTasks` API. It can display/filter states including:

- SUBMITTED
- WORKING
- INPUT_REQUIRED
- AUTH_REQUIRED
- COMPLETED
- CANCELED
- FAILED
- REJECTED

Selecting a row uses the real `GetTask` API and shows the task document, artifacts
and history.

## Streaming and subscription

**Enqueue + A2A stream** calls official `SendStreamingMessage` and displays its SSE
events.

**Subscribe (SSE)** calls official `SubscribeToTask` for an existing non-terminal
task. Terminal tasks are not subscribable. The UI disables that action and a direct
raw call is expected to return a protocol error rather than a fake stream.

**Follow** polls `GetTask` and is a UI convenience for recoverable subscription
failures; the raw JSON-RPC console does not hide stream/protocol errors.

## Cancellation

Cancel uses the official `CancelTask` surface and refreshes the real task state.
RC2 explicitly validated cancellation while Local-lite delivery was paused.

## Workflow-only controls

When the page is served by `a2amq-local-workflow`, the composer enables:

- input-required;
- continuation input;
- auth-required;
- approve/reject;
- delay/timer.

These controls are disabled under Local-lite because Local-lite intentionally rejects
Workflow-only semantics.

## Operations and backlog

The dashboard reads the authenticated `/operations/status` endpoint and exposes
operational values including:

- active tasks;
- queued/submitted tasks on the current task page;
- task records;
- pending actions;
- pending publications;
- oldest pending publication age.

## Dead letters and replay

The DLQ panel uses the normal authenticated operations endpoints.

**Demo DLQ** is a local deterministic test helper. It does not insert a database row
directly. For the selected task, the server:

1. checks ordinary task read/owner authorization;
2. loads the persisted command envelope;
3. reconstructs the canonical CloudEvent;
4. publishes it through the real `DaprPublisher.quarantine()` path;
5. lets the normal Dapr DLQ subscription archive it.

After it appears in **Dead letters**, use **Inspect** and **Replay**. Replayed
commands remain subject to the normal immutable identity/idempotency rules. RC2
validated that replay did not duplicate or corrupt an already committed terminal
result.

## Raw JSON-RPC

The advanced panel lets you edit and send a JSON-RPC document directly.

Presets include:

- ListTasks;
- SendMessage;
- SendStreamingMessage;
- GetTask;
- CancelTask;
- SubscribeToTask.

For streaming methods, Queue Lab sends the edited JSON unchanged and displays SSE
events. Deliberately invalid methods should return a clean JSON-RPC protocol error
without breaking the page.

## Session controls

**Clear** removes the browser's current authenticated/task/raw/activity state.

RC2 also validated an invalid bearer token returning 401 and a subsequent successful
reconnect using the current generated `client.jwt`.

## Local-lite versus Workflow

Use Local-lite first unless the scenario specifically requires Workflow semantics.

Local-lite steady state:

1. Python A2A-MQ;
2. `daprd`;
3. `nats-server`.

Workflow steady state adds:

4. Dapr Placement;
5. Dapr Scheduler.

Both remain loopback-only single-host profiles.

## Linux / Windows boundary

The Queue Lab frontend itself has no Linux-specific browser dependency. The
repository-pinned native bootstrap is currently certified only on Linux amd64/arm64.
A manually assembled Windows runtime is therefore outside the current repository
validation boundary.

## Security boundary

Queue Lab is intended for the built-in loopback local profiles.

It ships local static assets, uses a restrictive Content-Security-Policy and exposes
no raw SQLite or private-key endpoint. Distributed/production deployments should use
their normal authenticated API/operations surfaces rather than expose this local
manual console.

## RC2 smoke summary

The real Linux/Chromium RC2 run passed:

1. connect + health/readiness + metrics;
2. Echo and Uppercase;
3. pause -> burst -> cancel -> resume -> drain;
4. SendStreamingMessage SSE;
5. SubscribeToTask SSE + terminal rejection;
6. Demo DLQ -> Inspect -> Replay;
7. raw List/Get + invalid method + raw streaming;
8. Clear + invalid JWT 401 + valid reconnect;
9. Workflow input continuation, approve/reject and short timer;
10. clean runtime shutdown.

See [VALIDATION.md](VALIDATION.md) for the release evidence index.
