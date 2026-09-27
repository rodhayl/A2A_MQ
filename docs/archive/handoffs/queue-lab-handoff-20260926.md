# Queue Lab handoff — 2026-09-26

This is a historical handoff note for the post-`v0.1.0-rc1` Queue Lab work.
Use the branch ref as the source of truth if later commits advance beyond this note.

## Provenance before this handoff note

- branch: `implementation/production-hardening-20260916`
- code/test/docs HEAD before this docs-only handoff:
  `5a2269b9bfb372405df8e6ee162937f49eb5434e`
- tagged baseline: `v0.1.0-rc1` ->
  `3d2b2afd2dbbeb447dab21d1e71afb428e523e8f`
- protected `main`:
  `66c17153d7ec75b9ad85808ef141b9dba43a9d91`
- package version: `0.1.0`

The Queue Lab work is intentionally post-RC1 and must not inherit RC1/FULL_PASS
evidence by implication.

## Implemented Queue Lab scope

The local browser UI is mounted at `/lab/` for both local-lite and local Workflow.
It uses the real A2A/operations/Dapr/JetStream/SQLite surfaces rather than a second
queue implementation.

Current manual surfaces include:

- SendMessage with echo/uppercase presets, custom text, message/context IDs and batch;
- SendStreamingMessage via official A2A SSE;
- ListTasks/GetTask/CancelTask;
- SubscribeToTask for non-terminal tasks, with explicit terminal-task rejection;
- task follow/polling, state filtering and selected-task inspection;
- Workflow input/auth/delay controls when running the Workflow profile;
- pause/resume of the local-lite simulated agent at the delivery boundary;
- operational backlog metrics and submitted-task queue depth;
- DLQ list/read/replay;
- authenticated Demo DLQ injection using the selected task's persisted command through
  the real Dapr quarantine/DLQ path, preserving ownership and replayability;
- raw JSON-RPC console, including exact raw streaming payloads;
- browser session cleanup on JWT expiry/clear;
- local CSP/no-store/no-CDN boundary;
- bounded activity/SSE buffers and stoppable streaming.

The local routing contract still has one configured source/target per profile.
Queue Lab does not invent arbitrary agent/skill targets that the local runtime would
reject. Change the profile source if a different local identity is required.

## Important fixes already applied

- Queue Lab auth/control endpoints return bounded 401/403/404/422/503 results instead
  of leaking internal exceptions.
- local-lite pause holds delivery before ledger acceptance and is bounded by the
  existing callback/redelivery policy; shutdown releases held deliveries cleanly.
- terminal A2A task subscription is rejected by the bridge.
- raw SubscribeToTask/SendStreamingMessage sends the exact edited JSON-RPC payload.
- Workflow-only presets are disabled in local-lite.
- browser buffers are bounded and DOM/CSP assumptions have regression coverage.
- Demo DLQ ownership/replay semantics were hardened in both local profiles.

## Required next step

Do a focused Linux validation only. Do not rerun full acceptance, Kubernetes,
PostgreSQL/Vault/RabbitMQ infrastructure certification or long soak unless a focused
failure proves one of those boundaries is directly affected.

Recommended automated checks:

```bash
python3 tools/check.py
python -m pip check
python -m pytest -q tests/test_queue_lab.py
python -m pytest -q tests/test_bridge_sdk.py -k "subscribe or stream or cancel"
python -m pytest -q tests/test_local_lite.py -k "queue_lab or pause"
```

Then run one fresh local-lite manual browser smoke:

1. prepare/start a fresh profile;
2. load the generated `client.jwt`;
3. verify health/readiness/metrics;
4. Echo + Uppercase;
5. Pause simulated agent -> Burst -> verify SUBMITTED queue depth -> cancel one ->
   Resume -> remaining tasks complete;
6. SendStreamingMessage;
7. SubscribeToTask on a non-terminal task; terminal subscription must be refused;
8. Demo DLQ -> wait for archive -> Inspect -> Replay -> verify no task corruption;
9. Raw List/Get plus one invalid request and one raw streaming request;
10. Clear/reconnect session;
11. stop and confirm no orphan app/daprd/nats processes or bound local ports.

Run a small local Workflow smoke only for input-required continuation,
auth-required approve/reject and short delay. Do not repeat historical resource/soak
certification.

If the focused checks and browser smoke pass, commit any necessary fixes, push
fast-forward, and create `v0.1.0-rc2` on the exact tested commit. Do not move
`v0.1.0-rc1`.
