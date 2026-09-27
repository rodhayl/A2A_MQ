# Current status — v0.1.0-rc2

## Release identity

- Package version: **0.1.0**.
- Latest tested release candidate: **`v0.1.0-rc2`**.
- RC2 tagged commit: `4b1fc34065e37b68258f1944fbcb1e9a15f5fe6d`.
- Annotated tag object: `26faf43fcc41221d42e1e6f4829ea4e5c99e56b4`.
- Date: 2026-09-27.
- `v0.1.0-rc1` remains unchanged and points to
  `3d2b2afd2dbbeb447dab21d1e71afb428e523e8f`.
- RC2 was validated from the production-hardening branch on real Linux before the tag
  was created. Documentation-only follow-up commits may be newer than the tag; the tag
  itself is intentionally immutable.

## RC2 Linux validation

RC2 closed the post-RC1 Queue Lab/local-runtime gap on Ubuntu 24.04.5 LTS, x86_64,
Python 3.12.3 and Chromium 153.0.8010.47.

Focused automated gates:

- `python -m pip check` — PASS (`No broken requirements found.`);
- `python3 tools/check.py` — PASS;
- `tests/test_queue_lab.py` — **11 passed**;
- `tests/test_bridge_sdk.py -k "subscribe or stream or cancel"` — **13 passed**,
  7 deselected;
- `tests/test_local_lite.py -k "queue_lab or pause"` — **1 passed**, 9 deselected;
- no failures, errors or unexpected skips.

Native provenance gates:

- Local-lite: `PASS_NATIVE_VERSION`;
- Workflow: `PASS_NATIVE_VERSION`;
- Dapr `daprd` 1.18.3, all-components release archive;
- NATS Server 2.15.0;
- Workflow Placement and Scheduler 1.18.3.

User-facing Local-lite/Queue Lab smoke passed:

- health/readiness and authenticated metrics;
- Echo and Uppercase over real Dapr Pub/Sub + JetStream;
- pause simulated agent -> enqueue burst -> tasks remain `SUBMITTED` -> cancel one ->
  resume -> remaining tasks complete and canceled task stays canceled;
- official `SendStreamingMessage` SSE;
- official `SubscribeToTask` on a non-terminal task and clean rejection on a terminal task;
- Demo DLQ -> archive -> Inspect -> Replay with terminal task result preserved;
- raw List/Get, deliberate protocol error and raw streaming request;
- session Clear, invalid JWT 401 and successful reconnect;
- zero JavaScript console errors.

Targeted local Workflow smoke also passed:

- input-required continuation;
- auth-required approve and reject;
- short durable delay/timer.

Both local profiles shut down cleanly with no remaining A2A-MQ/Dapr/NATS processes
or bound runtime ports.

Detailed evidence: [validation/queue-lab-rc2-20260927.md](validation/queue-lab-rc2-20260927.md).

## Changes made during RC2 validation

Starting SHA was `5406a9f95892a9832597e4924afe50a09add3831`. Two test-only commits were
created before the final tag:

1. `d299c97991d444628e8d0dd6a7c9ddddd5a9732a` — increase one pause/retry test
   callback timeout from 50 ms to 500 ms;
2. `4b1fc34065e37b68258f1944fbcb1e9a15f5fe6d` — use 1.5 s in both Queue Lab
   pause regression tests to leave reliable headroom for synchronous SQLite commits.

Across those two commits only `tests/test_queue_lab.py` changed: two timeout values
were adjusted. No production code, assertion, skip/xfail policy, security boundary or
runtime default was changed.

## Product shape

### Default Local-lite

- one Python A2A-MQ process;
- one native `daprd` sidecar;
- one native `nats-server` with JetStream;
- SQLite application state;
- real A2A JSON-RPC/HTTP bindings, task ledger/outbox, cancel, DLQ/replay and Queue Lab.

Local-lite intentionally supports immediate deterministic work only. Workflow waits,
input/auth continuation, timers and fan-out/fan-in are rejected with
`WORKFLOW_FEATURE_UNAVAILABLE`.

### Optional local Workflow

Adds native Dapr Placement + Scheduler and Dapr Workflow state, for five steady-state
processes. It remains single-host/container-free and reuses the same TaskLedger, A2A
bridge, outbox and operational surfaces.

### Distributed / production profile

Supports role-separated API/Workflow/DLQ processes, shared PostgreSQL, JetStream or
RabbitMQ Dapr Pub/Sub profiles, MCP, optional Vault Transit payload protection,
telemetry, backup/recovery tooling and Kubernetes manifest rendering.

## Formal acceptance retained from earlier SHA

Exact source `45c459c632977143b8681c055847775e88d7ad97` achieved the repository
**FULL_PASS** across all required sub-gates:

- local broker-independent: 1,150 passed;
- official SDK/MCP: 26 passed;
- verified-TLS PostgreSQL 16: 4 passed;
- TLS Vault Transit: 1 passed;
- repository/dependency/Ruff/Mypy/package gates: PASS;
- JetStream live probe: 12/12;
- RabbitMQ live probe: 12/12.

This evidence remains valid for that source. It is not silently relabeled as a full
acceptance run of the later RC2 source.

## Kubernetes deployment acceptance retained from earlier image

A digest-pinned image built from source
`6458195b171020ece1911e68bc6d14998d216164` achieved
**PRODUCTION_DEPLOYMENT_PASS (19/19)** on live three-node Kubernetes v1.31.0 with
Dapr 1.18.3 and three-node JetStream.

That result is image-bound. A newly rebuilt image needs its own deployment gate.
RabbitMQ Kubernetes parity has not been separately executed.

## Hosted CI

GitHub Actions runs associated with the RC2 commit/tag currently fail before runner
assignment: jobs report `runner_id=0`, an empty runner name and zero executed steps.
This is an infrastructure/unexecuted-CI condition, not a failing source test and not
a pass.

## Evidence boundaries

- `productionCertified = false`
- `upstreamBindingCertified = false`
- no generic exactly-once guarantee for arbitrary external effects;
- no HA SLA, RPO/RTO or unmeasured latency/throughput claim;
- historical resource/soak results remain bound to their recorded SHA/environment;
- Windows native-runtime bootstrap is not repository-certified; the pinned bootstrap
  supports Linux amd64/arm64.

Historical construction material is under [archive](archive/).
