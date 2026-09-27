# Queue Lab / local-runtime RC2 validation — 2026-09-27

## Scope

This record captures the focused real-Linux validation used to create
`v0.1.0-rc2`. It intentionally does not relabel older full-acceptance, Kubernetes,
PostgreSQL, Vault, RabbitMQ or long-soak evidence as tests of this SHA.

## Git provenance

- starting SHA: `5406a9f95892a9832597e4924afe50a09add3831`;
- final tested SHA: `4b1fc34065e37b68258f1944fbcb1e9a15f5fe6d`;
- RC2 annotated tag object: `26faf43fcc41221d42e1e6f4829ea4e5c99e56b4`;
- RC2 tagged commit: `4b1fc34065e37b68258f1944fbcb1e9a15f5fe6d`;
- package version: `0.1.0`;
- `v0.1.0-rc1` remained on `3d2b2afd2dbbeb447dab21d1e71afb428e523e8f`.

Two validation commits were created:

1. `d299c97991d444628e8d0dd6a7c9ddddd5a9732a` — test-only timeout headroom for
   pause/retry completion;
2. `4b1fc34065e37b68258f1944fbcb1e9a15f5fe6d` — align both Queue Lab pause
   regression tests on a 1.5-second callback timeout.

The complete delta from the starting SHA to the final SHA is two timeout-value
changes in `tests/test_queue_lab.py`. Production source was not changed.

## Environment

- Ubuntu 24.04.5 LTS (Noble Numbat);
- Linux 6.8.0-139-generic, x86_64;
- Python 3.12.3 in an isolated `.venv`;
- Chromium 153.0.8010.47;
- no Docker substitution for the native local-runtime provenance gates.

## Python/repository gates

```text
python -m pip check
=> No broken requirements found.

python3 tools/check.py
=> Repository syntax, JSON, schema/binding copies and local-link checks passed

python -m pytest -q tests/test_queue_lab.py
=> 11 passed in 9.29s

python -m pytest -q tests/test_bridge_sdk.py -k "subscribe or stream or cancel"
=> 13 passed, 7 deselected in 9.81s

python -m pytest -q tests/test_local_lite.py -k "queue_lab or pause"
=> 1 passed, 9 deselected in 1.31s
```

There were zero failures, errors or unexpected skips.

## Exact native provenance

### Local-lite

`check_local_native_versions.py` returned `PASS_NATIVE_VERSION`.

| Asset | Version | Binary SHA-256 | Archive SHA-256 | Archive bytes |
| --- | --- | --- | --- | ---: |
| `daprd` | 1.18.3 allcomponents | `041c4c3ab9b0d3449faf9d0876377062f1cdcda64d3109989645a9f57a76f262` | `f5d1683b672d83bd88b8e79624d695779b72f856f7eb365ae1ea06354f2cb96c` | 63,716,263 |
| `nats-server` | 2.15.0 | `387ce54e7a0a249d877a579ed641d715bab047d7e061bfa42b4d07f6a0401341` | `5d2c51caca950333aba84911df7d377f826f3a59ec36061c6539105084f65c92` | 7,236,657 |

### Workflow additions

The Workflow provenance gate also returned `PASS_NATIVE_VERSION`.

| Asset | Version | Binary SHA-256 | Archive SHA-256 | Archive bytes |
| --- | --- | --- | --- | ---: |
| `placement` | 1.18.3 | `0d5db7d35dddc5de11a65c438e1a4cf7901d62948db408b02252e0537b3ccd78` | `1a697e769d9bb4b7eab883ee7d8776a243e2f2580bae6a001e8ff7fca12604ad` | 14,382,449 |
| `scheduler` | 1.18.3 | `fce7104deaaf276c3e0eee2b3eab04e7fd0b70fae241c575dabee682ae9d2bc1` | `aa55eae2a8c38ef70e6d3f418a9d88e6b106d7fa56f8f4950c10e79ba7577e6f` | 18,464,040 |

The repository checker verifies archive digest and exact size, extracts and hashes
the expected member, hashes the installed executable, requires the bootstrap-owned
path and refuses unsafe/symlink provenance. This is stronger than a version-string
probe.

## Local-lite Queue Lab browser smoke

Chromium was driven against the real Local-lite runtime at
`http://127.0.0.1:18000/lab/`.

1. **Connection** — `/health/live` reported alive; `/health/ready` reported ready;
   the Local-lite profile/route and authenticated metrics populated; zero JavaScript
   console errors were observed.
2. **Echo and Uppercase** — both tasks were admitted, traversed real NATS
   JetStream/Dapr delivery and reached `TASK_STATE_COMPLETED`; task/artifact
   inspection succeeded.
3. **Pause / queue / cancel / resume** — the simulated local agent was paused at the
   delivery boundary; a burst remained `SUBMITTED`; queue metrics increased; one
   queued task was canceled; after resume the remaining work completed and the
   canceled task stayed `CANCELED`.
4. **SendStreamingMessage** — official A2A SSE emitted three events and terminated
   with the task completed.
5. **SubscribeToTask** — a non-terminal task was subscribed over the official SSE
   surface and streamed snapshots through completion; a direct subscription attempt
   on a terminal task was rejected with `UNSUPPORTED_OPERATION` rather than opening
   a synthetic stream.
6. **DLQ / replay** — Demo DLQ used the real quarantine path; the record appeared in
   Dead letters; Inspect returned the archived CloudEvent; Replay queued successfully;
   the committed terminal task remained intact and non-duplicated.
7. **Raw JSON-RPC** — ListTasks/GetTask passed; a deliberate invalid method returned
   JSON-RPC `-32601 Method not found`; an edited raw streaming request produced SSE.
8. **Session lifecycle** — Clear removed authenticated/task/raw/activity state; an
   invalid JWT produced a controlled 401; loading the valid current `client.jwt`
   reconnected successfully.

## Targeted Workflow smoke

The optional local Workflow profile ran with the five-process topology:
A2A-MQ app, `daprd`, `nats-server`, Placement and Scheduler.

- input-required task paused, accepted continuation input and completed;
- auth-required approve completed;
- auth-required reject reached `TASK_STATE_REJECTED`;
- a task with `delaySeconds: 2` completed after at least two seconds.

## Shutdown

Local-lite and local Workflow were terminated cleanly. Final process checks found no
remaining A2A-MQ, `daprd` or `nats-server` process from the runs, and the checked
runtime ports were no longer listening.

Final `git status --short` was empty before tagging.

## Hosted GitHub Actions note

Actions runs for the same RC2 commit/tag currently fail before runner assignment.
The jobs report `runner_id=0`, empty runner names and `steps=[]`. No repository step
was executed, so those runs are neither source-test failures nor positive evidence.

## Result

The focused RC2 gates required by the Queue Lab Linux handoff passed, so the annotated
`v0.1.0-rc2` tag was created on the exact tested commit.
