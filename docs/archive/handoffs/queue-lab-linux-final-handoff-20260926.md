# Final Queue Lab Linux handoff — 2026-09-26

Use the branch `implementation/production-hardening-20260916` as the source of truth. Do not move
`v0.1.0-rc1`. Create `v0.1.0-rc2` only after the focused Linux smokes below pass.

## Provenance

- parent before this closing commit: `1b550d4d1afab31b2f6f5ac1628f8f0949371411`
- post-RC1 Queue Lab handoff ancestor: `95814b494ca7ee5d386cc2980075f2c97e2527f4`
- tagged baseline: `v0.1.0-rc1` -> `3d2b2afd2dbbeb447dab21d1e71afb428e523e8f`
- protected `main`: `66c17153d7ec75b9ad85808ef141b9dba43a9d91`

## Closing fix in this commit

The official `a2a-sdk==1.1.2` required-field validator still reads
`FieldDescriptor.label`. With protobuf 6.x that emits a specific
`DeprecationWarning`; this repository promotes warnings to errors, and protobuf then
surfaces a `SystemError` during valid bridge requests.

The bridge now suppresses only that exact SDK-internal deprecation and only around
`validate_proto_required_fields()`. All other warnings remain fatal.

The exact patch was exercised in the current Linux validation container with
protobuf 6.33.6.

## Focused checks already executed in this handoff session

These completed successfully before this commit, with the bridge selector repeated
after applying the exact fix now committed:

```text
python3 tools/check.py
=> Repository syntax, JSON, schema/binding copies and local-link checks passed

python -m pytest -q tests/test_queue_lab.py
=> 11 passed

python -m pytest -q tests/test_local_lite.py -k "queue_lab or pause"
=> 1 passed, 9 deselected

python -m pytest -q tests/test_bridge_sdk.py -k "subscribe or stream or cancel"
=> 13 passed
```

Chromium 144 was also proven controllable headlessly through its DevTools endpoint;
the remaining browser work is therefore a product smoke, not a browser-availability
problem.

## What remains to be done on a normal Linux host

Do not rerun the whole production certification. Only complete the focused local
runtime checks below.

### 1. Fresh checkout and Python gates

```bash
git clone https://github.com/rodhayl/A2A_MQ.git
cd A2A_MQ
git checkout implementation/production-hardening-20260916

python3 -m venv .venv
. .venv/bin/activate
python -m pip install -U pip
python -m pip install -e '.[local]'
python -m pip check

python3 tools/check.py
python -m pytest -q tests/test_queue_lab.py
python -m pytest -q tests/test_bridge_sdk.py -k "subscribe or stream or cancel"
python -m pytest -q tests/test_local_lite.py -k "queue_lab or pause"
```

All four focused gates must pass with zero failures and zero unexpected skips.

### 2. Bootstrap exact pinned native tools

Use the repository bootstrap. Do not substitute a same-version host binary for the
provenance gate.

```bash
rm -rf .local-validation
python scripts/bootstrap_local_tools.py --output .local-validation/tools

python scripts/check_local_native_versions.py \
  --daprd .local-validation/tools/bin/daprd \
  --nats-server .local-validation/tools/bin/nats-server \
  --bootstrap-manifest .local-validation/tools/manifest.json \
  --json .local-validation/native-versions.json

export PATH="$PWD/.local-validation/tools/bin:$PATH"
```

Expected native gate: `PASS_NATIVE_VERSION`, using Dapr 1.18.3 allcomponents and
NATS Server 2.15.0 from the exact retained release archives.

The previous validation container could recover the exact Dapr artifacts and a
working NATS 2.15.0 Homebrew build, but it could not download the exact official
NATS release tarball required by the repository's archive-provenance checker. Do not
weaken that checker; a normal networked Linux host should let
`bootstrap_local_tools.py` fetch and verify the pinned release asset directly.

### 3. Real local-lite Queue Lab browser smoke

```bash
a2amq-local prepare .local-validation/local-lite
a2amq-local run .local-validation/local-lite
```

Open `http://127.0.0.1:18000/lab/`, load
`.local-validation/local-lite/client.jwt`, and verify:

1. health/readiness/metrics populate;
2. Echo and Uppercase complete;
3. Pause simulated agent -> Burst -> tasks remain SUBMITTED -> cancel one -> Resume
   -> remaining tasks complete;
4. SendStreamingMessage emits official SSE and terminates;
5. SubscribeToTask works on a non-terminal task and terminal subscription is refused;
6. Demo DLQ -> archive -> Inspect -> Replay without task corruption/duplication;
7. raw List/Get, one intentionally invalid request, and one raw streaming request;
8. Clear/reconnect session;
9. stop the runtime and confirm no orphan app/daprd/nats process or bound local port.

### 4. Small Workflow-only smoke

Only if local-lite passes:

```bash
python -m pip install -e '.[local,local-workflow]'
python scripts/bootstrap_local_tools.py \
  --output .local-validation/workflow-tools --workflow
export PATH="$PWD/.local-validation/workflow-tools/bin:$PATH"

a2amq-local-workflow prepare .local-validation/local-workflow
a2amq-local-workflow run .local-validation/local-workflow
```

Verify only input-required continuation, auth-required approve/reject, and a short
delay. Do not repeat historical soak/resource/infrastructure certification unless a
focused failure points there.

## Finish condition

If the focused automated gates plus local-lite smoke and the small Workflow smoke
pass:

1. record exact command outputs and final `git status --short`;
2. require a clean working tree;
3. create annotated tag `v0.1.0-rc2` on the exact tested commit;
4. push the tag;
5. report the commit SHA, tag SHA, Linux/Python versions, and any skips.

If any focused check fails, fix only the demonstrated issue, rerun the directly
affected selector plus `tools/check.py`, and do not tag until green.
