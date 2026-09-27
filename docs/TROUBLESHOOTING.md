# Troubleshooting

This guide covers failures that the current A2A-MQ launchers/configuration can
actually produce.

Start local diagnosis with:

```bash
a2amq-local doctor .local-validation/local-lite
```

or:

```bash
a2amq-local-workflow doctor .local-validation/local-workflow
```

`doctor` is read-only: it does not install tools, repair profiles, kill processes
or rotate credentials.

## `OUTPUT_ALREADY_EXISTS`

**Cause:** `prepare` refuses to overwrite a profile.

**Check:**

```bash
ls -ld <profile-directory>
```

**Fix:** stop any runtime using that profile, then either keep it and run it, choose a
new profile directory, or intentionally delete the old disposable profile before
preparing again.

## `PROFILE_NOT_FOUND`

**Cause:** the path does not contain a regular local profile directory, or the path is
a symlink.

**Fix:** use the directory produced by the matching `prepare` command.

## `UNSAFE_LOCAL_PROFILE`

**Cause:** the local profile is group/world accessible.

**Fix on Linux:**

```bash
chmod 700 <profile-directory>
```

If generated files were manually changed, prefer preparing a fresh profile.

## `LOCAL_PROFILE_MODE_MISMATCH`

**Cause:** a Local-lite profile was passed to the Workflow launcher or vice versa.

**Fix:** use:

```text
a2amq-local              -> local-lite profile
a2amq-local-workflow     -> local-workflow profile
```

Do not edit `profile.json` to change modes.

## Missing native prerequisites

Typical message:

```text
ERROR: missing native prerequisite(s): daprd, nats-server
```

**Fix:**

```bash
python scripts/bootstrap_local_tools.py --output .local-validation/tools
export PATH="$PWD/.local-validation/tools/bin:$PATH"
a2amq-local doctor .local-validation/local-lite
```

For Workflow, bootstrap with `--workflow` and use that tools directory.

## Native version or provenance failure

If `doctor` finds bootstrap-owned binaries, it invokes the repository's exact
native provenance checker.

**Fix:** remove the disposable tools directory and bootstrap it again. Do not replace
individual files inside a validated tools directory.

The repository-pinned Local-lite versions are Dapr 1.18.3 and NATS Server 2.15.0.

## Local port already in use

Typical message:

```text
ERROR: local port 18000 is already in use
```

**Check:**

```bash
ss -tulpn | grep ':18000'
```

The launcher intentionally uses fixed loopback ports and does not silently attach to
another project's Dapr/NATS process.

Stop the conflicting process before running A2A-MQ.

## `health/live` works but `health/ready` returns 503

Liveness only says the application process can answer. Readiness also checks required
local storage/Dapr/broker dependencies.

**Check:**

1. run `a2amq-local doctor <profile>` before the next startup;
2. inspect the runtime terminal for a failed child;
3. confirm `daprd` and `nats-server` are still running;
4. confirm the expected loopback ports are bound by this run.

Do not treat a live-only process as ready for task admission.

## Queue Lab returns 401 after being open for a while

Local client tokens are short-lived. The launcher periodically refreshes
`client.jwt` on disk; an old browser tab may still hold the older token.

**Fix:**

1. click **Load file**;
2. load the current `<profile>/client.jwt`;
3. reconnect.

Do not copy `jwt-private.pem` into the browser.

## `WORKFLOW_FEATURE_UNAVAILABLE`

**Cause:** a Workflow-only feature was submitted to Local-lite.

Examples include input/auth waits and non-zero durable delays.

**Fix:** use the optional Workflow profile:

```bash
a2amq-local-workflow prepare .local-validation/local-workflow
a2amq-local-workflow run .local-validation/local-workflow
```

Do not add a second scheduler to Local-lite.

## `EXACTLY_ONE_STORAGE_BACKEND_REQUIRED`

**Cause:** a distributed role has neither storage backend configured, or both SQLite
and PostgreSQL were configured.

Configure exactly one of:

```text
A2A_SQLITE_PATH
A2A_POSTGRES_DSN / A2A_POSTGRES_DSN_FILE
```

Production deployments normally use PostgreSQL.

## `AMBIGUOUS_SECRET_CONFIG`

**Cause:** both direct and file variants of the same secret were supplied.

Examples:

```text
DAPR_API_TOKEN + DAPR_API_TOKEN_FILE
APP_API_TOKEN + APP_API_TOKEN_FILE
A2A_POSTGRES_DSN + A2A_POSTGRES_DSN_FILE
```

Remove one source.

## `COLOCATED_DAPR_REQUIRED`

Production role factories intentionally accept a colocated loopback Dapr sidecar,
not an arbitrary remote Dapr endpoint.

Check:

```text
DAPR_HTTP_ENDPOINT=http://127.0.0.1:3500
DAPR_GRPC_PORT=50001
```

Do not set `DAPR_GRPC_ENDPOINT`.

## PostgreSQL DSN errors

The production path rejects empty/malformed DSN configuration and, by default,
requires the secure PostgreSQL policy implemented by the store.

Prefer an operator-managed secret and avoid
`A2A_POSTGRES_ALLOW_INSECURE=true` outside an explicitly disposable test.

## Local smoke fails

Run:

```bash
python scripts/smoke_local.py .local-validation/local-lite
```

The script intentionally checks only the small happy path: health, readiness, Agent
Card, SendMessage, task completion/GetTask and operations status.

If it fails, use Queue Lab or the targeted pytest suites for deeper diagnosis rather
than expanding the smoke into a second acceptance framework.

## Runtime did not release local ports

After stopping the local runtime:

```bash
ps aux | grep -E 'a2amq|daprd|nats-server' | grep -v grep || true
ss -tulpn | grep -E ':(18000|3500|50001|50002|50005|50006|4222)\b' || true
```

If a child remains, identify its owner before killing anything. The normal launcher
shutdown path should terminate its own children.

## Unexpected Python traceback

Expected operator mistakes are converted to short actionable CLI messages. An
unexpected traceback is therefore useful bug evidence.

Capture:

- command;
- Python version;
- branch/tag/SHA;
- `doctor` output;
- traceback;
- whether the profile was newly prepared.

Do not include JWTs, broker tokens, private keys or DSNs in bug reports.
