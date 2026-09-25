# Independent review of an exact source SHA

## Scope is part of the verdict

Review the supplied 40-character commit SHA, not the current tip of a moving branch.
Use a fresh disposable environment without model-provider credentials. Reviewers
record failures and retain evidence; they do not edit, repair, or commit reviewed code.

This source contains the hardened phase-2 baseline and incremental phase-3 SQLite,
runtime and durable-service implementation, **not the recovered full MVP**.
`scripts/independent_mvp.py` defaults to `--scope full` and intentionally returns
`INCOMPLETE` (exit 2): the complete MVP and full strict validator are not available.
It never substitutes local tests for full acceptance.

`--scope local` explicitly tests only the present broker-independent source. It
excludes the `live`, `postgres`, and `upstream` markers. A `LOCAL_PASS` keeps
`mvpStatus=INCOMPLETE` and `productionCertified=false`. CI has a different declared
scope: it installs the pinned official A2A SDK and selects the upstream tests too.
The newer real HTTP scenario uses a local sidecar fixture, not a Dapr/broker runtime.

## Prepare a fresh checkout

Receive `SHA` from the handoff. Replace the placeholder before executing:

```sh
export SHA=REPLACE_WITH_EXACT_40_CHARACTER_COMMIT
export REVIEW_ROOT="$(mktemp -d /tmp/a2amq-independent.XXXXXXXX)"
git clone https://github.com/rodhayl/A2A_MQ.git "$REVIEW_ROOT/source"
cd "$REVIEW_ROOT/source"
git checkout --detach "$SHA"
test "$(git rev-parse HEAD)" = "$SHA"
test -z "$(git status --porcelain=v1 --untracked-files=all)"
python3 -m venv "$REVIEW_ROOT/venv"
. "$REVIEW_ROOT/venv/bin/activate"
export PYTHONDONTWRITEBYTECODE=1 A2AMQ_NO_LLM_TOKENS=1
python -m pip install -e '.[dev]' > "$REVIEW_ROOT/install.log" 2>&1
python -m pip check > "$REVIEW_ROOT/pip-check.log" 2>&1
```

Use Python 3.12 or 3.13. Each command's nonzero exit is a failed prerequisite;
stop that acceptance run and report it. Use a different fresh directory for each
Python version. Run without `PYTEST_ADDOPTS`, custom pytest plugins, Git repository
or index overrides, or unrelated credentials. Use ordinary complete checkouts;
sparse/index-masked worktrees are outside this procedure.

The runtime flag and credential check are guardrails, not a network sandbox. Inspect
fixtures and apply an external egress policy when independently proving zero model
calls. Local HTTP tests need loopback networking and subprocess execution. The
service process fixture uses POSIX socket descriptor inheritance; use the Linux
reference environment rather than silently skipping that test on another platform.

## Run the declared review scope

First record the full-MVP verdict (exit 2 is currently the expected blocking result):

```sh
python scripts/independent_mvp.py --sha "$SHA" --scope full \
  --output "$REVIEW_ROOT/full-evidence"
```

Then run the separate, narrower local review:

```sh
python scripts/independent_mvp.py --sha "$SHA" --scope local \
  --output "$REVIEW_ROOT/local-evidence"
python scripts/assert_junit_complete.py "$REVIEW_ROOT/local-evidence/local.junit.xml"
(cd "$REVIEW_ROOT/local-evidence" && sha256sum -c independent-verdict.sha256)
```

The output paths must not already exist and must be outside both source and Git
metadata. The reviewer checks SHA/tree/cleanliness before and after testing, runs
the repository check and pytest with warnings as errors, retains logs, verifies
nonempty JUnit with zero failures/errors/skips, and hashes retained evidence.
Validate every `evidenceSha256` entry against its named file. Hashes detect changes
against a separately retained digest; they are not signed attestations or protection
against a malicious runner replacing both evidence and hashes.

The timeout is per validation subprocess. It is not process-tree containment or
proof that hostile tests cannot escape. Run only in a disposable review environment.

For a narrower developer reproduction of the new phase-3 implementation, see
[the exact selected-suite command](docs/validation/durable-service-20260917.md).
Its 184-test pass is not a substitute for this entire local review procedure.

## Other gates remain explicit

Run `ruff check .`, `mypy`, and a clean wheel/sdist build with isolated installation,
retaining output outside the checkout. These are not implicitly included in
`LOCAL_PASS`. CI configures lint/types and dependency checks but a workflow file
is not execution evidence. The recorded offline wheel smoke used preinstalled
third-party dependencies and is narrower than this fresh installation gate.

For the available official A2A SDK codec tests:

```sh
python -m pip install -e '.[dev,upstream]'
python -m pip check
PYTHONPATH=src python -m pytest -q -W error -m upstream \
  --junitxml="$REVIEW_ROOT/upstream.junit.xml"
python scripts/assert_junit_complete.py "$REVIEW_ROOT/upstream.junit.xml"
```

These codec tests are not an independent A2A bridge interoperability test. For the
existing destructive legacy JetStream demo, follow `deploy/README.md` and the
prerequisites in `tests/test_live.py`, use disposable resources, select `-m live`,
retain JUnit externally, and reject missing/failed/skipped evidence. That deployment
still uses the volatile demo; it is not the new durable-service integration gate.

Real PostgreSQL, RabbitMQ parity, Dapr Workflow, the A2A HTTP bridge, MCP, Vault/KMS,
production identity/TLS, full-system chaos/E2E, and supply-chain/deployment acceptance
remain `INCOMPLETE`, not `PASS`. Implement and independently validate these gates
against a frozen source tree. Any recovered original snapshot must first pass its
checksum and Git bundle verification before being reconciled with newer changes.

## Report

Provide exact SHA/tree, Python/dependency environment, command exit codes, selected
and excluded tests, actual JUnit counts, retained artifact hashes, and every failing
or missing gate. Distinguish local mocks, real loopback HTTP, Git subprocesses and
real external services. Recheck HEAD and cleanliness. Do not publish a release or
claim production readiness on the strength of a local-only result.
