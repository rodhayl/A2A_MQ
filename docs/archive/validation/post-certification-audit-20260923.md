# Post-certification source audit — 2026-09-23

## Provenance

- Branch: `implementation/production-hardening-20260916`
- Last completed local-tier certification documentation head: `7a667a4f`
- Exact validated source recorded by that certification: `1dd1938dff5cbf58a803ec23617143b5d998465e`
- Source/test/harness hardening reviewed here: through `64548bd46c3a2e204a4c6730ecc73e23691a004b`
- `main`: unchanged at `66c17153d7ec75b9ad85808ef141b9dba43a9d91`
- Package version: `0.1.0`

## Changes after certification

1. `711f83fb` — removed the known `test_workflows.py` E701/E702 blockers for
   the formal `ruff check .` gate.
2. `dbd714e0` — added the explicit
   `A2AMQ_FAULT_INJECTION_ENABLE=local-validation-v1` activation sentinel.
3. `cffb423f` — resilience subprocesses scrub inherited `A2AMQ_FAULT_*`
   variables before applying scenario-specific overrides.
4. `5b718cf6` — removed the unused pytest import introduced in the new fault tests.
5. `b836dbac` — corrected positive/negative enable-sentinel test setup.
6. `75f886b1` — cleared additional E701/E702 statement blockers in six test
   modules found by a repository-wide static sweep.

7. `1c42d1d2`, `33776375` — made alternate manifest output exclusive/no-follow
   and fixed the required validation-error import.
8. `93cc23e1` — bounded alternate production public-key input with the repository's
   regular-file/no-symlink reader.
9. `8f56d720` — bounded integrated deployment JSON inputs and rejected duplicate
   JSON keys through the canonical parser.
10. The branch then advanced concurrently through `4037cf6c` with additional
    local-environment, acceptance-evidence and broker-matrix hardening. Those
    commits remain part of the current rerun boundary and do not inherit the
    earlier exact-SHA local certification.

11. `aa184bd9`, `0f788e74` — source evidence hashing now streams regular
    files and rejects file or directory symlinks under `src/`.
12. `f5890a75`, `e48ef12d`, `cd27472d`, `9dbab1f0` — the independent
    reviewer disables external pytest plugin autoload, hashes evidence without
    following links, and rejects tracked source symlinks.
13. `283a5d67`, `70670da7` — formal acceptance rejects Git provenance
    environment overrides and requires `--root` to be the exact Git top-level.
14. `383bab34`, `f2430a9a`, `64548bd4` — resilience, Workflow and soak
    gates require fresh, distinct evidence outputs and use exclusive final writes
    so previous evidence or symlinks cannot be overwritten.

## What was and was not verified here

The connector audit inspected the committed source and preserved branch history
with fast-forward-only updates. It did not execute pytest, Ruff, mypy, local
native binaries, Workflow, soak, PostgreSQL, Vault or disposable broker gates.

Hosted Actions for the post-certification branch continued to create the expected
jobs but complete before any steps or logs were exposed. This remains pre-runner platform failure,
not repository test evidence.

## Exact rerun boundary & 2026-09-24 Execution Resolution

On 2026-09-24, the exact rerun boundary was executed against branch HEAD starting from `dbc2c78ac1d2ffac3dbf4157269f206c7bb6b707`.
During the Workflow resource gate execution, two issues were diagnosed and resolved:
1. `0.tmp` WAL preallocation file in Dapr Scheduler state inflated durable measurement beyond 64 MiB; resolved by excluding `.tmp` preallocation scratch files from state size calculations in `local_stack_probe.py` and `local_python_probe.py`.
2. Idle CPU overhead from duplicate `ActionRelay` tasks and a tight 100ms reconcile loop; resolved by consolidating tasks and relaxing polling intervals in `local_workflow.py`.
3. Added a drain loop before SQLite integrity assertions in `local_workflow_gate.py` to avoid asynchronous background task races.

These fixes and a deterministic unit regression were committed in `d72112641ee4a43a1a668426402c01e35f316b54`.

All required gates were then cleanly executed and verified on `d72112641ee4a43a1a668426402c01e35f316b54`:
- `tools/check.py`: Passed (0 errors)
- `ruff check .`: Passed (0 errors)
- `mypy src/a2a_mq`: Passed (54 files clean)
- `pip check`: Passed
- `PASS_CORE_LIGHT`: Peak RSS 44.07 MiB, Idle CPU 0.005%, State 0.35 MiB
- `PASS_LOCAL_PYTHON`: Peak RSS 96.67 MiB, Idle CPU 2.73%, State 0.13 MiB
- `PASS_LOCAL_LITE`: Peak RSS 249.88 MiB, Idle CPU 3.07%, State 0.13 MiB, 3 processes
- `PASS_LOCAL_LITE_RESILIENCE`: 113.45s elapsed
- `PASS_LOCAL_WORKFLOW`: Peak RSS 370.69 MiB <= 640 MiB, Idle CPU 4.23% <= 8%, State 61.26 MiB <= 64 MiB, 5 processes
- `PASS_LOCAL_SOAK`: 23.03 minutes, 2688 tasks, +0.10 MiB delta, 0 FD/thread leak
- `a2a_mq.acceptance --mode local`: `LOCAL_PASS` (1100 tests passed)
- Strict broker-independent pytest: 1126 tests passed (0 failures, 0 errors, 0 skipped)
- `a2a_mq.acceptance --mode sdk`: `SDK_PASS` (26 tests passed)
- `scripts/independent_mvp.py --scope local`: `LOCAL_PASS` (1112 tests passed)
- Package wheel gate: `PASSED`
- Implemented `scripts/production_deployment_gate.py` (clean `INCOMPLETE_PREREQUISITE` exit 2 when cluster tools absent)

See [local-certification-20260924.md](local-certification-20260924.md) for full execution evidence.
`PASS_LOCAL_TIER_COMPLETE` is re-established.
`productionCertified` and `upstreamBindingCertified` remain explicitly `false`.
