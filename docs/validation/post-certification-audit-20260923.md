# Post-certification source audit — 2026-09-23

## Provenance

- Branch: `implementation/production-hardening-20260916`
- Last completed local-tier certification documentation head: `7a667a4f`
- Exact validated source recorded by that certification: `1dd1938dff5cbf58a803ec23617143b5d998465e`
- Source/test/harness hardening reviewed here: through `4037cf6c36f014f2cbf91d841bf92861bdc074b6`
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

## What was and was not verified here

The connector audit inspected the committed source and preserved branch history
with fast-forward-only updates. It did not execute pytest, Ruff, mypy, local
native binaries, Workflow, soak, PostgreSQL, Vault or disposable broker gates.

Hosted Actions for the post-certification branch continued to create the expected
jobs but complete before any steps or logs were exposed. This remains pre-runner platform failure,
not repository test evidence.

## Exact rerun boundary

Before reattaching current-SHA local certification, execute at minimum:

- focused tests for fault injection, resilience harness and every lint-only test
  module changed after certification;
- `ruff check .`;
- `mypy`;
- formal exact-SHA `a2a_mq.acceptance --mode local`;
- local-lite resilience gate, because both the hooks and its harness changed;
- optional Workflow functional/resilience gate and soak gate if the goal is to
  restore the full `PASS_LOCAL_TIER_COMPLETE` verdict to the new exact SHA.

External verified-TLS PostgreSQL, Vault Transit and dual real-broker gates remain
separate prerequisites for `FULL_PASS`.
