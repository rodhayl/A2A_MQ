# Usability/onboarding post-merge audit — 2026-09-27

## Scope

This addendum independently audits the repository state after the Linux validation
record in [usability-onboarding-20260927.md](usability-onboarding-20260927.md).
It does not replace or rewrite that original validation evidence.

## Repository state at audit start

- `main`: `8dcc243f983fc4ee96b1343b55d3f6f6236181fa`
- validation merge commit: `84097baa57da4919c1672a0392397e05c0404882`
- final tested usability branch commit: `401471b05da55ba8311b9bc6e5019a24a4250b9b`
- original post-RC2 main parent: `97d15da659e4e51f94d403148dae08edb279f1bf`
- package version: `0.1.0`

The merge commit has exactly two parents:

1. `97d15da659e4e51f94d403148dae08edb279f1bf`
2. `401471b05da55ba8311b9bc6e5019a24a4250b9b`

The next commit, `8dcc243f983fc4ee96b1343b55d3f6f6236181fa`, is documentation-only and
adds/indexes the Linux validation report.

## Validation-time code fix

The only commit added by the Linux validator after the originally handed-off branch
head `8f99db2db642117d089f05ed6975838dd5f6accf` was:

`401471b05da55ba8311b9bc6e5019a24a4250b9b` —
`fix(local): allow client JWT reading in doctor preflight without size truncation`.

Its complete production-code delta is limited to `src/a2a_mq/local_runtime.py`:

- `_read(path)` became `_read(path, limit=512)`;
- ordinary local secrets retain the 512-byte default;
- `_client_token_check()` reads generated `client.jwt` with a 65,536-byte bound.

This is a causal fix for generated RS256 JWTs larger than 512 bytes. It does not
relax the default secret-reader bound.

The existing regression test
`test_local_doctor_is_non_mutating_and_reports_ready_profile` prepares a real local
profile, invokes `doctor`, requires `ready`, and confirms the generated JWT remains
unchanged. That path exercises the defect that the validator found.

## Linux evidence reviewed

The original validator record reports:

- `python -m pip check` — no broken requirements;
- `python tools/check.py` — PASS;
- focused local/runtime/production-env/Queue-Lab suites — 93 passed;
- broker-independent repository suites — 1,162 passed;
- Local-lite native provenance — `PASS_NATIVE_VERSION`;
- Workflow native provenance — `PASS_NATIVE_VERSION`;
- positive and negative `doctor` cases;
- real three-process Local-lite startup and `PASS_LOCAL_SMOKE`;
- real API example requests;
- real five-process Workflow startup/ready;
- clean shutdown with no owned processes or checked runtime ports left open.

Those runtime/test statements are evidence supplied by the Linux validation run. This
post-merge repository audit did not independently rerun that Linux environment.

The validation did not relabel live PostgreSQL, TLS Vault Transit or Kubernetes
deployment evidence as tests of the usability branch. Those broader gates remain
bound to their earlier recorded source/image.

## Hosted GitHub Actions

Hosted CI cannot currently corroborate the Linux run because jobs fail before runner
assignment.

Observed runs:

| Source | Run | Result |
| --- | ---: | --- |
| final tested usability branch `401471b...` | `36305318006` | pre-runner failure |
| merge commit `84097ba...` | `36306201233` | pre-runner failure |
| validation-report commit `8dcc243...` | `36308979807` | pre-runner failure |

For all three, jobs report:

- `runner_id=0`;
- empty `runner_name`;
- `steps=[]`.

No repository test step executed. These runs are therefore neither code failures nor
passes.

## Integration method

No pull request for `implementation/usability-onboarding-20260927` was found in the
repository. Integration was performed as a normal two-parent merge commit rather than
a squash or history rewrite.

The original validation prompt preferred a pull request but made successful validated
integration and preserved history the functional requirement. The direct merge does
not alter the tested branch commit or RC tags.

## Release invariants

Verified after the merge/report commit:

- package version remains `0.1.0`;
- `v0.1.0-rc1` tag object remains
  `9bc710ed96b625fd68774a77d0542a3a11786601`, pointing to
  `3d2b2afd2dbbeb447dab21d1e71afb428e523e8f`;
- `v0.1.0-rc2` tag object remains
  `26faf43fcc41221d42e1e6f4829ea4e5c99e56b4`, pointing to
  `4b1fc34065e37b68258f1944fbcb1e9a15f5fe6d`;
- no release tag was moved or recreated.

## Documentation correction identified by this audit

Two live documents still described the usability branch as separate from `main`
after it had already been validated and merged:

- `README.md`;
- `docs/STATUS.md`.

This audit corrects those statements and updates the hosted-CI boundary in
`docs/STATUS.md` and `docs/VALIDATION.md`. No runtime, protocol, dependency or
release-tag change is required.

## Audit conclusion

The Linux validator completed the required functional usability/onboarding work and
found/fixed one real bounded-read defect before integration. The repository state and
merge provenance agree with the validator's core claims.

No additional production-code fix is demonstrated by this post-merge audit. The only
remaining work identified here is documentation/tracing cleanup, performed in the
same post-merge audit change.
