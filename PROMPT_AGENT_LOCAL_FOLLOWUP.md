# /goal - Verify the published local audit, without overloading the host

Start a NEW conversation in `rodhayl/A2A_MQ`. Read `AGENTS.md`, this file and
`docs/validation/local-audit-20260922.md`. The publication branch is
`implementation/production-hardening-20260916`; record the actual SHA being tested.

## Preserve work and the host

Fetch the branch through the already configured authorized remote. Preserve local
uncommitted work and the earlier unpushed fixes `b3d17fc` / `8d541e2`; their equivalent
fixes are already upstream. Do not reset, force-push or blindly cherry-pick them.
When history diverges, use a separate clean worktree of the fetched branch and
leave the original checkout intact. Do not claim a SHA passed if subsequent fixes
changed its tested bytes.

Use the existing project-private venv when it is suitable, or create a new ignored
venv without system-site-packages. Every Python/pip command uses its absolute
executable. No sudo, global/user package installation, Docker, PostgreSQL, Redis,
Vault, additional services, model calls, broad extras or parallel test jobs.
Keep caches, fresh state and sanitized evidence in a new ignored
`.local-validation/` run directory on disk. Keep at least 512 MiB host memory
headroom and 2 GiB free disk; stop workloads if these reserves cannot be maintained.
Do not install a new interpreter or alter swap/system configuration.

## First: core viability

Use the core prerequisites in `requirements/core-probe.txt`; do not install all
extras before measuring. From the clean source root, with PY set to the venv's
absolute interpreter and STATE/EVIDENCE to fresh private directories:

```bash
umask 077
"$PY" scripts/local_resource_probe.py \
  --state-dir "$STATE/core-probe" --json "$EVIDENCE/core-resources.json"
```

Capture the exit code and inspect JSON. Continue only on exit 0 AND
`PASS_CORE_LIGHT`. `INCOMPLETE_MEASUREMENT`, `INCOMPLETE_CORE_PROBE` and
`INCOMPLETE_HOST_HEADROOM` are blockers to economic acceptance, not evidence of
cheap execution or proof of a code defect. Preserve reports; choose fresh paths
for retries. Keep the default budgets unchanged. The probe's sleeping idle CPU is
not the running application's idle CPU; host-wide swap is not process attribution.

## Then: selected regressions, one file at a time

Check installed dependencies in the venv. Reuse them; never perform an all-extras
upgrade to make collection pass. The profile-generation tests need PyJWT and
cryptography: if unavailable, record that subset INCOMPLETE and continue eligible
files rather than installing a broad SDK bundle. The six candidate files are:

```text
tests/test_local_resource_probe.py
tests/test_local_resource_probe_regressions.py
tests/test_local_runtime.py
tests/test_local_launcher_regressions.py
tests/test_local_stack_probe.py
tests/test_local_stack_probe_regressions.py
```

Use the following command separately for each TEST, with a unique STEM. Use an
existing timeout utility or a bounded agent execution call (300 seconds per file).

```bash
PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 PYTHONDONTWRITEBYTECODE=1 PYTHONNOUSERSITE=1 \
OMP_NUM_THREADS=1 OPENBLAS_NUM_THREADS=1 MKL_NUM_THREADS=1 \
"$PY" -m pytest -p pytest_asyncio.plugin -p no:cacheprovider -o addopts= \
  -o asyncio_default_fixture_loop_scope=function -q -W error --maxfail=1 "$TEST" \
  --basetemp="$STATE/tmp/$STEM" --junitxml="$EVIDENCE/$STEM.xml"
"$PY" scripts/assert_junit_complete.py "$EVIDENCE/$STEM.xml"
```

Preserve pytest's own exit code as well as JUnit. Expected count at the recorded
code commit is 74; evaluate actual collected tests, not a hard-coded count or
historical PASS. A timeout, import failure, skip, xfail or missing report is not a
passing selected gate. Fix only reproducible defects, add a regression first,
rerun the affected files and make a focused local commit. No cosmetic refactoring.

## Native economics remains a separate conditional gate

Only after the core and selected tests pass, follow Phase 2 in
`PROMPT_AGENT_LOCAL_TEST.md`. The owner has authorized the repository's pinned
private bootstrap for this validation: if exact native binaries are not already
available, run `scripts/bootstrap_local_tools.py` into this run's
`$STATE/tools` only, prepend that private `bin/` to PATH, and require
`scripts/check_local_native_versions.py` to return `PASS_NATIVE_VERSION` before
starting the stack. This is not a host installation and must not write outside the
ignored run directory. A platform/download/checksum/version failure remains
`INCOMPLETE_NATIVE_PREREQ` or `INCOMPLETE_NATIVE_VERSION`.

Do not launch Workflow, containers, PostgreSQL, Redis, Vault, RabbitMQ, collectors
or any alternative service as a workaround. Run only one local-lite stack and
stop/reap only its owned processes afterward.

For a real stack run, record actual binary versions, startup/idle peak RSS,
PID/start-time identities, CPU and flushed disk footprint. Missing readings or
process death during observation cannot pass. Keep the native 3-process measurement
separate from core numbers and from synthetic process fixtures. Never substitute
rough Dapr/NATS estimates for measurements or remove Dapr on that basis.

## Deliverable

Write a compact `LOCAL_VALIDATION_REPORT.md`: actual starting/final SHA, venv,
commands/exits/JUnit counts, core JSON, confirmed fixes, existing work preserved,
resource limits and unexecuted gates. Keep raw evidence private and uncommitted;
exclude tokens/private keys, venvs and generated profiles from any shared bundle.
Report independently: CORE ECONOMICS, SELECTED REGRESSION, NATIVE LOCAL-LITE
ECONOMICS, and FULL ACCEPTANCE. The last remains INCOMPLETE without its real gates.
Do not push, merge or release unless the owner separately authorizes it.
