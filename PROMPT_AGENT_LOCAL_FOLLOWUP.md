# /goal - Verify post-local-pass hardening and advance remaining gates

Start a NEW conversation in `rodhayl/A2A_MQ` on branch
`implementation/production-hardening-20260916`. Read `AGENTS.md`,
`docs/STATUS.md`, `docs/VALIDATION.md`,
`docs/validation/local-run-20260922.md` and this file before changing code.

Record the actual starting SHA. The exact-SHA historical `LOCAL_PASS` belongs to
`3415b10ddcfc5c721f9872c926fead5e11897543`; do not attribute it to a later
HEAD without rerunning acceptance.

## Preserve repository and evidence invariants

- Preserve unrelated user changes.
- Keep `main` untouched.
- No force-push, tag, release or version bump.
- Use small cohesive commits and push this implementation branch.
- Generated evidence, venvs, private binaries, tokens and profiles stay ignored and
  uncommitted.
- A missing prerequisite is UNAVAILABLE/INCOMPLETE, never a synthetic PASS.
- No LLM/model calls belong in deterministic infrastructure validation.
- Prefer regression/root-cause fixes over cosmetic refactors.

## 1. Independently verify the post-3415 hardening first

Inspect the actual diffs after `3415b10`, including the latest acceptance/CI/MCP
hardening. Do not trust commit messages alone.

At minimum run focused regressions covering:

- `tests/test_application_new.py`
- `tests/test_acceptance_new.py`
- `tests/test_ci_contract.py`
- `tests/test_native_adapters_new.py` once pinned SDK dependencies are present
- directly affected MCP/A2A tests
- any additional tests required by defects you find

Important contracts now expected in source:

- static crypto/Vault configuration is validated before mounted token access;
- acceptance rejects model-provider credentials and binds evidence to the same
  clean exact Git HEAD/tree before and after execution;
- FULL_PASS requires repository contracts and an isolated package/wheel smoke;
- package smoke builds from external `git archive HEAD`, uses PEP 517 build
  isolation so the pinned build-system backend is honored, and proves imports
  come from the installed wheel;
- acceptance hashes nested evidence recursively; symlink/special evidence paths
  must make the verdict incomplete rather than silently escape the manifest;
- the formal acceptance runner and independent reviewer use the same model-credential
  boundary, including OpenAI/Anthropic/Google-family plus Mistral, Cohere, Groq,
  DeepSeek, Together, OpenRouter, Hugging Face, xAI, Cerebras and Replicate;
- MCP 2.2 explicitly sets its token-resource validation mode because our
  JWT verifier already enforces exact audience;
- official A2A regression covers cross-owner cancel and subscribe on live tasks;
- CI wheel smoke must not rely on PYTHONPATH under `python -I`.

If a focused test fails, reproduce/root-cause/fix it before broad validation.

## 2. Install and run the pinned official SDK gate

Use the repository-pinned extras in an isolated project venv. Run dependency check,
then:

```bash
python -m a2a_mq.acceptance --mode sdk --root <repo> --output <fresh-external-dir> --timeout 1800
```

The gate must execute the pinned A2A SDK 1.1.2, Dapr Workflow 1.18.3 and MCP 2.2.0
contracts. Warnings are errors. No skipped/missing JUnit may count as PASS.

After the MCP 2.2 tests execute, inspect `src/a2a_mq/mcp_adapter.py` against the
canonical `src/a2a_mq/mcp_server.py`. Production/local composition uses the latter
and ADR 0009 requires request-scoped token revalidation, session-manager lifespan
and disabled legacy sessions. If runtime evidence shows `mcp_adapter.py` is stale
and has no source caller, remove the duplicate adapter/tests and update docs, or
convert it into a thin compatibility shim. Preserve it unchanged if evidence shows
it still owns a distinct supported contract. Do not consolidate by inspection alone.

## 3. Turn Ruff and mypy into actionable defects

Run exactly:

```bash
ruff check .
mypy src/a2a_mq
```

The previous validated run reported 110 Ruff findings outside the clean `src/`
subset and 69 mypy errors across source. Treat those counts only as historical.

Classify the current diagnostics. Fix real defects, undefined names, invalid control
flow, interface mismatches and straightforward typing errors. Avoid broad annotation
churn or suppressions whose only purpose is making the counter reach zero.

Use small commits and rerun the narrow affected checks after each logical fix.

## 4. Re-establish exact-current-SHA local acceptance

After all code changes, ensure the checkout is clean and run:

```bash
python tools/check.py
python -m pip check
python -m a2a_mq.acceptance --mode local --root <repo> --output <fresh-external-dir> --timeout 1800
```

This is mandatory because the historical `LOCAL_PASS` is tied to `3415b10`.

Before deciding whether to skip the expensive resource probes, inspect the raw
`.local-validation` evidence from the `3415b10` run if it still exists. The
committed validation report contains resource-probe command strings that do not
match the CLI accepted by those scripts at `3415b10`, so exact command provenance
is not established by Git history alone.

If the raw JSON/logs and shell history/agent transcript establish valid invocations,
preserve that historical evidence and only rerun resource probes when relevant
runtime/probe bytes changed. If the raw evidence is absent or cannot resolve the
mismatch, rerun core, warmed Python and native local-lite probes once with the
current documented CLI before treating their measurements as reproducible evidence.

Use the actual current CLIs; do not reconstruct the old transcription:

```bash
"$PY" "$REPO/scripts/local_resource_probe.py" \
  --state-dir "$STATE/core-probe" \
  --json "$EVIDENCE/core-resources.json"

"$PY" "$REPO/scripts/local_python_probe.py" \
  --state-dir "$STATE/python-app" \
  --json "$EVIDENCE/python-app-resources.json"

TOOLS="$STATE/tools"
"$PY" "$REPO/scripts/bootstrap_local_tools.py" --output "$TOOLS" \
  > "$EVIDENCE/bootstrap-local-tools.json"
export PATH="$TOOLS/bin:$PATH"
"$PY" "$REPO/scripts/check_local_native_versions.py" \
  --daprd "$TOOLS/bin/daprd" \
  --nats-server "$TOOLS/bin/nats-server" \
  --bootstrap-manifest "$TOOLS/manifest.json" \
  --json "$EVIDENCE/native-versions-bootstrap.json"

PROFILE="$STATE/local-lite"
"$PY" -m a2a_mq.local_runtime prepare "$PROFILE" \
  > "$EVIDENCE/local-prepare.json"
"$PY" "$REPO/scripts/local_stack_probe.py" \
  --profile "$PROFILE" \
  --tools-dir "$TOOLS" \
  --json "$EVIDENCE/local-stack-resources.json" \
  --log "$EVIDENCE/local-stack.log"
```

Require the exact PASS verdict expected by each probe and its exit code 0.
For the native stack probe, require `nativeToolsBound=true` and exact equality of
`bootstrapManifestSha256` plus every `binarySha256` entry between the native-version
JSON and stack-resource JSON. This proves the measured children are the same private
executables whose retained release archives were verified. Any mismatch stays
`INCOMPLETE_NATIVE_PREREQ`; a measurement failure is not a resource PASS.

## 5. Execute real external gates when prerequisites exist

Attempt, in order and without mock substitution:

1. PostgreSQL verified-TLS;
2. Vault Transit;
3. disposable JetStream/RabbitMQ only when container use is explicitly allowed;
4. full acceptance only when every required prerequisite is real.

Record unavailable prerequisites exactly.

## 6. Hosted CI

Inspect the latest Actions run. Recent jobs have shown `runner_id: 0`, empty
runner name and zero steps, which is a pre-runner platform failure. Change workflow
source only when a concrete repository-side defect is evidenced.

## Deliverable

Return:

1. starting and final SHA;
2. commits created/pushed;
3. focused tests and exact results;
4. SDK gate result;
5. Ruff/mypy diagnostics fixed vs remaining;
6. exact-current-SHA local acceptance result and evidence-manifest integrity;
7. external gate results/unavailable prerequisites;
8. hosted CI state;
9. confirmation that `main` and version remain unchanged;
10. the single highest-value remaining action.

Keep source presence, historical evidence, current-SHA executed evidence and
unavailable infrastructure clearly separated.
