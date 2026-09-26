# Implementation guidance

Read these before changing the repository:

1. `docs/README.md`
2. `docs/STATUS.md`
3. `docs/VALIDATION.md`
4. the relevant ADR under `docs/adr/`

Everything under `docs/archive/` is historical context, not current instruction.

## Release invariants

- Keep package version at `0.1.0` until an explicit release/version decision.
- Do not modify protected `main` as part of branch hardening.
- Never convert missing/skipped infrastructure into PASS.
- Bind acceptance evidence to exact source SHA/tree.
- Bind deployment evidence to exact image digest.
- Keep `productionCertified=false` and `upstreamBindingCertified=false` unless
  their actual certification mechanisms complete.
- Keep `spec/envelope.schema.json` and `src/a2a_mq/schema.json` byte-identical.

## Architecture invariants

Default local-lite remains exactly:

- one Python A2A-MQ process;
- one native `daprd`;
- one native `nats-server`;
- SQLite application state.

Do not pull PostgreSQL, Redis, Kubernetes, Vault, OTLP, Placement, Scheduler or
multiple app roles into local-lite merely for convenience. Workflow-only semantics
belong to the optional Workflow tier.

Preserve tenant/consumer namespacing, immutable message IDs, fencing, transactional
outbox behavior, terminal guards and explicit replay authorization. Do not claim
generic exactly-once external effects.

## Validation discipline

Run the narrowest relevant tests first. A source change invalidates prior SHA-bound
evidence; rebuilding a container invalidates the old image-digest deployment
evidence until the new image is exercised.

Do not repeat expensive resource/soak gates for documentation-only changes.
Hosted CI's current pre-runner failure is not a repository test failure.

## Documentation discipline

Keep active docs concise and current. Put superseded plans, prompts and historical
execution reports under `docs/archive/`. ADRs are decision records and may retain
historical state descriptions; `docs/STATUS.md` is the current source of truth.
