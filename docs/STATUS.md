# Source and acceptance status - 2026-09-25

## Current branch

Branch: `implementation/production-hardening-20260916`.

The end-to-end production deployment acceptance and full acceptance gate execution is recorded in
`docs/validation/production-certification-20260925.md`:
- Formal full acceptance source at `45c459c632977143b8681c055847775e88d7ad97` (clean tree `23fdbcdf30adedf984c631b8a060442cc97b426a`).
- Certified Kubernetes application image built from `6458195b171020ece1911e68bc6d14998d216164` (clean tree `ed9af9e6c04a3b6bba1f0880ca40d7f9a7f4e758`).

That execution established:

- **Formal Acceptance (`a2a_mq.acceptance`) Verdict: `FULL_PASS`**:
  - `local`: 1150 tests passed (0 failures, 0 errors, 0 skipped, 0 unraisable exception warnings);
  - `sdk`: 26 tests passed (official upstream SDK & canonical MCP contracts);
  - `postgres`: 4 tests passed against real verified-TLS PostgreSQL 16 (`sslmode=verify-full`);
  - `vault`: 1 test passed against real TLS HashiCorp Vault Transit;
  - `repository-check`, `dependency-check`, `lint` (Ruff), `types` (Mypy): 100% clean passes;
  - `package`: clean isolated wheel build, package install, and import smoke passed;
  - `jetstream`: 12 live probe checks passed on disposable NATS JetStream container stack;
  - `rabbitmq`: 12 live probe checks passed on disposable RabbitMQ container stack.
- **Production Deployment Gate (`scripts/production_deployment_gate.py`) Verdict: `PRODUCTION_DEPLOYMENT_PASS`**:
  - Executed in `executionMode: "live"` against a 3-node Kubernetes v1.31.0 cluster with Dapr 1.18.3 control plane and 3-node NATS StatefulSet;
  - Target application image: `a2amq-prod:6458195-ca@sha256:a54c969183a0c314c77df93c7761db53de19dea8da9e9b20ddebccec425958af`;
  - All 19 required production checks passed (0 missing, 0 incomplete, 0 failed);
  - Running container image digest verified across all application pods (`a2amq-api`, `a2amq-worker`, `a2amq-dlq`);
  - Pod security context, Dapr sidecar injection, PDB enforcement, PostgreSQL state store attachment, authenticated E2E task lifecycle, cross-owner/tenant 403 denial, authenticated MCP query, workflow worker restart with UID change, DLQ routing and replay, rolling restart zero-loss, and 135-second JetStream broker outage recovery verified;
  - Clean automated post-run resource deletion (0 remaining gate resources).
- **Parity Boundary**:
  - RabbitMQ real live acceptance: `PASS` (12 checks in container stack).
  - JetStream Kubernetes production deployment: `PASS` (19 checks on live multi-node Kubernetes cluster).
  - RabbitMQ Kubernetes production deployment: not separately executed.
- **Certification Booleans**: `productionCertified: false` and `upstreamBindingCertified: false` strictly preserved.

The preceding local-tier certification is recorded in
`docs/validation/local-certification-20260924.md` (validated at `d72112641ee4a43a1a668426402c01e35f316b54`), establishing `PASS_LOCAL_TIER_COMPLETE`, `PASS_LOCAL_LITE_RESILIENCE`, `PASS_LOCAL_WORKFLOW`, `PASS_LOCAL_SOAK`, `PASS_CORE_LIGHT`, `PASS_LOCAL_PYTHON`, and `PASS_LOCAL_LITE`.

The Dapr Scheduler WAL scratch investigation and gate implementation details are documented in
[production-gate-audit-20260924.md](validation/production-gate-audit-20260924.md).

`main` remains strictly unchanged at
`66c17153d7ec75b9ad85808ef141b9dba43a9d91`.

Package version remains strictly **0.1.0**. No merge to main or release is implied by this branch.

## Current implementation

The source now contains substantially more than the 2026-09-17 reference service.

### Lightweight local product

The default local product is intentionally the small three-process topology:

- one Python A2A-MQ process;
- one native `daprd`;
- one native `nats-server`;
- SQLite application state.

PostgreSQL, Redis, Docker, Kubernetes, Vault, OTLP, Placement and Scheduler are
not default local-lite requirements.

Implemented local support includes:

- `a2a_mq.local_lite:from_env`;
- `a2amq-local` prepare/run launcher;
- atomic private profile generation;
- bounded/fail-closed profile and secret reads;
- explicit loopback-port preflight;
- explicit Dapr internal gRPC port;
- least-privilege child environments;
- core, Python-product and native-stack resource probes;
- peak/steady RSS, CPU and per-role native measurements;
- private pinned native bootstrap for Dapr 1.18.3 all-components and
  NATS Server 2.15.0;
- bootstrap provenance, SHA-256 and exact archive/binary-size evidence.

The optional local-workflow tier adds Placement + Scheduler only when native Dapr
Workflow semantics are required. It remains separate from local-lite economics.

### Distributed / production-like source

Source is present for the role-separated integrated stack and related production
capabilities, including:

- SQLite and PostgreSQL stores;
- Dapr Pub/Sub with JetStream and RabbitMQ profiles;
- Dapr Workflow;
- standard A2A JSON-RPC / HTTP+JSON bridge and official SDK contracts;
- canonical authenticated MCP server;
- owner/tenant authorization and cancellation/continuation/approval flows;
- independent observers and operations/DLQ/replay paths;
- Vault Transit payload protection;
- observability/operations modules;
- deployment generators and production environment factories;
- backup/restore and retention/maintenance support.

Presence of source is not equivalent to a passed external acceptance gate.

## Important hardening added in this iteration

The current branch also includes focused fixes around:

- local resource-measurement correctness and warmed Python-product economics;
- Dapr/NATS bootstrap provenance and native-tooling cost evidence;
- local secret isolation and deterministic port ownership;
- production PostgreSQL DSN wiring for non-root Kubernetes pods;
- public health endpoints compatible with Kubernetes probes while callbacks stay
  app-token protected;
- Dapr Kubernetes secret-store behavior with ServiceAccount token automount disabled;
- canonical MCP runtime use in production composition;
- PostgreSQL acceptance DSN alignment and explicit disposable-CI insecure override;
- MCP marker classification and MCP 2.x `httpx2` prerequisite;
- official SDK cancellation and MCP authentication-denial coverage;
- integrated Dapr runtime/control-plane alignment on **1.18.3**;
- required Vault startup validation;
- independent send/read authorization capabilities;
- capacity reservation before durable stream admission;
- SQLite backup destination symlink-race protection.

### Post-wrap-up source hardening

A follow-up source audit after the status wrap-up found and fixed additional
deterministic issues without changing the settled local-first architecture:

- integrated Kubernetes pods no longer retain unnecessary runtime ServiceAccount
  credentials, and injected Dapr sidecars disable the unused built-in Kubernetes
  secret-store API (`37d7974f`);
- task snapshot streams terminate cleanly at both `INPUT_REQUIRED` and
  `AUTH_REQUIRED` pauses rather than timing out while awaiting approval
  (`a1d18e97`);
- formal acceptance PASS verdicts are bound to a clean exact Git commit, and an
  unsafe/unreadable PostgreSQL gate file now fails closed with structured evidence
  (`03ff8ddf`, `21fb5406`);
- the alternate production environment accepts normal libpq conninfo containing
  spaces from both secret environment variables and private DSN files while still
  rejecting ambiguous/control-character input (`e341e850`, `4840091f`).

These post-wrap-up commits were independently audited and verified on the local host.
Six defects and regressions were identified and remediated across commits `3ba3216`,
`26f8205`, `c25bcb4`, `e4a1692`, `bf8713b`, and `3415b10`:

- fixed a syntax error (`SyntaxError: unmatched '}'`) in `deploy/production/generate.py`
  and extended `tools/check.py` AST syntax checking across scripts, deploy, and tools (`3ba3216`);
- increased `TaskAPI` wait timeouts in fixtures (`test_task_api.py`, `test_mcp_tools_new.py`,
  `test_bridge_sdk.py`) from 0.1-0.2s to 5.0s to accommodate physical ext4 fsync latency (`26f8205`, `3415b10`);
- corrected binary digest tamper verification logic in `test_local_native_versions.py` (`c25bcb4`);
- added missing imports (`require` in `backup.py`, `PermanentError` in `test_operations_v2.py`) (`e4a1692`);
- fixed crypto config schema validation order in `application.py:_protectors` before reading the secret
  token file, and increased loop timeouts in `test_application_new.py` (`bf8713b`);
- restored workflow step IDs in `ci.yml`, aligned `test_ci_contract.py` with `mcp` extra, and aligned
  `test_service.py` with unauthenticated `/health/ready` probe access and `DaprPublisher` closed state (`3415b10`).

### Second follow-up audit after the validated local run

A source-level audit after the executed `3415b10` validation added further
hardening through `a3d7020a`. These changes have **not** inherited the historical
LOCAL_PASS; they require rerun on their exact final SHA.

The follow-up changes include:

- strict CI contract restoration for the canonical MCP dependency set
  (`cef60bdc`);
- complete static crypto/Vault configuration validation before mounted-token
  inspection (`b0cd527f`);
- broader model-credential rejection in formal acceptance (`d68b67db`);
- pre/post clean Git HEAD+tree binding for acceptance evidence, closing a
  provenance TOCTOU gap (`64cf9db3`);
- explicit MCP 2.2 resource-validation mode, avoiding its deprecation warning
  under the SDK gate's `-W error` policy (`eb287f49`);
- official A2A-client regression coverage for cross-owner cancel/subscribe on
  live tasks (`6a89959c`);
- repository-contract checking as a mandatory FULL_PASS gate (`979d8c66`);
- an isolated CI wheel smoke that proves imports come from the installed wheel
  rather than the editable checkout (`dc7a8161`);
- package/supply-chain smoke as a formal FULL_PASS gate, built from an external
  `git archive HEAD`, plus regression coverage that pins that isolation
  (`c66964af`, `a3d7020a`).

Exact pinned upstream source was reviewed for A2A SDK 1.1.2, Dapr Workflow
1.18.3 and MCP 2.2.0 while making these changes. No external SDK, PostgreSQL,
Vault or broker gate is claimed as executed by that review.

Additional acceptance/supply-chain hardening after `a3d7020a` includes:

- recursive evidence hashing across nested package/broker output, with symlink or
  special-file evidence paths forcing an incomplete verdict (`796f2a3f`);
- alignment of the formal and independent model-credential guards with additional
  current provider credentials including xAI, Cerebras and Replicate (`b5460d44`);
- PEP 517 build isolation for CI and the formal package gate so the exact
  `setuptools==82.0.1` build-system pin is honored rather than an arbitrary host
  backend (`70ad2ece`).

Further source/harness hardening after `70ad2ece` includes:

- optional Workflow validation now uses separately pinned official Dapr 1.18.3
  Placement/Scheduler assets while local-lite retains its two-binary footprint;
  retained release archives, manifests and extracted binaries are cross-bound by
  SHA-256 provenance (`7c317b75`, `ba3bd5d9`, `3c7dfba1`, `09b003fb`);
- the resource probe now requires a prior matching `PASS_NATIVE_VERSION` report
  for the exact manifest/binary fingerprints and emits structured
  `INCOMPLETE_MEASUREMENT` evidence when native provenance preflight fails
  (`8a374e64`, `79383583`);
- production input validation was tightened for Kubernetes names, ingress/broker/
  Redis endpoints and egress CIDRs, without broadening default egress
  (`6eb41a67`, `b7ea2862`);
- central DLQ replay now safely reconstructs and revalidates the original Dapr
  CloudEvent/envelope and refuses truncated or identifier/topic-mismatched payloads
  (`90af24fe`);
- Workflow HTTP startup no longer blocks on runtime connection, while fatal
  background-task failure now drives liveness to 503 for both Workflow and API
  critical loops so Kubernetes can restart a dead worker/relay
  (`3839f543`, `afb56ed6`, `b30da821`);
- disposable full-stack acceptance removes generated JWT/TLS/private broker/database
  credentials after a successful Compose teardown; explicit `--keep-failed`
  remains the opt-in diagnostic retention path (`eec02c99`).

These changes are source/harness hardening only. They have not inherited the
historical `LOCAL_PASS`, resource observations or any external gate result and
require focused/current-SHA execution.

### Latest source audit after `1f63ca73`

A further connector-based source audit preserved all existing branch commits and
added narrowly scoped fixes through `9dafa7d5`:

- the alternate production operations surface now replays central-DLQ Dapr
  CloudEvents through the same validation/two-phase replay contract as
  `CentralDLQ.replay_dlq`, and the legacy `OperationsManager` exposes the same
  split replay interface (`22683c86`);
- operational metrics expose a thread-safe snapshot required by the central-DLQ
  metrics endpoint (`12e967e1`);
- MCP, A2A bridge, Dapr HTTP transport and broker provisioner entrypoints reject
  malformed hosts, invalid ports and whitespace before third-party SDK/client
  construction (`29eda348`, `16d37ccf`, `58a87775`);
- disposable Compose generation is fail-clean and no-overwrite: failed generation
  removes its own partial credential tree, dangling output symlinks are preserved,
  and a concurrently occupied destination is never replaced
  (`c4c06fa0`, `9bb3081b`, `9dafa7d5`).

The corresponding regression tests were added/extended in
`test_operations_v2.py`, `test_mcp_adapter.py`, `test_mcp_tools_new.py`,
`test_bridge_sdk.py`, `test_transport.py` and `test_deployment_new.py`.
This audit environment could inspect and push the private repository through the
GitHub connector but could not clone it in the execution container because DNS
resolution for `github.com` failed. Therefore these latest commits are
**source-reviewed but not locally executed here**; they require the focused and
exact-SHA gates in `PROMPT_AGENT_LOCAL_FOLLOWUP.md`.

Hosted Actions for these commits still created the three expected jobs but each
job completed with zero steps and no runner execution. That remains platform-side
pre-runner failure evidence, not a repository test result.


### Post-certification audit after `7a667a4f`

A connector-based audit after the completed local certification added narrowly
scoped hardening through `75f886b1`:

- removed known E701/E702 blockers that made the formal `ruff check .` FULL_PASS
  gate impossible even when all external infrastructure was available
  (`711f83fb`, `75f886b1`);
- required an explicit validation-only sentinel before fault-injection mode,
  record or synchronization hooks can activate (`dbd714e0`);
- scrubbed inherited `A2AMQ_FAULT_*` variables from resilience child-process
  environments so a parent shell cannot contaminate an allegedly disarmed run
  (`cffb423f`);
- removed a new unused-import lint defect and corrected the sentinel regression
  tests themselves (`5b718cf6`, `b836dbac`).

These changes were source-reviewed through the GitHub connector. They were not
executed in this connector environment. The latest hosted Actions run for
`75f886b1` again created `postgres`, `local (3.12)` and `local (3.13)`
jobs with no steps or logs, so hosted CI remains pre-runner/unexecuted evidence.

The current branch therefore requires focused regressions plus exact-SHA local
revalidation before `PASS_LOCAL_TIER_COMPLETE` can be reattached to the new HEAD.


Subsequent deployment/acceptance hardening advanced the branch through
`4037cf6c`. This includes exclusive/no-follow alternate manifest output, bounded
public-key and integrated JSON config inputs, plus later concurrent
local-environment, evidence-hashing and broker-matrix hardening. These commits are
inside the same current-SHA rerun boundary; no historical PASS is promoted to the
new HEAD without execution.


The same post-certification audit subsequently hardened source/provenance and
local-tier evidence handling through `64548bd4`: source hashes reject symlinked
source trees, the independent reviewer is plugin-isolated and no-follow, formal
acceptance rejects Git redirection and non-root checkout provenance, and
resilience/Workflow/soak outputs are fresh and exclusive. These changes also
require exact-SHA rerun before current HEAD can carry the local certification.

## Evidence that exists

Historical evidence must stay attached to the SHA it actually tested.

The 2026-09-22 validation report for HEAD `3415b10ddcfc5c721f9872c926fead5e11897543`
is documented in `docs/validation/local-run-20260922.md`. A later audit found that
the three recorded resource-probe command lines do not match the CLI accepted by
those scripts at `3415b10`. Accordingly, the resource figures below are reported
historical observations pending raw-evidence inspection or rerun; they are not
currently exact-command reproducible evidence from repository history:

1. **`PASS_CORE_LIGHT`:** Pure-core Python + SQLite economic probe executed 100 tasks;
   peak RSS 44.22 MiB (<= 192 MiB budget), idle CPU 0.002% (<= 2.0% budget), state DB 0.134 MiB.
2. **`PASS_LOCAL_PYTHON`:** Warmed Python-product probe (`a2a_mq.local_lite:from_env`)
   warmed peak RSS 88.66 MiB (<= 192 MiB budget), 60s idle CPU 2.467% (<= 3.0% budget).
3. **Selected local regressions:** All 11 test files (118 tests total) passed with exit code 0
   and strict JUnit verification.
4. **`PASS_NATIVE_VERSION`:** Repository-pinned native bootstrap verified Dapr 1.18.3
   all-components (`c644d6db...`) and NATS Server 2.15.0 (`0cce2896...`).
5. **`PASS_LOCAL_LITE`:** Real native 3-process stack probe (Python ASGI + daprd + nats-server)
   measured peak RSS 242.61 MiB (<= 384 MiB budget; daprd 130.0 MiB, python 89.9 MiB, nats 22.7 MiB),
   idle CPU 2.85% (<= 5.0% budget), functional roundtrip verified.
6. **Packaging and contracts:** `tools/check.py` passed, wheel built and verified outside source
   tree in clean venv (`a2a_mq-0.1.0-py3-none-any.whl`), `ruff check src/` passed (0 errors).
7. **`LOCAL_PASS`:** The separate broker-independent acceptance gate is reported
   as having executed and passed all 929 tests (0 failures, 0 errors, 0 skipped;
   exit code 0) on `3415b10`. Its command shape is compatible with the acceptance
   CLI and is distinct from the resource-probe transcription issue.

## Distributed and external gates status

The distributed and external infrastructure gates were fully authorized, executed, and passed on 2026-09-25 against clean HEAD `6458195b171020ece1911e68bc6d14998d216164` (evidence in `docs/validation/production-certification-20260925.md`):

1. **PostgreSQL verified-TLS gate:** **`PASSED`** (4 tests passed using real verified-TLS PostgreSQL 16 with `sslmode=verify-full`).
2. **Vault Transit gate:** **`PASSED`** (1 test passed using real TLS HashiCorp Vault Transit engine).
3. **Disposable JetStream / RabbitMQ gates:** **`PASSED`** (all 12 live probe checks passed on disposable NATS JetStream container stack and all 12 checks passed on RabbitMQ container stack).
4. **Full Acceptance Gate (`a2a_mq.acceptance`):** **`FULL_PASS`** (all 11 sub-gates passed with exit code 0).
5. **Production Deployment Gate (`scripts/production_deployment_gate.py`):** **`PRODUCTION_DEPLOYMENT_PASS`** (all 19 checks passed on live 3-node Kubernetes v1.31.0 cluster with Dapr 1.18.3 and NATS JetStream StatefulSet).

## Hosted CI status

Recent GitHub Actions push runs complete as failures in roughly five seconds.
The raw job records show `runner_id: 0`, an empty runner name and `steps: []`;
job log retrieval returns no logs. This is failure before runner assignment, not
evidence that repository tests executed and failed. The platform-side reason for
the missing runner is not exposed by the available interface.

Hosted CI therefore remains **not passed** and must be rerun/diagnosed separately.

## Original snapshot recovery

The historical staged handoff at
`e80096da84190a8af9aa044a4bd97b0123fca4ad` expected bundle SHA-256
`47a2c5418ec35d0d1c5fbcbb0fa4cd502b8479760b935a3b94ed01efe93965e9`
and final tree `79521dec0000e246baaab5bf4129841aa4799914`.

Accessible recovery fragments contain literal ellipsis corruption/incompleteness;
that exact tree has not been checksum-verified or reconstructed. Do not fabricate
or silently substitute recovery bytes. Current implementation work proceeds from
the accessible tested branch instead.

## Next machine handoff

Use `PROMPT_AGENT_LOCAL_FOLLOWUP.md` for the next bounded owner-machine run.
It is intentionally local-first, venv-based and non-containerized until the
lightweight gates pass.
