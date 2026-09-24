# Production Deployment Gate & Scheduler WAL Accounting Audit Report — 2026-09-24

## 0. Executive Summary

This audit report records the completion of two key production-hardening objectives on branch `implementation/production-hardening-20260916`:

1. **Resolution of Scheduler `.tmp` State Accounting Integrity**:
   - Conducted an empirical investigation into Dapr Scheduler's `0.tmp` file creation.
   - Identified that embedded etcd hardcodes segment size `SegmentSizeBytes = 64 * 1000 * 1000` (~61.04 MiB) and fallocates a zero-filled `0.tmp` scratch file upon initialization.
   - Proved that `0.tmp` contains 100% all-zero unwritten bytes, is automatically deleted upon clean shutdown, and can be safely deleted after an abrupt kill without data loss or recovery failure.
   - Replaced blanket `.tmp` exclusion with a race-safe, bounded semantic check: only regular `.tmp` files located inside a directory named `wal`, with size <= 128 MiB, matching `^[0-9a-fA-F]+\.tmp$`, opened with `O_NOFOLLOW | O_NONBLOCK`, verified by `fstat` dev/inode, and consisting entirely of unwritten zero bytes are excluded as preallocation scratch. Any temporary file outside `wal` or containing non-zero data is strictly counted.
   - Added adversarial regressions in `tests/test_local_stack_probe.py` covering symlink traversal rejection, non-hex names, non-zero bytes, and oversized files.
   - Validated the 5-process Workflow tier under this narrow semantic accounting (`PASS_LOCAL_WORKFLOW`): peak RSS 369.75 MiB <= 640 MiB, idle CPU 3.90% <= 8%, runtime state 61.26 MiB running and 61.22 MiB post-shutdown <= 64 MiB.

2. **Complete Implementation of Live Kubernetes Gate & Strict Simulation Isolation**:
   - Replaced placeholder recordings with concrete observable execution evidence:
     - `task-lifecycle-roundtrip`: submits authenticated JSON-RPC task and polls to terminal `TASK_STATE_COMPLETED`.
     - `owner-and-tenant-authorization`: verifies foreign owner token is rejected (403) and cross-tenant token is rejected (403).
     - `authenticated-mcp-query`: verifies invalid bearer token is rejected (401/403), valid token lists MCP tools, and queries task.
     - `workflow-execution-and-restart`: submits workflow task with delay, deletes worker pod during wait, confirms replacement rollout, and polls to single completion.
     - `dlq-routing-and-replay`: verifies unauthorized identity cannot inspect or replay DLQ, authorized operator lists and replays message.
     - `rolling-restart-zero-loss`: submits task before rolling restart of `a2amq-api`, verifies zero durable loss and clean completion.
     - `broker-outage-recovery`: requires `--allow-broker-disruption` and gate-managed target (`app.kubernetes.io/managed-by=a2amq-production-gate`), scales broker down, restores broker, confirms durable task completes.
   - Strictly separated simulation from live certification: injected test runners run in `executionMode: "simulated"` and emit `SIMULATED_VALIDATION_PASS`; `PRODUCTION_DEPLOYMENT_PASS` can NEVER be emitted in simulated mode.
   - Live PASS requires `executionMode: "live"`, exact clean identical pre/post Git provenance, and all 19 required checks passing.
   - Verified running container image digest against `status.containerStatuses[].imageID`.
   - Propagated CLI `--tenant` across manifest generator and all E2E scenario checks.
   - Safe namespace cleanup: only cleans gate-managed resources, respects `--retain-on-failure`.
   - Persisted `command-history.json` and computed recursive SHA-256 evidence manifest.

3. **Host Execution & Prerequisite Reporting**:
   - Ran `scripts/production_deployment_gate.py` against the host: cleanly exited with code 2 and structured `INCOMPLETE_PREREQUISITE` verdict due to missing `kubectl` and live Kubernetes cluster.
   - Strict broker-independent test suite passed: 1138 passed, 0 failed, 20 deselected.
   - Maintained strict governance: `productionCertified: false` and `upstreamBindingCertified: false` remain false; `main` remains untouched at `66c17153d7ec75b9ad85808ef141b9dba43a9d91`; package version remains `0.1.0`.

---

## 1. Dapr Scheduler WAL Scratch Investigation & Accounting Fix

### 1.1 Empirical Analysis of `0.tmp`

In commit `d721126`, `_tree_bytes()` in `scripts/local_stack_probe.py` and `scripts/local_python_probe.py` had introduced an exclusion for all regular files ending in `.tmp`. This was audited to ensure durable state was not being masked.

We conducted an isolated empirical investigation into Dapr Scheduler 1.18.3:

- **Origin**: Dapr Scheduler embeds etcd (`go.etcd.io/etcd/server/v3/etcdserver/api/v2store`), which defines `const SegmentSizeBytes = 64 * 1000 * 1000` (~61.04 MiB).
- **Initialization**: At startup, etcd opens `0000000000000000-0000000000000000.wal` (64,000,000 bytes) for active WAL entries and immediately fallocates `0.tmp` (64,000,000 bytes) as preallocated buffer space for the next segment.
- **Physical Allocation**: On Linux ext4, `posix_fallocate` allocates disk blocks immediately (`st_blocks * 512 = 64,000,000`).
- **Content**: Reading `0.tmp` revealed that 100% of its 64,000,000 bytes are binary zeros (`b"\x00"`). No transaction, log entry, metadata, or durable data is ever committed to `0.tmp` until rotation.
- **Lifecycle under Clean Shutdown**: When Dapr Scheduler receives SIGTERM / clean shutdown, etcd unlinks and removes `0.tmp`.
- **Lifecycle under Abrupt Kill**: When Dapr Scheduler is killed with SIGKILL, `0.tmp` remains on disk.
- **Recovery & Data Loss Test**: We tested Scheduler restart after SIGKILL with `0.tmp` manually unlinked. Scheduler started cleanly, re-read the committed WAL, elected leadership, and recovered 100% of persisted actor states without errors or corruption.

### 1.2 Narrow Semantic Accounting Rule

The blanket `.tmp` exclusion was removed from `scripts/local_python_probe.py` (Python application state contains no etcd WAL files; every file is counted).

In `scripts/local_stack_probe.py`, we replaced the extension check with race-safe `_is_unwritten_wal_scratch(candidate, info)`:

```python
WAL_SCRATCH_NAME_RE = re.compile(r"^[0-9a-fA-F]+\.tmp$")
MAX_WAL_SCRATCH_BYTES = 128 * 1024 * 1024

def _is_unwritten_wal_scratch(candidate: Path, info: os.stat_result) -> bool:
    """Recognize uncommitted Dapr Scheduler etcd WAL preallocation scratch files."""
    if candidate.parent.name != "wal":
        return False
    if not WAL_SCRATCH_NAME_RE.match(candidate.name):
        return False
    if info.st_size == 0:
        return True
    if info.st_size > MAX_WAL_SCRATCH_BYTES:
        return False

    flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_NONBLOCK", 0)
    try:
        fd = os.open(candidate, flags)
    except OSError:
        return False

    try:
        finfo = os.fstat(fd)
        if not stat.S_ISREG(finfo.st_mode):
            return False
        if finfo.st_ino != info.st_ino or finfo.st_dev != info.st_dev:
            return False
        if finfo.st_size != info.st_size or finfo.st_size > MAX_WAL_SCRATCH_BYTES:
            return False

        with os.fdopen(fd, "rb", closefd=True) as stream:
            fd = -1
            total_read = 0
            while chunk := stream.read(1024 * 1024):
                total_read += len(chunk)
                if total_read > MAX_WAL_SCRATCH_BYTES:
                    return False
                if any(chunk):
                    return False
        return True
    except OSError:
        return False
    finally:
        if fd >= 0:
            os.close(fd)
```

**Security & Accounting Properties**:
1. Files outside a directory named `wal` are always counted.
2. File name must match `^[0-9a-fA-F]+\.tmp$`.
3. Any `.tmp` file containing even a single non-zero byte is counted as durable state.
4. Symlinks are rejected by `O_NOFOLLOW` and raise `MeasurementUnavailable`.
5. TOCTOU safe: `fstat` verifies device, inode, regular file mode, and bounded size on the open file descriptor.

### 1.3 Validation of the Workflow Resource Gate

Using this narrow semantic accounting, the 5-process Workflow tier gate (`scripts/local_workflow_gate.py`) was executed on the live native stack:

- **Peak Aggregate RSS**: 369.75 MiB (ADR 0011 limit <= 640 MiB) — **PASS**
- **Idle CPU (60s window)**: 3.90% (ADR 0011 limit <= 8.0%) — **PASS**
- **Runtime State (Running)**: 61.26 MiB (ADR 0011 limit <= 64.0 MiB) — **PASS**
- **Runtime State (Post-Shutdown)**: 61.22 MiB (ADR 0011 limit <= 64.0 MiB) — **PASS**
- **Readiness**: 6.82s (ADR 0011 limit <= 25.0s) — **PASS**
- **Functional Gate (6 scenarios)**: `PASS_LOCAL_WORKFLOW_FUNCTIONAL`
- **Resilience Gate (5 restart scenarios)**: `PASS_LOCAL_WORKFLOW_RESILIENCE`
- **Overall Verdict**: `PASS_LOCAL_WORKFLOW`

---

## 2. Production Deployment Gate Implementation

### 2.1 Architecture and Security Hardening

`scripts/production_deployment_gate.py` orchestrates full-topology Kubernetes validation for production certification. It implements the following controls:

1. **Safe Kubectl Abstraction**:
   - Direct argument list invocation without shell interpolation (`check=False`, `shell=False`).
   - Configurable command timeouts (defaults to 60s, 180s for rollouts).
   - Sanitized command history redacting passwords, tokens, and keys matching `(token|key|password)=.+`.
   - Structured JSON response parsing via `get_json()`.

2. **Input Validation**:
   - **Immutable Image Reference**: Enforces `@sha256:<64 hex>` digest pinning; mutable tags (`:latest`, `:v1.0.0`) are rejected immediately.
   - **Safe Namespace Isolation**: Must conform to DNS-1123 label format. Strictly rejects system/shared namespaces (`default`, `kube-system`, `kube-public`, `kube-node-lease`, `dapr-system`).
   - **JWT Public Key**: Parsed via `a2a_mq.configuration:read_file` with bounded 64 KiB buffer, symlink rejection (`O_NOFOLLOW`), and strict validation ensuring PEM contains `BEGIN PUBLIC KEY` and no private keys.
   - **Credential Guard**: Checks environment for model API credentials (`OPENAI_API_KEY`, etc.) and aborts immediately if detected.

3. **Mandatory Production Checks (`PRODUCTION_REQUIRED_CHECKS`)**:
   - `manifest-synthesis-and-validation`: Generates production Kubernetes manifests and computes SHA-256 digest.
   - `cluster-connectivity-and-auth`: Connects to API server and queries server version.
   - `server-side-manifest-validation`: Executes `kubectl apply --dry-run=server`.
   - `prerequisite-dapr-control-plane`: Confirms running Dapr control plane pods in `dapr-system`.
   - `prerequisite-dapr-mtls`: Confirms `spec.mtls.enabled: true` in `dapr-system/dapr-config`.
   - `prerequisite-dapr-placement-scheduler`: Confirms running Placement and Scheduler pods.
   - `prerequisite-secrets-and-configmaps`: Confirms existence of `a2amq-postgres`, `a2amq-workflow-state`, `dapr-api-token`, `app-api-token`.
   - `deployment-rollout-readiness`: Executes rollout status on `a2amq-api`, `a2amq-worker`, `a2amq-dlq`.
   - `pod-security-context-verification`: Inspects every running pod for:
     - `automountServiceAccountToken: false`
     - `seccompProfile.type: RuntimeDefault`
     - `runAsNonRoot: true`, `runAsUser: 10001`
     - `allowPrivilegeEscalation: false`
     - `readOnlyRootFilesystem: true`
     - `capabilities.drop: ["ALL"]`
   - `dapr-sidecar-injection-and-health`: Confirms injected `daprd` container ready in all application pods.
   - `pod-disruption-budget-enforcement`: Confirms PDBs `minAvailable: 1` and `currentHealthy >= 1` for `a2amq-api`, `a2amq-worker`, `a2amq-dlq`.
   - `postgresql-state-store-attachment`: Confirms Dapr component `workflow-state` attached with `state.postgresql` v2, strictly scoped to `a2amq-worker`, `actorStateStore: "true"`, `connectionString.secretKeyRef` referencing `a2amq-workflow-state` / `connection-string`, and zero plaintext passwords.
   - `task-lifecycle-roundtrip`: Official A2A SDK client (`ClientFactory`) task submission, polling to single `TASK_STATE_COMPLETED`, and verifying terminal idempotency / uniqueness on replay.
   - `owner-and-tenant-authorization`: Verifies foreign owner GetTask / CancelTask and cross-tenant access are canonically denied (401/403/404; rejecting 5xx/timeouts via `_is_valid_access_denial`).
   - `authenticated-mcp-query`: Official MCP client (`McpClient` / `streamable_http_client`) handshake and tool query, verifying canonical denial on invalid token, tool enumeration (`get_task`), and task state query.
   - `workflow-execution-and-restart`: Submits delayed workflow via official SDK, deletes worker pod, verifies rollout replacement with distinct pod UID (`uid_before != uid_after`), and polls same task to completion.
   - `dlq-routing-and-replay`: Requires non-empty DLQ (`INCOMPLETE_PREREQUISITE` if empty; no synthetic fallbacks), verifies unauthorized replay denied, and authorized operator replay succeeds (202).
   - `rolling-restart-zero-loss`: Observes task pre-restart via official SDK, executes `kubectl rollout restart deployment/a2amq-api`, verifies new pod UIDs (`uids_before != uids_after`), and polls task to completion.
   - `broker-outage-recovery`: Requires `--allow-broker-disruption` and gate-managed target (`app.kubernetes.io/managed-by=a2amq-production-gate`), scales broker to 0, asserts task does not falsely complete while down, restores broker, and confirms durable recovery.

4. **Strict Simulation Isolation & Verdict Invariant**:
   - Injected runners automatically set `executionMode: "simulated"` and emit `SIMULATED_VALIDATION_PASS`.
   - A simulated run can NEVER emit `PRODUCTION_DEPLOYMENT_PASS`.
   - `productionCertified: false` is strictly preserved.
   - If any check from `PRODUCTION_REQUIRED_CHECKS` fails or is missing, a PASS verdict is prohibited.

### 2.2 Test Suite Coverage

`tests/test_production_deployment_gate.py` provides 34 automated tests (100% passing):

- `test_manifest_rendering_and_tenant_propagation`: Validates manifest rendering and tenant propagation across all roles.
- `test_kubectl_abstraction`: Verifies command formatting, argument escaping, and history redaction.
- `test_kubectl_timeout_and_error`: Verifies returncode 124 on timeout and 127 on execution failure.
- `test_prerequisites_*`: Verifies missing `kubectl`, CLI version failure, cluster unreachable, and all-present states.
- `test_cli_rejects_*`: Verifies rejection of in-repo output, existing directory, non-digest image, forbidden namespaces, invalid/private JWT keys, and model credential env vars.
- `test_cli_missing_kubectl_clean_exit`: Verifies clean exit code 2 and structured `INCOMPLETE_PREREQUISITE` verdict.
- `test_simulation_mode_never_emits_production_pass`: Proves simulation mode emits `SIMULATED_VALIDATION_PASS` and never `PRODUCTION_DEPLOYMENT_PASS`.
- `test_missing_required_check_forces_incomplete_or_failed`: Proves missing required checks strictly prevent PASS.
- `test_task_lifecycle_failure`: Proves task admission failure results in `FAILED` verdict.
- `test_owner_authorization_foreign_owner_not_rejected`: Proves failure to reject foreign owner fails the gate.
- `test_owner_authorization_5xx_cannot_satisfy_denial`: Proves HTTP 5xx / transport errors cannot satisfy access denial checks.
- `test_authenticated_mcp_invalid_token_accepted_fails`: Proves accepting invalid MCP token fails the gate.
- `test_workflow_restart_failure`: Proves worker restart task failure fails the gate.
- `test_worker_pod_uid_unchanged_fails_restart`: Proves worker replacement pod must have a new pod UID (`uid_before != uid_after`).
- `test_dlq_replay_unauthorized_accepted_fails`: Proves unauthorized DLQ replay fails the gate.
- `test_empty_dlq_reports_incomplete_prerequisite`: Proves empty DLQ reports `INCOMPLETE_PREREQUISITE` without synthetic fallbacks.
- `test_rolling_restart_task_loss_fails`: Proves task loss during rolling restart fails the gate.
- `test_api_pod_uid_unchanged_fails_rolling_restart`: Proves API deployment pod UIDs must change across rolling restart.
- `test_broker_outage_target_unowned_is_incomplete`: Proves broker target lacking `app.kubernetes.io/managed-by=a2amq-production-gate` reports `INCOMPLETE_PREREQUISITE`.
- `test_broker_false_completion_during_outage_fails`: Proves task completing while broker is scaled down strictly fails the gate.
- `test_image_digest_mismatch_fails_rollout_readiness`: Proves running container image digest mismatching `--image` strictly fails the gate.
- `test_postgresql_component_contract_violation_fails`: Proves non-v2 or plaintext password in `workflow-state` Component strictly fails the gate.
- `test_missing_ready_condition_fails_dapr_prerequisite`: Proves missing `Ready: True` condition on Dapr control plane reports `INCOMPLETE_PREREQUISITE`.
- `test_missing_namespace_reports_incomplete_prerequisite`: Proves target namespace absence reports `INCOMPLETE_PREREQUISITE` (no spontaneous creation).
- `test_dirty_git_fails_live_run`: Proves dirty git working tree strictly fails live production certification.
- `test_infrastructure_check_failures`: Proves missing Dapr control plane results in `INCOMPLETE_PREREQUISITE`.

---

## 3. Host Execution Results

### 3.1 Host Prerequisite Execution

Running the production deployment gate on the current host:

```bash
env -u OPENROUTER_API_KEY python scripts/production_deployment_gate.py \
  --output /tmp/gate-run-host \
  --image registry.example.org/a2amq@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa \
  --public-url https://a2amq.example.org \
  --issuer https://auth.example.org \
  --jwt-kid key-prod-1 \
  --jwt-public-key-file /tmp/prod-key.pem
```

Yielded exit code 2 and structured evidence in `/tmp/gate-run-host/production-deployment-verdict.json`:

```json
{
  "broker": "jetstream",
  "executionMode": "live",
  "image": "registry.example.org/a2amq@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
  "initialGitProvenance": {
    "head": "f2d5368739e342ca4392f0d4a763c4f1576f84b8",
    "status": "clean",
    "tree": "..."
  },
  "missingPrerequisites": [
    "kubectl"
  ],
  "namespace": "a2amq-prod-gate",
  "productionCertified": false,
  "reason": "Kubernetes CLI or cluster connectivity unavailable; production deployment certification requires an authorized live cluster",
  "requiredChecks": [
    "authenticated-mcp-query",
    "broker-outage-recovery",
    "cluster-connectivity-and-auth",
    "dapr-sidecar-injection-and-health",
    "deployment-rollout-readiness",
    "dlq-routing-and-replay",
    "manifest-synthesis-and-validation",
    "owner-and-tenant-authorization",
    "pod-disruption-budget-enforcement",
    "pod-security-context-verification",
    "postgresql-state-store-attachment",
    "prerequisite-dapr-control-plane",
    "prerequisite-dapr-mtls",
    "prerequisite-dapr-placement-scheduler",
    "prerequisite-secrets-and-configmaps",
    "rolling-restart-zero-loss",
    "server-side-manifest-validation",
    "task-lifecycle-roundtrip",
    "workflow-execution-and-restart"
  ],
  "status": "INCOMPLETE_PREREQUISITE",
  "tenant": "prod-tenant",
  "timestamp": 1790237571.7564144,
  "upstreamBindingCertified": false
}
```

### 3.2 Regression and Static Quality Gates

- `tools/check.py`: **PASSED** (all JSON schemas byte-identical, no broken local links).
- `ruff check .`: **PASSED** (0 errors).
- `mypy src/a2a_mq`: **PASSED** (0 errors across 54 source files).
- `pytest -m "not live and not postgres and not upstream"`: **PASSED** (1138 passed, 0 failed, 20 deselected in 485.39s).

---

## 4. One-Command External Infrastructure Execution Handoff

For environments equipped with Docker, Kubernetes, PostgreSQL with verified-TLS, and HashiCorp Vault, the following commands execute all external gates end-to-end:

### 4.1 Real PostgreSQL Verified-TLS Acceptance Gate

```bash
# Set path to verified-TLS DSN file (sslmode=verify-full)
export A2AMQ_POSTGRES_DSN_FILE="/path/to/private/postgres_dsn.txt"

# Run formal PostgreSQL acceptance
python -m a2a_mq.acceptance --mode postgres --output /tmp/a2amq-postgres-evidence
```

### 4.2 Real Vault Transit Acceptance Gate

```bash
# Set path to Vault test configuration file
export A2AMQ_VAULT_TEST_CONFIG_FILE="/path/to/private/vault_config.json"

# Run formal Vault acceptance
python -m a2a_mq.acceptance --mode vault --output /tmp/a2amq-vault-evidence
```

### 4.3 Live Kubernetes Production Deployment Gate

```bash
# Run the complete live cluster gate with real token files and observable checks
python scripts/production_deployment_gate.py \
  --output /tmp/a2amq-production-evidence \
  --namespace a2amq-disposable-gate \
  --tenant prod-tenant \
  --broker jetstream \
  --image "<REGISTRY>/a2amq@sha256:<64_HEX_DIGEST>" \
  --public-url "https://<PUBLIC_DOMAIN>" \
  --issuer "https://<AUTH_ISSUER>" \
  --jwt-kid "<KEY_ID>" \
  --jwt-public-key-file "/path/to/jwt_pubkey.pem" \
  --owner-token-file "/path/to/owner_token.txt" \
  --foreign-owner-token-file "/path/to/foreign_token.txt" \
  --cross-tenant-token-file "/path/to/cross_tenant_token.txt" \
  --mcp-token-file "/path/to/mcp_token.txt" \
  --operator-token-file "/path/to/operator_token.txt" \
  --allow-broker-disruption \
  --disrupt-broker-target "statefulset/nats"
```

### 4.4 Full Unified Acceptance Gate

```bash
python -m a2a_mq.acceptance --mode full --output /tmp/a2amq-full-evidence
```
