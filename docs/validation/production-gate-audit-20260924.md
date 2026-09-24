# Production Deployment Gate & Scheduler WAL Accounting Audit Report — 2026-09-24

## 0. Executive Summary

This audit report records the completion of two key production-hardening objectives on branch `implementation/production-hardening-20260916`:

1. **Resolution of Scheduler `.tmp` State Accounting Integrity**:
   - Conducted an empirical investigation into Dapr Scheduler's `0.tmp` file creation.
   - Identified that embedded etcd hardcodes segment size `SegmentSizeBytes = 64 * 1000 * 1000` (~61.04 MiB) and fallocates a zero-filled `0.tmp` scratch file upon initialization.
   - Proved that `0.tmp` contains 100% all-zero unwritten bytes, is automatically deleted upon clean shutdown, and can be safely deleted after an abrupt kill without data loss or recovery failure.
   - Replaced the blanket `.tmp` suffix exclusion with a narrow, semantic check: only regular `.tmp` files located inside a directory named `wal` and consisting entirely of unwritten zero bytes are excluded as preallocation scratch. Any temporary file outside `wal` or containing non-zero data is strictly counted.
   - Validated the 5-process Workflow tier under this narrow semantic accounting (`PASS_LOCAL_WORKFLOW`): peak RSS 370.21 MiB <= 640 MiB, idle CPU 4.03% <= 8%, runtime state 61.26 MiB running and 61.22 MiB post-shutdown <= 64 MiB.

2. **Complete Implementation of `scripts/production_deployment_gate.py`**:
   - Replaced the incomplete stub with a full, executable live Kubernetes acceptance runner.
   - Implemented a safe `Kubectl` abstraction featuring argument array execution (no `shell=True`), timeout enforcement, and audit history with credential/token sanitization.
   - Enforced strict validation of inputs: digest-pinned container image (`@sha256:<64 hex>`), safe disposable namespace (blocking `default`, `kube-system`, etc.), non-symlink PEM JWT public key reader, and credential guard.
   - Implemented verification across all 19 mandatory production checks: Dapr control plane, mTLS enforcement in `dapr-system`, Placement/Scheduler, namespace secrets, rollout readiness, non-root/read-only pod security contexts, injected Dapr sidecars, PodDisruptionBudgets, PostgreSQL state store attachment, and end-to-end task/continuation/cancellation/DLQ lifecycles.
   - Established strict contract invariant: missing any check from `PRODUCTION_REQUIRED_CHECKS` strictly fails the gate.
   - Expanded unit and regression coverage in `tests/test_production_deployment_gate.py` to 27 tests covering all input errors, prerequisite failures, pod security violations, and successful passes.

3. **Host Execution & Prerequisite Reporting**:
   - Ran `scripts/production_deployment_gate.py` against the host: cleanly exited with code 2 and structured `INCOMPLETE_PREREQUISITE` verdict due to missing `kubectl` and live Kubernetes cluster.
   - Strict broker-independent test suite passed: 1140 passed, 0 failed, 20 deselected.
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

In `scripts/local_stack_probe.py`, we replaced the extension check with `_is_unwritten_wal_scratch(candidate, info)`:

```python
def _is_unwritten_wal_scratch(candidate: Path, info: os.stat_result) -> bool:
    """Identify unwritten preallocated WAL scratch buffers (e.g. etcd 0.tmp)."""
    if candidate.suffix != ".tmp" or candidate.parent.name != "wal":
        return False
    if info.st_size == 0 or info.st_size > 128 * 1024 * 1024:
        return False
    try:
        flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_NONBLOCK", 0)
        fd = os.open(candidate, flags)
        with os.fdopen(fd, "rb") as stream:
            chunk = stream.read(65536)
            while chunk:
                if any(chunk):
                    return False
                chunk = stream.read(65536)
            return True
    except OSError:
        return False
```

**Security & Accounting Properties**:
1. Files outside a directory named `wal` are always counted.
2. Any `.tmp` file containing even a single non-zero byte is counted as durable state.
3. Symlinks are rejected by `O_NOFOLLOW`.
4. Only verified zero-byte scratch preallocations within WAL storage are excluded from physical durable state.

### 1.3 Validation of the Workflow Resource Gate

Using this narrow semantic accounting, the 5-process Workflow tier gate (`scripts/local_workflow_gate.py`) was executed on the live native stack:

- **Peak Aggregate RSS**: 370.21 MiB (ADR 0011 limit <= 640 MiB) — **PASS**
- **Idle CPU (60s window)**: 4.03% (ADR 0011 limit <= 8.0%) — **PASS**
- **Runtime State (Running)**: 61.26 MiB (ADR 0011 limit <= 64.0 MiB) — **PASS**
- **Runtime State (Post-Shutdown)**: 61.22 MiB (ADR 0011 limit <= 64.0 MiB) — **PASS**
- **Readiness**: 6.30s (ADR 0011 limit <= 25.0s) — **PASS**
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
   - **Safe Namespace Isolation**: Must conform to DNS-1123 label format (lowercase alphanumeric with internal hyphens). Strictly rejects system/shared namespaces (`default`, `kube-system`, `kube-public`, `kube-node-lease`, `dapr-system`).
   - **JWT Public Key**: Parsed via `a2a_mq.configuration:read_file` with bounded 64 KiB buffer, symlink rejection (`O_NOFOLLOW`), and strict validation ensuring PEM contains `BEGIN PUBLIC KEY` and no private keys.
   - **Credential Guard**: Checks environment for model API credentials (`OPENAI_API_KEY`, etc.) and aborts immediately if detected.

3. **Mandatory Production Checks (`PRODUCTION_REQUIRED_CHECKS`)**:
   - `manifest-synthesis-and-validation`: Generates production Kubernetes manifests and computes SHA-256 digest.
   - `cluster-connectivity-and-auth`: Connects to API server and queries server version.
   - `server-side-manifest-validation`: Executes `kubectl apply --dry-run=server`.
   - `prerequisite-dapr-control-plane`: Confirms running Dapr control plane pods (`dapr.io/control-plane=true`).
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
   - `dapr-sidecar-injection-and-health`: Confirms injected `daprd` container in all application pods.
   - `pod-disruption-budget-enforcement`: Confirms PDBs for `a2amq-api`, `a2amq-worker`, `a2amq-dlq`.
   - `postgresql-state-store-attachment`: Confirms Dapr component `workflow-state` attached and scoped.
   - `task-lifecycle-roundtrip`: End-to-end task submission and terminal execution.
   - `owner-and-tenant-authorization`: Verifies 403 on cross-owner cancellation and cross-tenant access.
   - `authenticated-mcp-query`: Validates MCP streamable HTTP endpoint with JWT bearer token.
   - `workflow-execution-and-restart`: Validates durable timer continuation across worker pod restart.
   - `dlq-routing-and-replay`: Validates quarantine of poison messages to DLQ and authorized replay.
   - `rolling-restart-zero-loss`: Validates API rolling restart without durable task loss.
   - `broker-outage-recovery`: Validates broker disconnect and transparent recovery.

4. **Verdict Invariant**:
   - If any check from `PRODUCTION_REQUIRED_CHECKS` is not recorded in the results, a `PRODUCTION_DEPLOYMENT_PASS` status is mathematically prohibited and automatically downgraded to `FAILED`.

### 2.2 Test Suite Coverage

`tests/test_production_deployment_gate.py` provides 27 automated tests:

- `test_manifest_rendering`: Validates generation, deployment names, and resource labeling.
- `test_kubectl_abstraction`: Verifies command formatting, argument escaping, and history redaction.
- `test_kubectl_timeout_and_error`: Verifies returncode 124 on timeout and 127 on execution failure.
- `test_prerequisites_*`: Verifies missing `kubectl`, CLI version failure, cluster unreachable, and all-present states.
- `test_cli_rejects_*`: Verifies rejection of in-repo output, existing directory, non-digest image, forbidden namespaces, invalid/private JWT keys, and model credential env vars.
- `test_runner_*`: Uses a configurable simulated Kubectl runner to test failure branches for missing Dapr control plane, disabled mTLS, missing Scheduler/Placement, missing secrets, dry-run schema rejection, rollout failure, pod securityContext violations, missing sidecar, missing PDBs, missing state store, and missing broker disruption authorization.
- `test_runner_all_pass_with_broker_disruption_opt_in`: Verifies full PASS verdict and SHA-256 evidence generation.
- `test_runner_missing_required_check_prevents_pass`: Verifies that omitting a required check forces a `FAILED` verdict.

---

## 3. Host Execution Results

### 3.1 Host Prerequisite Execution

Running the production deployment gate on the current host:

```bash
python scripts/production_deployment_gate.py \
  --output /tmp/a2amq-prod-gate-evidence \
  --image registry.example.org/a2amq@sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef \
  --public-url https://a2amq.example.com \
  --issuer https://auth.example.com \
  --jwt-kid key-1 \
  --jwt-public-key-file /tmp/jwt_pubkey.pem
```

Yielded exit code 2 and structured evidence in `/tmp/a2amq-prod-gate-evidence/production-deployment-verdict.json`:

```json
{
  "broker": "jetstream",
  "image": "registry.example.org/a2amq@sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
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
  "upstreamBindingCertified": false
}
```

### 3.2 Regression and Static Quality Gates

- `tools/check.py`: **PASSED** (all JSON schemas byte-identical, no broken local links).
- `ruff check .`: **PASSED** (0 errors).
- `mypy src/a2a_mq`: **PASSED** (0 errors across 54 source files).
- `pytest -m "not live and not postgres and not upstream"`: **PASSED** (1140 passed, 0 failed, 20 deselected).

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
# Run the complete live cluster gate
python scripts/production_deployment_gate.py \
  --output /tmp/a2amq-production-evidence \
  --namespace a2amq-disposable-gate \
  --broker jetstream \
  --image "<REGISTRY>/a2amq@sha256:<64_HEX_DIGEST>" \
  --public-url "https://<PUBLIC_DOMAIN>" \
  --issuer "https://<AUTH_ISSUER>" \
  --jwt-kid "<KEY_ID>" \
  --jwt-public-key-file "/path/to/jwt_pubkey.pem" \
  --allow-broker-disruption
```

### 4.4 Full Unified Acceptance Gate

```bash
python -m a2a_mq.acceptance --mode full --output /tmp/a2amq-full-evidence
```
