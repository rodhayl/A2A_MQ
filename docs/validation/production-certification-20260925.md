# Production Certification & Full Acceptance Report — 2026-09-25

## 0. Executive Summary

This validation record documents the end-to-end production certification and full acceptance gate execution for A2A-MQ on branch `implementation/production-hardening-20260916`:

- **Exact Git Commit**: `6458195b171020ece1911e68bc6d14998d216164` (clean working tree).
- **Git Tree SHA**: `ed9af9e6c04a3b6bba1f0880ca40d7f9a7f4e758`
- **Application Image Reference**: `a2amq-prod:6458195-ca@sha256:a54c969183a0c314c77df93c7761db53de19dea8da9e9b20ddebccec425958af`
- **Formal Acceptance Gate (`a2a_mq.acceptance`) Verdict**: `FULL_PASS` (11/11 sub-gates passed).
- **Production Deployment Gate (`scripts/production_deployment_gate.py`) Verdict**: `PRODUCTION_DEPLOYMENT_PASS` (19/19 checks passed on live 3-node Kubernetes cluster with Dapr 1.18.3 and NATS JetStream).
- **Protected `main` Invariant**: `66c17153d7ec75b9ad85808ef141b9dba43a9d91` strictly untouched.
- **Package Version**: strictly `0.1.0`.

---

## 1. Formal Acceptance Gate (`a2a_mq.acceptance`) — `FULL_PASS`

Executed with `--mode full --allow-disposable-containers` in an isolated environment against live infrastructure:
- **Verified-TLS PostgreSQL 16**: SSL verification with CA bundle (`sslmode=verify-full`).
- **TLS HashiCorp Vault Transit**: Authenticated transit encryption / decryption.
- **Real NATS JetStream**: Disposable container stack executing full 12-check live probe.
- **Real RabbitMQ**: Disposable container stack executing full 12-check live probe.

### Gate Results Summary

| Sub-Gate | Tests / Scope | Status | Notes |
| :--- | :--- | :--- | :--- |
| `local` | 1149 tests | **PASSED** | 0 failures, 0 errors, 0 skipped |
| `sdk` | 26 tests | **PASSED** | Upstream SDK & canonical MCP contracts |
| `postgres` | 4 tests | **PASSED** | Verified TLS connection (`sslmode=verify-full`), schema migration, ACID transactions |
| `vault` | 1 test | **PASSED** | TLS Vault Transit authenticated payload cipher/decipher |
| `repository-check` | Static check | **PASSED** | Clean repo tree, license/metadata verification |
| `dependency-check` | Requirements | **PASSED** | Bounded version verification |
| `lint` | Ruff | **PASSED** | 0 lint errors |
| `types` | Mypy | **PASSED** | Strict typing across all packages |
| `package` | Wheel & Smoke | **PASSED** | Isolated build, install, import smoke |
| `jetstream` | 12 live checks | **PASSED** | JetStream live probe with unique CloudEvent IDs across hold windows |
| `rabbitmq` | 12 live checks | **PASSED** | RabbitMQ live probe with quorum queues and dead-lettering |

**Overall Acceptance Status**: `FULL_PASS`
**Verdict File**: `/tmp/a2amq-full-evidence/verdict.json`

---

## 2. Production Deployment Gate (`scripts/production_deployment_gate.py`) — `PRODUCTION_DEPLOYMENT_PASS`

Executed against a live 3-node disposable Kubernetes v1.31.0 cluster (`a2amq-cluster`) configured with Dapr 1.18.3 control plane (Operator, Sentry mTLS, Sidecar Injector, Placement, and a 3-node Scheduler cluster), backed by a 3-node NATS StatefulSet (`nats-0`, `nats-1`, `nats-2`) in namespace `a2amq-prod-gate`.

### 19 Required Checks

| # | Check Name | Status | Details |
| :---: | :--- | :---: | :--- |
| 1 | `manifest-synthesis-and-validation` | **PASSED** | Rendered and validated secret-free manifests tagged with run ID `gate-536f235d` |
| 2 | `cluster-connectivity-and-auth` | **PASSED** | Verified authenticated connection to Kubernetes v1.31.0 |
| 3 | `server-side-manifest-validation` | **PASSED** | Server-side dry-run accepted all workload, PDB, and service manifests |
| 4 | `prerequisite-dapr-control-plane` | **PASSED** | All 7 Dapr control-plane pods active and Ready (version 1.18.3) |
| 5 | `prerequisite-dapr-mtls` | **PASSED** | Verified `spec.mtls.enabled=true` in `dapr-system/dapr-config` |
| 6 | `prerequisite-dapr-placement-scheduler` | **PASSED** | Placement (1 pod) and Scheduler (3 pods) present and Ready in `dapr-system` |
| 7 | `prerequisite-secrets-and-configmaps` | **PASSED** | Required secrets (`a2amq-postgres`, `a2amq-workflow-state`, `dapr-api-token`, `app-api-token`) bound |
| 8 | `deployment-rollout-readiness` | **PASSED** | Deployments `a2amq-api`, `a2amq-worker`, `a2amq-dlq` rolled out and running target image digest `sha256:a54c969183a0c314c77df93c7761db53de19dea8da9e9b20ddebccec425958af` |
| 9 | `pod-security-context-verification` | **PASSED** | Verified non-root (UID 10001), drop ALL capabilities, read-only rootfs, `RuntimeDefault` seccomp |
| 10 | `dapr-sidecar-injection-and-health` | **PASSED** | `daprd` sidecar injected and 2/2 Ready on all 6 application pods |
| 11 | `pod-disruption-budget-enforcement` | **PASSED** | PDBs `minAvailable=1` and `currentHealthy>=1` verified for api, worker, and dlq roles |
| 12 | `postgresql-state-store-attachment` | **PASSED** | `state.postgresql` v2 `workflow-state` attached, scoped to worker, actorStateStore enabled |
| 13 | `task-lifecycle-roundtrip` | **PASSED** | End-to-end task lifecycle verified via official `a2a.client` SDK; idempotent re-admission confirmed |
| 14 | `owner-and-tenant-authorization` | **PASSED** | Foreign owner access rejected (403 Forbidden); cross-tenant access denied with canonical status |
| 15 | `authenticated-mcp-query` | **PASSED** | Streamable HTTP MCP tools query authenticated via official client (6 tools discovered); invalid token rejected |
| 16 | `workflow-execution-and-restart` | **PASSED** | Workflow entered non-terminal wait, survived worker pod kill/restart (`uid_before != uid_after`), and completed cleanly |
| 17 | `dlq-routing-and-replay` | **PASSED** | Poison message routed to DLQ; unauthorized replay rejected; authorized operator replayed message and downstream verified with `replayedAt` timestamp |
| 18 | `rolling-restart-zero-loss` | **PASSED** | Rolling restart of `a2amq-api` completed with zero durable loss; pre-restart admitted task completed cleanly |
| 19 | `broker-outage-recovery` | **PASSED** | Controlled broker outage (NATS scaled to 0 replicas); held for 135s (canonical JetStream hold); verified task did not complete during outage; restored 3 ready broker pods; task completed cleanly |

**Overall Gate Status**: `PRODUCTION_DEPLOYMENT_PASS`
**Verdict File**: `/tmp/a2amq-prod-gate-evidence/production-deployment-verdict.json`
**Cleanup**: 0 remaining gate resources, clean automated teardown.

---

## 3. Provenance & Artifact Verification

- **Evidence Checksums (`/tmp/a2amq-prod-gate-evidence/sha256.json`)**:
  - `command-history.json`: `ca530a5d33cdb005b18eab6c64f5251e294890b1fc3bbd03e6764d65bc4cfab0`
  - `production-deployment-verdict.json`: `935cdbf6593c94454643ceb2c471fe30b22e4ab51118f011d1d7338ba21788c5`
  - `rendered-manifests.json`: `d6746e0eaaa102f2a9139da24cd59e9ec958e863bd8599c4b8cf7f4d1764c46e`
- **Application Image**:
  - Local Tag: `localhost:5001/a2amq-prod:6458195-ca`
  - Registry Manifest Digest: `sha256:a54c969183a0c314c77df93c7761db53de19dea8da9e9b20ddebccec425958af`
  - Build Base: Clean Git tree at `6458195b171020ece1911e68bc6d14998d216164` with trusted PostgreSQL TLS CA.
- **Git State**:
  - HEAD: `6458195b171020ece1911e68bc6d14998d216164`
  - Tree: `ed9af9e6c04a3b6bba1f0880ca40d7f9a7f4e758`
  - Status: Clean (no untracked or modified source files).

---

## 4. Replication and Verification Commands

To independently reproduce the certification on an authorized host with disposable infrastructure:

```bash
# 1. Full acceptance gate
python -m a2a_mq.acceptance \
  --mode full \
  --output /tmp/a2amq-full-evidence \
  --allow-disposable-containers \
  --timeout 2400

# 2. Production deployment gate
python scripts/production_deployment_gate.py \
  --output /tmp/a2amq-prod-gate-evidence \
  --namespace a2amq-prod-gate \
  --tenant prod-tenant \
  --broker jetstream \
  --image "a2amq-prod:6458195-ca@sha256:a54c969183a0c314c77df93c7761db53de19dea8da9e9b20ddebccec425958af" \
  --public-url "https://127.0.0.1:31443" \
  --issuer "https://a2amq-idp.internal" \
  --jwt-kid "prod-key-1" \
  --jwt-public-key-file /tmp/disposable-infra/gate/jwt_public.pem \
  --owner-token-file /tmp/disposable-infra/gate/owner.jwt \
  --foreign-owner-token-file /tmp/disposable-infra/gate/foreign_owner.jwt \
  --cross-tenant-token-file /tmp/disposable-infra/gate/cross_tenant.jwt \
  --mcp-token-file /tmp/disposable-infra/gate/mcp.jwt \
  --operator-token-file /tmp/disposable-infra/gate/operator.jwt \
  --ca-bundle /tmp/disposable-infra/gate/ca.crt \
  --disrupt-broker-target "statefulset/nats" \
  --allow-broker-disruption \
  --timeout 900
```
