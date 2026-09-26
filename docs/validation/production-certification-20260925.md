# Production Deployment Acceptance & Full Acceptance Report — 2026-09-25

## 0. Executive Summary

This validation record documents the end-to-end production deployment acceptance and formal full acceptance gate execution for A2A-MQ on branch `implementation/production-hardening-20260916`:

- **Formal Acceptance Source SHA**: `45c459c632977143b8681c055847775e88d7ad97` (clean working tree).
- **Formal Acceptance Git Tree SHA**: `23fdbcdf30adedf984c631b8a060442cc97b426a`
- **Kubernetes-Validated Image Source SHA**: `6458195b171020ece1911e68bc6d14998d216164` (clean tree `ed9af9e6c04a3b6bba1f0880ca40d7f9a7f4e758`).
- **Application Image Reference**: `a2amq-prod:6458195-ca@sha256:a54c969183a0c314c77df93c7761db53de19dea8da9e9b20ddebccec425958af`
- **Formal Acceptance Gate (`a2a_mq.acceptance`) Verdict**: `FULL_PASS` (11/11 sub-gates passed).
- **Production Deployment Gate (`scripts/production_deployment_gate.py`) Verdict**: `PRODUCTION_DEPLOYMENT_PASS` (19/19 checks passed on live 3-node Kubernetes cluster with Dapr 1.18.3 and NATS JetStream).
- **Certification Booleans**: `productionCertified: false` and `upstreamBindingCertified: false` strictly preserved; this report reflects successful acceptance gate executions rather than signed certification booleans.
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
| `local` | 1150 tests | **PASSED** | 0 failures, 0 errors, 0 skipped, and no unexpected/unfiltered warning failures |
| `sdk` | 26 tests | **PASSED** | Upstream SDK & canonical MCP contracts |
| `postgres` | 4 tests | **PASSED** | Verified TLS connection (`sslmode=verify-full`), schema migration, ACID transactions |
| `vault` | 1 test | **PASSED** | TLS Vault Transit authenticated payload cipher/decipher |
| `repository-check` | Static check | **PASSED** | Clean repo tree, license/metadata verification |
| `dependency-check` | Requirements | **PASSED** | Bounded version verification |
| `lint` | Ruff | **PASSED** | 0 lint errors |
| `types` | Mypy | **PASSED** | Mypy validation across src/a2a_mq: PASS |
| `package` | Wheel & Smoke | **PASSED** | Isolated build, install, import smoke |
| `jetstream` | 12 live checks | **PASSED** | JetStream live probe with unique CloudEvent IDs across hold windows |
| `rabbitmq` | 12 live checks | **PASSED** | RabbitMQ live probe with quorum queues and dead-lettering |

**Overall Acceptance Status**: `FULL_PASS`  
**Verdict Artifact**: `docs/validation/evidence/20260925/full/verdict.json`  
**Checksums Artifact**: `docs/validation/evidence/20260925/full/sha256.json`  

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
**Verdict Artifact**: `docs/validation/evidence/20260925/prod-gate/production-deployment-verdict.json`  
**Cleanup**: 0 remaining gate resources, clean automated teardown.

---

## 3. RabbitMQ Kubernetes Production Parity Boundary

- **RabbitMQ Real Live / Full-Stack Acceptance**: **PASS** (all 12 live probe checks passed in disposable container stack during formal full acceptance).
- **JetStream Kubernetes Production Deployment**: **PASS** (all 19 required production checks passed on live multi-node Kubernetes cluster).
- **RabbitMQ Kubernetes Production Deployment**: **Not separately executed** (the disposable Kubernetes cluster topology was provisioned with NATS JetStream; RabbitMQ live acceptance was independently verified in the container topology without exposing implementation defects).

---

## 4. Provenance & Artifact Verification

- **Preserved Sanitized Evidence Selection (`docs/validation/evidence/20260925/`)**:
  - `full/verdict.json`: Formal full acceptance verdict recording `FULL_PASS` across all 11 sub-gates at source SHA `45c459c632977143b8681c055847775e88d7ad97` (tree `23fdbcdf30adedf984c631b8a060442cc97b426a`).
  - `full/sha256.json`: Original SHA256 checksum manifest for the full acceptance evidence tree; not every referenced raw file is preserved in Git.
  - `prod-gate/production-deployment-verdict.json`: 19-check Kubernetes production deployment verdict (`935cdbf6593c94454643ceb2c471fe30b22e4ab51118f011d1d7338ba21788c5`).
  - `prod-gate/rendered-manifests.json`: Sanitized, secret-free Kubernetes manifests applied during live gate (`d6746e0eaaa102f2a9139da24cd59e9ec958e863bd8599c4b8cf7f4d1764c46e`).
  - `prod-gate/command-history.json`: Complete command execution log (`ca530a5d33cdb005b18eab6c64f5251e294890b1fc3bbd03e6764d65bc4cfab0`).
  - `prod-gate/sha256.json`: Checksum manifest confirming evidence integrity.
- **Application Image**:
  - Local Tag: `localhost:5001/a2amq-prod:6458195-ca`
  - Registry Manifest Digest: `sha256:a54c969183a0c314c77df93c7761db53de19dea8da9e9b20ddebccec425958af`
  - Build Base: Clean Git tree at `6458195b171020ece1911e68bc6d14998d216164` with trusted PostgreSQL TLS CA.
- **Git Provenance**:
  - Formal Acceptance Source HEAD: `45c459c632977143b8681c055847775e88d7ad97`
  - Formal Acceptance Source Tree: `23fdbcdf30adedf984c631b8a060442cc97b426a`
  - Status: Clean (no untracked or modified source files).
