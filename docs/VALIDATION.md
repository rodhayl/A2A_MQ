# Validation record — current v0.1.0 pre-release

This file indexes current evidence. Historical phase/audit/resource records are under
[archive/validation](archive/validation/) and remain bound to their recorded SHAs.

## Evidence provenance

Formal FULL_PASS evidence is bound to:

- source SHA: `45c459c632977143b8681c055847775e88d7ad97`
- tree: `23fdbcdf30adedf984c631b8a060442cc97b426a`
- package: `0.1.0`

The tagged `v0.1.0-rc1` checkpoint is separately backed by the focused local
user-smoke recorded in STATUS.md.

The working branch now includes **post-RC1 Queue Lab source changes**. Therefore the
current branch HEAD must not be described as FULL_PASS or RC1-smoke-tested merely by
inheritance. Queue Lab requires its own focused tests/browser smoke before the next
release-candidate tag.

## Formal acceptance — FULL_PASS

| Gate | Result |
| --- | --- |
| Local broker-independent | 1,150 passed; 0 failures/errors/skips |
| Official SDK/MCP | 26 passed |
| Verified-TLS PostgreSQL 16 | 4 passed |
| TLS Vault Transit | 1 passed |
| Repository check | PASS |
| Dependency check | PASS |
| Ruff | PASS |
| Mypy (`src/a2a_mq`) | PASS |
| Isolated wheel/package smoke | PASS |
| JetStream live probe | 12/12 |
| RabbitMQ live probe | 12/12 |

Current durable evidence:

- [production deployment/full acceptance report](validation/production-certification-20260925.md)
- [release-cleanliness report](validation/release-cleanliness-report-20260925.md)
- [sanitized evidence selection](validation/evidence/20260925/)

The evidence directory intentionally preserves reviewed verdict/manifests and the
original checksum manifest, not every raw fixture file.

## Kubernetes production-deployment acceptance

A digest-pinned image built from source `6458195b171020ece1911e68bc6d14998d216164`
achieved **PRODUCTION_DEPLOYMENT_PASS (19/19)** with JetStream on live three-node
Kubernetes v1.31.0 + Dapr 1.18.3.

No deployed runtime module changed between that image source and `45c459c`. This
verdict remains evidence for the exact image digest, not for an unbuilt future image.

RabbitMQ Kubernetes parity is not separately executed.

## Historical local economics

Historical native local-lite, Workflow, resilience and soak measurements remain
useful but SHA-bound. The latest local certification is archived at
[archive/validation/local-certification-20260924.md](archive/validation/local-certification-20260924.md).

Do not relabel historical RSS/CPU/soak figures as measurements of a later source
without rerunning the relevant gates.

## Hosted CI and certification semantics

Hosted Actions currently fail before runner assignment: `runner_id=0`, empty
runner name and zero executed steps. This is unexecuted hosted CI, not a source-test
failure and not a pass.

`FULL_PASS` and `PRODUCTION_DEPLOYMENT_PASS` are repository-defined acceptance
verdicts. They do not change:

- `productionCertified = false`
- `upstreamBindingCertified = false`
