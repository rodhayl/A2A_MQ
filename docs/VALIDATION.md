# Validation evidence index

Current CV/portfolio evidence:

- [2026-10-09 validation](validation/portfolio-20261009.md): runtime, package and core evidence.
- [2026-10-10 replay](validation/evidence/20261010-replay/README.md): the bounded
  replay-after-dedup checkpoint `f24a2e8411a902e2669bd7c16ae193ce45e3effb`.

See [STATUS](STATUS.md) for the completed experiment scope and retained limitations.
These records retain their exact tested identities; no new full-suite run is claimed.

The records below describe earlier release candidates, not current-candidate test runs.
This file is the index for retained release-candidate evidence. Historical phase,
audit, resource and acceptance records remain SHA-bound under `archive/validation/`
and `validation/`.

## Usability and onboarding validation (post-RC2)

The usability onboarding implementation was validated on real Linux (Ubuntu 24.04.5 LTS /
Python 3.12.3) at
`401471b05da55ba8311b9bc6e5019a24a4250b9b` and merged into `main` via
`84097baa57da4919c1672a0392397e05c0404882`. The validation proved the
non-mutating `doctor` preflight, actionable CLI error handling, Local-lite runtime,
smoke script, official API examples, and local Workflow runtime.

Detailed Linux evidence:
[validation/usability-onboarding-20260927.md](validation/usability-onboarding-20260927.md).

Independent repository/post-merge audit:
[validation/usability-onboarding-post-merge-audit-20260927.md](validation/usability-onboarding-post-merge-audit-20260927.md).

## RC2 focused Linux validation

Release candidate:

- tag: `v0.1.0-rc2`;
- tested/tagged commit: `4b1fc34065e37b68258f1944fbcb1e9a15f5fe6d`;
- annotated tag object: `26faf43fcc41221d42e1e6f4829ea4e5c99e56b4`;
- package: `0.1.0`;
- validation date: 2026-09-27;
- environment: Ubuntu 24.04.5 LTS, x86_64, Python 3.12.3, Chromium 153.0.8010.47.

Focused automated gates:

| Gate | Result |
| --- | --- |
| `python -m pip check` | PASS — no broken requirements |
| `python3 tools/check.py` | PASS |
| `tests/test_queue_lab.py` | 11 passed |
| bridge selector: subscribe/stream/cancel | 13 passed, 7 deselected |
| local-lite selector: queue_lab/pause | 1 passed, 9 deselected |
| unexpected skips | 0 |

Native provenance:

| Binary | Version | Archive SHA-256 |
| --- | --- | --- |
| `daprd` | 1.18.3 allcomponents | `f5d1683b672d83bd88b8e79624d695779b72f856f7eb365ae1ea06354f2cb96c` |
| `nats-server` | 2.15.0 | `5d2c51caca950333aba84911df7d377f826f3a59ec36061c6539105084f65c92` |
| `placement` | 1.18.3 | `1a697e769d9bb4b7eab883ee7d8776a243e2f2580bae6a001e8ff7fca12604ad` |
| `scheduler` | 1.18.3 | `aa55eae2a8c38ef70e6d3f418a9d88e6b106d7fa56f8f4950c10e79ba7577e6f` |

`scripts/check_local_native_versions.py` returned `PASS_NATIVE_VERSION` for the
Local-lite tools and again for the Workflow tools. The checker binds the executable
to the exact retained pinned release archive; it is not only a version-string check.

Real browser/runtime smoke:

- Local-lite health/readiness/authenticated metrics passed;
- Echo and Uppercase completed through real Dapr Pub/Sub + JetStream;
- pause/queue/cancel/resume preserved `SUBMITTED` work and drained correctly;
- official A2A `SendStreamingMessage` and `SubscribeToTask` SSE passed;
- terminal subscription was refused cleanly;
- Demo DLQ -> archive -> Inspect -> Replay passed without corrupting or duplicating
  the committed task result;
- raw JSON-RPC List/Get, deliberate invalid method and raw streaming passed;
- Clear, invalid JWT 401 and valid-token reconnect passed;
- zero JavaScript console errors;
- local Workflow input continuation, approve/reject and a 2-second delay passed;
- both profiles shut down with zero leftover runtime processes/ports.

Full RC2 record:
[validation/queue-lab-rc2-20260927.md](validation/queue-lab-rc2-20260927.md).

## Test-only adjustments between the initial handoff and RC2

RC2 validation started from `5406a9f95892a9832597e4924afe50a09add3831` and
created two test-only commits before the tag. The complete diff from that SHA to
`4b1fc340...` is two timeout-value changes in `tests/test_queue_lab.py`.

No production source file changed, no assertion was removed, and no skip/xfail or
global-warning suppression was introduced.

## Earlier formal acceptance — FULL_PASS

Formal FULL_PASS evidence remains bound to:

- source SHA: `45c459c632977143b8681c055847775e88d7ad97`;
- tree: `23fdbcdf30adedf984c631b8a060442cc97b426a`;
- package: `0.1.0`.

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

Related retained records:

- [validation/production-certification-20260925.md](validation/production-certification-20260925.md)
- [validation/release-cleanliness-report-20260925.md](validation/release-cleanliness-report-20260925.md)
- [validation/evidence/20260925/](validation/evidence/20260925/)

## Earlier Kubernetes production-deployment acceptance

A digest-pinned image built from source `6458195b171020ece1911e68bc6d14998d216164`
achieved **PRODUCTION_DEPLOYMENT_PASS (19/19)** with JetStream on live three-node
Kubernetes v1.31.0 + Dapr 1.18.3.

The deployment verdict is tied to that image digest. RabbitMQ Kubernetes parity has
not been separately executed.

## Historical local economics

Resource, resilience and soak results remain useful but SHA/environment-bound. The
latest archived local certification is
[archive/validation/local-certification-20260924.md](archive/validation/local-certification-20260924.md).

Do not copy historical RSS/CPU/soak numbers onto a later source without rerunning the
corresponding gate.

## Hosted CI

Hosted GitHub Actions remain unavailable as executable evidence. RC2 runs and the
post-RC2 usability branch/merge/report runs all failed before runner assignment. For
the usability work specifically, runs `36305318006`, `36306201233` and
`36308979807` report `runner_id=0`, empty runner names and `steps=[]`.

They therefore provide neither a source-test failure nor a source-test pass. See the
post-merge audit above for the exact boundary.

## Certification semantics

`FULL_PASS`, `PASS_NATIVE_VERSION` and `PRODUCTION_DEPLOYMENT_PASS` are
repository-defined acceptance verdicts, not third-party certification. They do not
change:

- `productionCertified = false`
- `upstreamBindingCertified = false`
