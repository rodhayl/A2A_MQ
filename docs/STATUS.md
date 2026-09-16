# Source and acceptance status - 2026-09-16

## Present source

Branch: `implementation/production-hardening-20260916`.
Base: `66c17153d7ec75b9ad85808ef141b9dba43a9d91` (phase 2).
Version: **0.1.0**. `main` has not been merged or force-updated by this hardening.
No release or certification is issued.

The source contains envelope validation, task-state helpers, Dapr HTTP publishing
and subscription, the volatile echo demo and its disposable deployment generator.
Hardening adds strict JUnit evidence, permanent JSON error normalization, stable
reference event bytes, cooperative transport deadlines, pure-ASGI token checks,
CloudEvent consistency, frozen-SHA review tooling and stricter CI.

The [hardening record](validation/hardening-20260916.md) distinguishes tested scopes.
The [review procedure](../TEST_MVP.md) deliberately cannot return full-MVP acceptance.
Architecture, whitepaper, requirements, workflow/security/operations plans and old
phase records are design or historical material, not proof of current implementation.

## Recovery blocker

The staged handoff at `e80096da84190a8af9aa044a4bd97b0123fca4ad` expected a bundle with
SHA-256 `47a2c5418ec35d0d1c5fbcbb0fa4cd502b8479760b935a3b94ed01efe93965e9`,
10 checkpoints and final tree `79521dec0000e246baaab5bf4129841aa4799914`.
No verified reconstruction of that tree was obtained.

The committed `.bundle/part-01` blob `f347f90d3b4db2c80ae61d718b69c41af3dc9be8`
has a literal `...` suffix. The alternate `.sync/base-719f.b64` blob
`d22c45df80281ce9fc8fe6e29bfc16bedbba18e7` also has a literal ellipsis suffix.
Encoded-tail inspection distinguished these stored bytes from tool display truncation.
These payloads are not accepted as complete base64 archives. The other legacy parts
have not established a complete checksum-verified recovery either.

The normal source branch preserves the accessible baseline and tested changes. It
is not a substitute for the lost later snapshot. Obtain intact original bytes, verify
the expected checksum and `git bundle verify`, and compare the recovered final tree
before replacing any source. Keep recovery payloads outside the product branch.

## Staged patch disposition

| Block | Disposition |
| --- | --- |
| 02 | Standalone JUnit gate recovered and strengthened; CI adapted to the present source. Missing PostgreSQL/full-stack/runtime gates are not claimed. |
| 03-04 | Full canonical agent E2E and its flush fix deferred: durable admission, bridge and workflow implementation absent. |
| 05 | Full chaos matrix deferred for the same dependency gap. Transport timeout/cancellation/replay regressions added separately, not equivalent to full-stack chaos. |
| 06-07 | Full security/config/coverage integration deferred: target modules and complete baseline absent. JSON and token-boundary changes are narrower. |
| 08 | Isolated full-MVP wheel/migration smoke deferred: packaged persistence/resources not recovered. |
| 09 | The proposed 0.3.1 completion claims were not applied. README and status describe the actual 0.1.0 source. |
| 10 | A new limited frozen-SHA review tool and procedure implement evidence guards. The missing full validator is explicitly blocked, not reconstructed or certified. |

Deferred means the patch was not safely applicable as a complete feature on the
available baseline; it is not a conclusion that the original change was invalid.

## Unverified and absent gates

No real PostgreSQL, NATS JetStream, RabbitMQ, Dapr Workflow, official A2A bridge
client, MCP server or Vault service was exercised in this hardening closure.
The full persistence/workflow/bridge/security/encryption/MCP implementation remains
unrecovered. Docker and those services are absent locally; the pinned upstream SDK,
Ruff and mypy are also unavailable locally. Container network DNS failed, preventing
a fresh clone or clean network dependency resolution. The authorized GitHub connector
was used to read and publish the source.

Hosted run `35153620064` at `c4449c94bf1bad5b9ea1ca2f6f4538999156d7bc` failed
with no step summaries: Python 3.12 job failed, Python 3.13 was cancelled. That is
not a code-test verdict, and the underlying runner failure was not established.
Later CI configuration changes require their own successful hosted execution.

Independent full acceptance, wheel/sdist installation, lint/types, dependency/SBOM
and container audits, real infrastructure parity, production deployment and performance
SLAs remain unverified. Local pass, hosted CI pass, full-MVP acceptance and production
readiness are separate statements; none implies the next one.
