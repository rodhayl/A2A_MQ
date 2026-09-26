# Local launcher and resource-evidence audit - 2026-09-22

## Provenance and scope

Tested code commit: `6912404e55f6ee8462406d6df0b20b7491c19957`.
The audit started from `da464645d9bb455666a7072d2be7981733e4a555` and preserved
concurrent changes through `b5d4cb17bc2109a980ad337b6e9266746d7e7625` before publication.
The following documentation-only commit does not alter the tested Python bytes.

This was a selected-file execution workspace, not a complete Git checkout. Source
files were read through the authorized GitHub connector, materialized locally and
checked against Git blob hashes. Published changed blobs match the executed files.
The local workspace's synthetic commits are not the repository's public ancestry.

Execution used a fresh isolated Python 3.13.5 venv without system-site-packages.
Installed distributions were copied offline into that venv because direct network
access was unavailable; this was NOT a fresh package-index resolution. `pip check`
passed. Relevant versions: pytest 9.0.2, pytest-asyncio 1.3.0, jsonschema 4.26.0,
cryptography 46.0.4 and PyJWT 2.13.0. No global packages or host services were changed.
Tests ran sequentially with umask 077, automatic pytest plugins disabled, private
temporary directories, and no Dapr/NATS/Docker/database daemons or model calls.

## Corrections

- Both local launchers resolve profile paths before changing child cwd. Native
  executables found through relative PATH entries also become absolute. Generated
  paths with spaces remain usable, while symlink profile roots remain rejected.
- The app explicitly uses the sidecar endpoint it launches on 127.0.0.1:3500 rather
  than inheriting another project's DAPR_HTTP_ENDPOINT. Provisioning is bounded
  to 30 seconds, with cleanup still attempted on failure.
- The unsafe-permission fixture explicitly chmods its directory: otherwise umask
  077 silently removed the permissions that this negative test meant to exercise.
- The core probe requires real memory/RSS/disk measurements, checks all projection
  pages, rejects nonfinite budgets, reserves fresh private state/report files and
  preserves an incomplete verdict when interrupted. Old reports cannot be reused
  or overwritten. Execution errors produce sanitized structured failure evidence.
- The stack probe checks executable roles, stable PID/start-time topology and
  readable CPU/RSS/state. Dead, replaced, extra or unmeasured processes cannot
  masquerade as a cheap healthy stack. Startup/idle peaks, flushed shutdown state,
  spawn-failure evidence and PID-reuse-safe cleanup from concurrent changes remain.

The launcher, original core-gate and original stack-gate defects were reproduced
before repair. Their failing stages overlap with later test stages and are not
additional passing tests. No architectural replacement or dependency was added.

## Final executed regression

| Test file | Passed | Failures/errors/skips |
| --- | ---: | --- |
| test_local_resource_probe.py | 3 | 0/0/0 |
| test_local_resource_probe_regressions.py | 14 | 0/0/0 |
| test_local_runtime.py | 18 | 0/0/0 |
| test_local_launcher_regressions.py | 10 | 0/0/0 |
| test_local_stack_probe.py | 11 | 0/0/0 |
| test_local_stack_probe_regressions.py | 18 | 0/0/0 |
| **Total** | **74** | **0/0/0** |

Each file ran separately, returned exit 0 and produced nonempty JUnit. Actual
case outcomes were checked, not only console summaries. Fifteen Python files also
passed AST parsing; this does not substitute for Ruff or mypy.

Commands followed this pattern, with a distinct stem for each file:

```bash
umask 077
PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 PYTHONDONTWRITEBYTECODE=1 PYTHONNOUSERSITE=1 \
"$PY" -m pytest -p pytest_asyncio.plugin -p no:cacheprovider \
  -o asyncio_default_fixture_loop_scope=function -q -W error "$TEST" \
  --basetemp="$STATE/tmp/$STEM" --junitxml="$EVIDENCE/$STEM.xml"
```

SQLite transactions, files, profile generation and JWT signing/refresh were real.
Native launch and process-observation tests use controlled fixtures. They prove
launcher/probe behavior, not interoperability with running native daemons.

## Separate real core observation

The unmodified default budgets were used in a separate Python process with the
final core-probe code and a fresh database, outside pytest:

| Observation | Result |
| --- | ---: |
| Peak Python RSS | 47.664 MiB |
| Core imports | 0.9066 s |
| 100 SQLite task operations | 0.3292 s |
| Final DB/WAL/SHM footprint | 0.348 MiB |
| Idle CPU during probe sleep | 0.004% of one core |
| Host swap growth | 0.0 MiB |
| Verdict | PASS_CORE_LIGHT |

This is the assistant's execution sandbox, not the owner's server. Idle CPU here
measures the core probe sleeping, NOT the active bridge/relays. Swap is host-wide
and cannot be causally attributed to this process. This run does not estimate the
full Python application's imports, Dapr, NATS, SDK or Workflow overhead. A passing
core budget is not proof of the complete local product's economics.

## Post-audit native bootstrap correction

The 74-test selected regression remains valid for the Python launcher/probe bytes
described above, but it did not execute native Dapr/NATS. A later source audit found
that Dapr v1.18.3 registers `pubsub.jetstream` only under the `allcomponents`
build tag. The bootstrap present at the historical tested SHA selected the smaller
`stablecomponents` daprd asset, so that historical bootstrap was not capable of
proving the intended JetStream-backed three-process stack.

This was corrected after the audit: the private bootstrap now pins Dapr v1.18.3's
official default/all-components assets and the native preflight requires bootstrap
provenance with `daprFlavor=allcomponents` in addition to exact Dapr/NATS versions.
This correction does not turn the native gate into PASS; it removes a prerequisite
bug so the next owner's-machine run can measure the intended stack.

## Unexecuted gates and next run

Real native local-lite, official A2A/MCP interoperability, Dapr Workflow, PostgreSQL,
Vault, RabbitMQ, full repository regression, fresh dependency resolution, packaging,
Ruff/mypy and hosted CI were not established by this audit. Direct container GitHub
DNS access was unavailable; authorized connector access does not supply a native
runtime. No measured Dapr/NATS footprint is claimed.

Keep the existing lightweight architecture until real stack measurements justify
changing it. Prior unmeasured estimates of Dapr/NATS RAM are not a basis for
replacing the transport adapter. Follow `PROMPT_AGENT_LOCAL_FOLLOWUP.md` for a
bounded rerun on the owner's machine without repeating unrelated infrastructure work.
