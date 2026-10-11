# Current status

## Native encrypted-topic component demo, 2026-10-11

The [new native demo](AGENT_BUS_DEMO.md) passed on real Dapr/NATS with a separate
producer/worker, two independent observers, encrypted command/event payloads,
offline-observer recovery and four rejected-message cases. Its final reviewed
[report and manifest](validation/evidence/20261011-agent-bus/manifest.json) bind
source-content SHA-256
`7b249f711a72995eb5477a800c397dac76c72cdad1557bd3bff1c7daa535cf73`;
367 focused checks passed separately. This is development-only LocalKeyRing
component integration, not a new full-suite, A2A API, ServiceApplication, Vault or
production acceptance result. Historical counts below are not aggregated with it.

This repair also makes the separate `production_env` entrypoints reject unsupported
`A2A_CRYPTO_CONFIG` explicitly instead of silently ignoring it; the supported crypto
composition remains `application:from_env`. See [payload protection](PAYLOAD_PROTECTION.md).
The original Local-lite runtime and recovery-demo script remain unchanged; their
completed, dated scope below is preserved, not relabeled as encrypted acceptance.

## Public CV/portfolio experiment, 2026-10-11

The existing repository was made public with owner approval on 2026-10-11 at
`cc761a14e792c273fc2599909100b3aea51c9e9c`; anonymous README access was verified.
Publication did not change the runtime, retained tags or evidence. No license grant
was added. See [publication preparation and exposure limits](PUBLICATION.md).

The agreed Local-lite experiment is complete. Its reviewed checkpoint is
`f24a2e8411a902e2669bd7c16ae193ce45e3effb`, with unchanged runtime `src/` and package
metadata from `cbe1c4f09f1dd6dc56cc7b312be5f2ae187b82ee`. Package version remains 0.1.0.

The [portfolio validation](validation/portfolio-20261009.md) retains the 1,205-test
core, four real PostgreSQL tests, native recovery demo and isolated-wheel evidence.
The [bounded replay record](validation/evidence/20261010-replay/README.md) adds 14
harness contracts and real same-identity receipt/ACK after the unchanged 120-second
broker deduplication window, with one terminal record and unchanged artifacts.
These are separate dated scopes, not a new aggregate run or certification.

No further local-agent campaign is required for this bounded closure. Optional
Vault/distributed-Docker gates and Windows native validation remain outside this
bounded CV/portfolio closure. Publication was reviewed separately; it does not
extend those runtime guarantees. No new infrastructure, credentials or feature work
is required.
The guarantee boundary below remains unchanged.

## Retained portfolio preparation, 2026-10-09

Package version remains **0.1.0**. Preparation was based on
`2f57ad386c8dfba94c1910cbcc639c809af28f34`. The candidate is privately checkpointed at
`dfebe8d0f6ef7cbb96b7489440e67a5c5c64ff40`, tree
`b01618a612366763278d3cad82334aee2b35adb5`; later documentation/packaging refreshes
retain separate identities.

The candidate improves first-run documentation, aligns the binding inventory with
A2A 1.0 method names, and adds a small real-transport failure demonstration. Its
actual checks and open limitations are in
[portfolio validation](validation/portfolio-20261009.md). The dated
[exposure audit](publication-exposure-20261009.md) completed ordinary reachable-history
and enumerated remote-metadata inspection for the historical baseline, plus a
separate exact candidate-tree screen. The [publication preparation](PUBLICATION.md)
records the later tracked-tree review, recoverable branch cleanup and separate-surface
checks, together with their historical exposure limits. No license grant is supplied.
The later visibility change is recorded above, separately from the completed experiment.

## Supported product shape

- Local-lite: one Python app/reference worker, native daprd + nats-server, SQLite,
  immediate Echo/Uppercase, official A2A routes and Queue Lab.
- Optional local Workflow: adds native Placement/Scheduler for waits, timers,
  continuation and approval. It is not part of the minimal demo.
- Distributed: role-separated API/worker/operations, PostgreSQL, JetStream or RabbitMQ,
  optional Vault/MCP/telemetry and Kubernetes deployment tools.

See [compatibility](COMPATIBILITY.md) for supported and unsupported protocol surfaces.
The operations modules are distinct surfaces; their similar names do not make them
dead code. Local/Workflow use `/operations`; the role-separated production entrypoint
also has its own `/ops` contract.

## Historical release identity

Latest retained release-candidate identity recorded in this source is
`v0.1.0-rc2`, tagged at `4b1fc34065e37b68258f1944fbcb1e9a15f5fe6d`.
Post-RC2 onboarding was validated at `401471b05da55ba8311b9bc6e5019a24a4250b9b`
and merged through `84097baa57da4919c1672a0392397e05c0404882`.
These records are retained in [Validation](VALIDATION.md); they are not new runs
against this portfolio candidate.

Earlier source `45c459c632977143b8681c055847775e88d7ad97` and the Kubernetes image
built from `6458195b171020ece1911e68bc6d14998d216164` have separate SHA/image-bound
acceptance records. Their numerical results are not repeated as current claims.
Hosted CI observed on the baseline failed before runner assignment. This preparation
does not fix or claim a successful hosted run.

## Unchanged guarantee boundary

`productionCertified=false`, `upstreamBindingCertified=false`. No generic exactly-once
external effects, HA SLA, RPO/RTO, unmeasured performance, full A2A certification or
Windows native-runtime validation. Local-lite API and worker share one process.
