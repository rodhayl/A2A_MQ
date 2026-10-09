# Current status

## Portfolio candidate, 2026-10-09

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
separate exact candidate-tree screen. Corrupt embedded history and separate-surface
checks remain open. Licensing/rights decisions and explicit publication approval are
still required; see [publication checklist](PUBLICATION.md).

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
