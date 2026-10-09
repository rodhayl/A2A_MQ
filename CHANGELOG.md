# Changelog

## Unreleased — portfolio preparation

- Add bounded real-transport failure demo and public onboarding/compatibility guidance.
- Align machine-readable operations with official A2A 1.0 method names.
- Pin local Dapr advertisement to loopback, consistent with the single-host profile.
- Record current test evidence separately from historical release acceptance.


This changelog records user-visible/release-relevant changes, not every commit.

## Unreleased

### Added

- non-mutating `a2amq-local doctor` and `a2amq-local-workflow doctor` preflight;
- actionable CLI messages for expected local setup errors;
- small `scripts/smoke_local.py` Local-lite end-to-end smoke;
- centralized configuration and troubleshooting references;
- A2A JSON-RPC request examples;
- distributed deployment guide.

## v0.1.0-rc2 — 2026-09-27

### Added

- Queue Lab browser/manual test console;
- Local-lite pause/queue/cancel/resume test control at the real delivery boundary;
- official A2A streaming and task subscription flows in Queue Lab;
- local Demo DLQ -> Inspect -> Replay workflow;
- focused optional local Workflow smoke.

### Fixed

- narrow protobuf deprecation compatibility around the pinned A2A SDK validation path;
- Linux Queue Lab pause-regression test timing without changing production runtime
  behavior or assertions.

### Validated

- Ubuntu 24.04.5 LTS, x86_64, Python 3.12.3;
- exact repository-pinned Dapr 1.18.3 and NATS Server 2.15.0 provenance;
- real Chromium Queue Lab smoke;
- Workflow Placement/Scheduler 1.18.3 provenance and focused workflow scenarios;
- clean local shutdown.

The annotated `v0.1.0-rc2` tag remains bound to
`4b1fc34065e37b68258f1944fbcb1e9a15f5fe6d`.

## v0.1.0-rc1 — 2026-09-26

First tagged Local-lite release-candidate checkpoint with pinned native bootstrap,
official A2A task roundtrip, readiness and clean-shutdown smoke.

See [docs/VALIDATION.md](docs/VALIDATION.md) for exact SHA-bound evidence and the
boundary between release-candidate smokes and earlier broader acceptance.
