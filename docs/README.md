# Documentation map — v0.1.0-rc2

This directory separates maintained product documentation from historical project
records. If you are new to A2A-MQ, start with `GETTING_STARTED.md` and use `STATUS.md`
and `VALIDATION.md` for the exact release/evidence boundary.

## Start here

- [GETTING_STARTED.md](GETTING_STARTED.md) — fresh-checkout quickstart for Local-lite.
- [QUEUE_LAB.md](QUEUE_LAB.md) — graphical/manual queue and protocol test console.
- [STATUS.md](STATUS.md) — current `v0.1.0-rc2` state and known boundaries.
- [VALIDATION.md](VALIDATION.md) — SHA-bound current and historical evidence.
- [ARCHITECTURE.md](ARCHITECTURE.md) — deployment tiers and consistency boundaries.
- [LOCAL_RUNTIME.md](LOCAL_RUNTIME.md) — Local-lite and optional local Workflow.
- [IMPLEMENTED_STACK.md](IMPLEMENTED_STACK.md) — integrated/distributed capabilities.
- [IMPLEMENTATION_COVERAGE.md](IMPLEMENTATION_COVERAGE.md) — requirement-to-evidence summary.
- [CONFIGURATION.md](CONFIGURATION.md) — supported local and distributed configuration.
- [TROUBLESHOOTING.md](TROUBLESHOOTING.md) — recovery for startup/configuration failures.
- [API_EXAMPLES.md](API_EXAMPLES.md) — copyable official A2A JSON-RPC requests.
- [DEPLOYMENT.md](DEPLOYMENT.md) — distributed deployment responsibility and rendering guide.
- [../CHANGELOG.md](../CHANGELOG.md) — user-visible release/unreleased changes.

## Product contracts

- [PROTOCOL.md](PROTOCOL.md) — envelope, routing and delivery semantics.
- [SECURITY.md](SECURITY.md) — trust boundaries and controls.
- [WORKFLOWS.md](WORKFLOWS.md) — durable task/workflow failure boundaries.
- [OPERATIONS.md](OPERATIONS.md) — health, recovery, DLQ and backup boundaries.
- [REQUIREMENTS.md](REQUIREMENTS.md) — stable requirement IDs and traceability.
- [WHITEPAPER.md](WHITEPAPER.md) — research framing and limitations.

## Current validation records

- [validation/queue-lab-rc2-20260927.md](validation/queue-lab-rc2-20260927.md) —
  real Linux RC2 Local-lite/Queue Lab/Workflow validation.
- [validation/production-certification-20260925.md](validation/production-certification-20260925.md) —
  earlier full acceptance and production-deployment evidence.
- [validation/release-cleanliness-report-20260925.md](validation/release-cleanliness-report-20260925.md) —
  release-cleanliness evidence for its recorded SHA.
- [validation/evidence/20260925/](validation/evidence/20260925/) — reviewed sanitized
  evidence selection.

Validation records are immutable historical evidence: each result applies only to the
source/image/environment identified by that record.

## Architecture decisions

[adr/](adr/) contains architecture decision records. ADRs preserve the decision and
context at the time they were accepted; they are not rewritten to mimic the current
implementation. Use `ARCHITECTURE.md`, `STATUS.md` and `VALIDATION.md` for current state.

## Historical material

[archive/](archive/) contains superseded plans, old agent handoffs, reference material
and older validation records. Archive content remains for traceability and is not
current release guidance.
