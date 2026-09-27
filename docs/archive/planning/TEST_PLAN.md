# Test plan and evidence policy

## Layers

`tests/` contains executable tests. Offline unit/component/SQLite tests exercise
real local code and SQL transactions. HTTP mocks prove request/response handling,
not JetStream's broker semantics. `live`, `postgres`, and `upstream` markers require
explicit prerequisites and must appear as skipped or not executed when absent.
A test report is evidence only for the environment in which it actually ran.

## Preserved scenario IDs

| ID | Scenario | Phase / layer |
|---|---|---|
| T-001 | Publish before workers exist | 2 / live broker |
| T-002 | Competing worker pool | 2/3 / live plus SQL race |
| T-003 | Crash before ACK | 3 / subprocess plus live redelivery |
| T-004 | Commit effect before ACK | 3 / SQLite crash fixture |
| T-005 | Duplicate event | 3 / projection |
| T-006 | Stale/out-of-order version | 3 / snapshot permutations |
| T-007 | Input pause/restart/continue | 4 |
| T-008 | Auth pause/restart/continue | 4/6 |
| T-009 | Safe cancellation | 4 |
| T-010 | Cancel/completion race | 4; terminal invariant in 1/3 |
| T-011 | Poison/DLQ | 2 / HTTP contract and live gate |
| T-012 | Malformed envelope | 0/1/2 |
| T-013 | Expired command | 1/3 |
| T-014 | Unauthorized publisher | 6 |
| T-015 | Unauthorized subscriber | 6 |
| T-016 | Broker sees no plaintext | 7 |
| T-017 | Ciphertext/AAD tampering | 7 |
| T-018 | Rotation read window | 7 |
| T-019 | Retired key | 7 |
| T-020 | Publish dependency unavailable | 2 |
| T-021 | Store unavailable before ACK | 3 |
| T-022 | Independent observers | 2 / live |
| T-023 | Payload/resource limits | 1/2 |
| T-024 | Standard A2A client | 5; SDK codec check earlier |
| T-025 | Bridge restart | 5 |

## Additional regression cases

Every source/target state pair; route collision attempts; duplicate JSON keys,
non-finite values, invalid UTF-8, nesting, extra fields, null/both destinations;
A2A payload identifier mismatch and old enum shape; unsupported encryption; changed
payload under an existing ID; same ID in different tenants/consumer groups; expired
lease takeover; stale token/fence rejection; rollback after SQL effect insertion;
process death after durable completion; event publish before outbox mark; equal
snapshot version conflicts; terminal rewrite; different task authority/context;
query filter/cursor mismatch, injection input and bounded pagination.

## Running

```sh
python tools/check.py
python -m pytest -m 'not live and not postgres'
ruff check .
mypy
```

Live and PostgreSQL instructions are in `deploy/README.md`. Preserve phase-specific
reports under `docs/validation/` and summarize actual pass/skip counts in
`VALIDATION.md`. No benchmark target is an achieved result. CI configuration is not
proof that CI ran. Future failure tests must include real process kills, not only
raised exceptions, at transaction and publication boundaries.
