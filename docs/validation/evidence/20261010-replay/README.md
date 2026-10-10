# Private replay checkpoint, 2026-10-10

This directory preserves the original bounded replay deliverable byte-for-byte.
The accompanying changes to `scripts/portfolio_demo.py`,
`tests/test_portfolio_demo.py` and `docs/PORTFOLIO_DEMO.md` are exactly
`replay-evidence.patch` applied to base
`cbe1c4f09f1dd6dc56cc7b312be5f2ae187b82ee`.
No runtime `src/` file or package metadata changes in this checkpoint.

The original run's source-content SHA-256 is
`411faa1750bee94016b275f343a72fadc1187b9c1c763c9a01f6ae5d73bec2a0`.
It is reproduced from this candidate using the harness's `source_digest` algorithm.
This hash covers runtime Python/JSON, `pyproject.toml`, and the demo script;
it is not a whole-tree or Git commit identity. `manifest.json` binds the three
changed files, original patch, and retained evidence. The delivered ZIP hash was
`aa066685171463366ae74680304177bf43007c838621c98b16a530a4a28ae4cc`.

Read [RESULT.md](RESULT.md) for results and limits, [real-replay.json](real-replay.json)
for the original live observation, and [focused-tests.xml](focused-tests.xml)
for the 14 focused harness contracts. The original report's statement that no
publication occurred describes its creation time, before this private checkpoint.
No fresh runtime or full-suite execution is claimed by this preservation step.

The reporting-only `reporting-label-followup.patch` remains unapplied. The existing
field `replayRequestedAfterDedupSeconds` measures through receipt/ACK, not the
instant before the replay POST. Ordering comes from `originalBrokerAck.observedAt`,
`observationCheckpoint`, and `replayReceipt.timestamp`. Do not silently rename the
field or rewrite the checksum-bound original report.

The observed replay used the same immutable identity, a new stream sequence (9),
zero pending ACKs, unchanged terminal artifacts and one terminal record after
waiting 121.0125 seconds beyond the original ACK observation (120-second effective
deduplication window). This demonstrates bounded receipt and safe handling of an
already-completed synthetic command. It does not prove new execution, a specific
terminal-guard branch, exhausted retries or generic exactly-once external effects.

Only source/test/guide and these small reviewed evidence files belong in this
checkpoint. No profile, credential, database, native executable/archive, raw runtime
log, or local environment is included. The synthetic test marker and code that
reads runtime credentials are source code, not retained credentials. Original JUnit
contains a disposable execution hostname as engineering metadata.

Keep the existing repository private. This checkpoint does not choose a license,
authorize public exposure, merge main, provide full acceptance or change
`productionCertified=false` / `upstreamBindingCertified=false`. Previous validation
records remain historical and bound to their own identities.
