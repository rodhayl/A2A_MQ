# Bounded Local-lite replay evidence, 2026-10-10

PASS on the retained tested harness; no runtime product source changed from base cbe1c4f09f1dd6dc56cc7b312be5f2ae187b82ee.

- Real official Dapr 1.18.3 and NATS 2.15.0 passed archive/binary provenance checks.
- Original command broker ACK: stream sequence 1, ACK floor 1, zero pending ACKs.
- Effective dedup window: 120 seconds, unchanged. Observation checkpoint followed the original ACK observation by 121.0125 seconds.
- Same immutable message/task identity arrived physically after the checkpoint. Replay ACK: new stream sequence 9, ACK floor 9, zero pending ACKs.
- Terminal artifacts remained equal and terminal record count remained one. Specific terminal-guard branch is not verified.
- Crash/redelivery, duplicate/content-conflict rejection, pause/cancel, official SDK SendMessage/GetTask and 20 scoped samples passed.
- 14 focused harness contracts passed with zero failures/errors/skips; pip check passed. No full-suite rerun or certification claim.
- Owned processes stopped; all declared ports (18000, 3500, 50001, 50002, 4222) were bindable after completion.

Identity: real-replay.json sourceContentSha256 is 411faa1750bee94016b275f343a72fadc1187b9c1c763c9a01f6ae5d73bec2a0, recomputed and verified against retained final files. manifest.json binds the patch, changed files and evidence. No commit or publication was performed.

Reporting caveat: replayRequestedAfterDedupSeconds is measured after receipt/ACK, not immediately before POST. Ordering is independently established by originalBrokerAck.observedAt, observationCheckpoint and replayReceipt.timestamp. reporting-label-followup.patch proposes only renaming that field to replayObservedAfterAckSeconds. It is NOT applied and is NOT the live-tested source. The preserved JSON is unchanged.

This checks safe receipt/handling of an already-completed synthetic command after explicit quarantine. It does not demonstrate new execution, exhausted retries, generic exactly-once external effects, production certification, or other infrastructure profiles. Private fixture credentials, databases and runtime logs are excluded from this deliverable.
