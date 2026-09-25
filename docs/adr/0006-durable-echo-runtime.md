# ADR 0006 - Bounded durable echo runtime and outbox relay

Status: accepted 2026-09-17 for the single-host reference slice.

## Decision

Connect the real SQLiteStore to a bounded asynchronous database lane, a
DurableEchoWorker, a SnapshotObserver and an OutboxRelay. Keep the existing Dapr
HTTP transport and the model-free echo fixture. Introduce no broker, model,
workflow engine or arbitrary external-effect executor into the core.

A worker claims an immutable command, computes the pure echo fixture, and commits
the reference SQL effect, complete terminal task, outbox event and inbox result.
Only that commit or a verified completed replay makes its callback ACK-ready.
A live competing claim remains retryable. Logical source identity remains stable
across physical replicas; random lease owners are not used in event identities.
This slice publishes only the complete terminal snapshot (sequence 3). It does
not publish intermediate fixture states as observed durable workflow progress.

The database lane owns one worker thread and accepts at most its configured
number of submitted futures (default 8). A cancelled callback can cancel queued
work but cannot stop already-running SQLite. Running operations retain capacity
until they actually finish. Started transactions may commit after caller timeout;
a redelivery reads the committed outcome or reclaims an expired unfinished claim.
Application shutdown closes admission, cancels queued work and drains started SQL.

The relay marks an outbox item only after Publisher.publish returns successfully.
Publish/mark failures retain the event; a crash between those operations may
republish identical bytes. A bounded UUID keyset cursor advances past normal
publication failures and wraps each sweep so one failing first page does not
starve all later rows. The cursor is not an acknowledgment and need not persist.
UUID scan order is not FIFO or a task ordering guarantee. Snapshot consumers
remain responsible for deduplication and sequence convergence.

Recovery reads verify stored result/outbox identity, canonical wire, digest and
scope. Inconsistent persisted evidence raises a retryable storage integrity error;
it does not turn a legitimate incoming message into poison. Published outbox rows
remain referenced by completed results. Future retention must coordinate both;
manual deletion is not a supported cleanup procedure.

## Scope and evidence

The asynchronous runtime has 37 focused tests. Together with the 103 existing
SQLite tests and 18 recovery/pagination checks, 158 tests passed in one invocation
on Python 3.13.5 with warnings as errors and zero skips. This is not evidence of
PostgreSQL transactions, real Dapr/broker delivery or complete-MVP acceptance.

## References checked 2026-09-17

- Python Future cancellation and executor shutdown:
  https://docs.python.org/3.13/library/concurrent.futures.html
- Dapr publication and callback statuses:
  https://docs.dapr.io/reference/api/pubsub_api/
- Dapr at-least-once delivery:
  https://docs.dapr.io/developing-applications/building-blocks/pubsub/pubsub-overview/
