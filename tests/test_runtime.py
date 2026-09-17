"""Real SQLite + asynchronous worker/relay integration; publication is a fixture."""
import asyncio
from contextlib import asynccontextmanager, closing
import sqlite3
import threading

import pytest

from a2a_mq.domain import Envelope, PermanentError, RetryableError
from a2a_mq.reference import echo_events
from a2a_mq.runtime import AsyncSQLite, DurableEchoWorker, OutboxRelay, SnapshotObserver
from a2a_mq.storage import SQLiteStore
from test_storage import NOW, complete, counts, request, result, store  # noqa: F401


async def until(predicate):
    async with asyncio.timeout(5):
        while not predicate():
            await asyncio.sleep(0.005)


@asynccontextmanager
async def lane(store, capacity=8):
    db = AsyncSQLite(store, capacity=capacity)
    try:
        yield db
    finally:
        await db.aclose()


class RecordingPublisher:
    def __init__(self):
        self.sent = []
        self.fail_ids = set()
    async def publish(self, event):
        self.sent.append(event)
        if event.as_dict()["messageId"] in self.fail_ids:
            raise PermanentError("PUBLISH_REJECTED")


async def test_worker_commits_without_publication_and_duplicate_uses_saved_result(store, monkeypatch):
    async with lane(store) as db:
        worker = DurableEchoWorker(db)
        await worker.handle(request())
        outcome = store.pending_outbox()[0].envelope
        assert outcome == echo_events(request(), "workers")[-1]
        assert counts(store) == dict(inbox=1, effects=1, tasks=1, outbox=1)
        def forbidden(*args, **kwargs):
            raise AssertionError("completed delivery executed twice")
        monkeypatch.setattr("a2a_mq.runtime.echo_events", forbidden)
        await DurableEchoWorker(db).handle(request())
        assert len(store.pending_outbox()) == 1


async def test_busy_claim_is_retryable_never_ack_ready(store):
    store.claim(request(), "another-worker")
    async with lane(store) as db:
        with pytest.raises(RetryableError, match="CLAIM_BUSY"):
            await DurableEchoWorker(db).handle(request())
    assert counts(store) == dict(inbox=1, effects=0, tasks=0, outbox=0)


async def test_two_worker_replicas_execute_one_sql_effect(store):
    async with lane(store) as first, lane(store) as second:
        workers = [DurableEchoWorker(first), DurableEchoWorker(second)]
        assert workers[0].owner != workers[1].owner
        outcomes = await asyncio.gather(*(w.handle(request()) for w in workers), return_exceptions=True)
        assert any(o is None for o in outcomes)
        assert all(o is None or isinstance(o, RetryableError) for o in outcomes)
        await workers[1].handle(request())
    assert counts(store) == dict(inbox=1, effects=1, tasks=1, outbox=1)


async def test_worker_error_rolls_back_and_claim_can_be_reclaimed(store, monkeypatch):
    original = store.commit_claim
    monkeypatch.setattr(store, "commit_claim", lambda *a, **kw: (_ for _ in ()).throw(RuntimeError("fixture")))
    async with lane(store) as db:
        worker = DurableEchoWorker(db, lease_seconds=1)
        with pytest.raises(RuntimeError):
            await worker.handle(request())
        assert counts(store) == dict(inbox=1, effects=0, tasks=0, outbox=0)
        store.clock = lambda: NOW + 2
        monkeypatch.setattr(store, "commit_claim", original)
        await worker.handle(request())
    assert counts(store)["effects"] == 1


@pytest.mark.parametrize("bad_request", [request(tenant="other"), result()])
async def test_worker_route_checked_before_any_claim(store, bad_request):
    async with lane(store) as db:
        with pytest.raises(PermanentError, match="UNAUTHORIZED_ROUTE"):
            await DurableEchoWorker(db).handle(bad_request)
    assert counts(store)["inbox"] == 0


async def test_worker_continuation_rejected_without_effect(store):
    d = request().as_dict()
    d["payload"]["message"]["taskId"] = "existing"
    async with lane(store) as db:
        with pytest.raises(PermanentError, match="CONTINUATION_NOT_IMPLEMENTED"):
            await DurableEchoWorker(db).handle(Envelope.from_dict(d))
    assert counts(store)["effects"] == 0


async def test_completed_reply_corruption_is_retryable(store):
    complete(store)
    with closing(sqlite3.connect(store.path)) as conn, conn:
        conn.execute("UPDATE a2amq_inbox SET result_wire=NULL")
    async with lane(store) as db:
        with pytest.raises(RetryableError, match="STORAGE_INTEGRITY_ERROR"):
            await DurableEchoWorker(db).handle(request())


async def test_cancelled_running_sql_holds_capacity_and_can_finish_commit(store, monkeypatch):
    entered, release = threading.Event(), threading.Event()
    original = store.commit_claim
    def delayed(*args, **kwargs):
        entered.set()
        assert release.wait(5)
        return original(*args, **kwargs)
    monkeypatch.setattr(store, "commit_claim", delayed)
    async with lane(store, capacity=1) as db:
        worker = DurableEchoWorker(db)
        task = asyncio.create_task(worker.handle(request()))
        try:
            await until(entered.is_set)
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
            for _ in range(20):
                with pytest.raises(RetryableError, match="STORAGE_OVERLOADED"):
                    await worker.handle(request(2))
            assert len(db._pending) == 1
        finally:
            release.set()
        await until(lambda: not db._pending)
        await worker.handle(request())
    assert counts(store) == dict(inbox=1, effects=1, tasks=1, outbox=1)


async def test_cancelled_queued_operation_does_not_start(store):
    entered, release, ran = threading.Event(), threading.Event(), threading.Event()
    def blocked():
        entered.set()
        assert release.wait(5)
    async with lane(store, capacity=2) as db:
        first = asyncio.create_task(db.call(blocked))
        try:
            await until(entered.is_set)
            second = asyncio.create_task(db.call(ran.set))
            await until(lambda: len(db._pending) == 2)
            second.cancel()
            with pytest.raises(asyncio.CancelledError):
                await second
        finally:
            release.set()
            await first
        await until(lambda: not db._pending)
        assert not ran.is_set()


async def test_close_drains_running_work_cancels_queue_and_rejects_new_work(store):
    entered, release, ran = threading.Event(), threading.Event(), threading.Event()
    def blocked():
        entered.set()
        assert release.wait(5)
    db = AsyncSQLite(store, capacity=2)
    first = asyncio.create_task(db.call(blocked))
    close = second = None
    try:
        await until(entered.is_set)
        second = asyncio.create_task(db.call(ran.set))
        await until(lambda: len(db._pending) == 2)
        close = asyncio.create_task(db.aclose())
        await until(lambda: db._closed)
        with pytest.raises(RetryableError, match="STORAGE_CLOSED"):
            await db.call(lambda: 1)
        assert not close.done()
        await until(second.done)
    finally:
        release.set()
        await first
        await db.aclose()
        if close is not None:
            await close
        if second is not None:
            with pytest.raises(asyncio.CancelledError):
                await second
    assert not ran.is_set()


async def test_operation_exception_releases_capacity(store):
    def failed():
        raise ValueError("fixture")
    async with lane(store, capacity=1) as db:
        with pytest.raises(ValueError):
            await db.call(failed)
        await until(lambda: not db._pending)
        assert await db.call(lambda: 42) == 42


@pytest.mark.parametrize("capacity", [0, 129, True, 1.5, None])
def test_invalid_lane_capacity(store, capacity):
    with pytest.raises(PermanentError, match="INVALID_STORAGE_CAPACITY"):
        AsyncSQLite(store, capacity=capacity)


async def test_relay_marks_only_success_and_replays_identical_bytes(store):
    complete(store)
    publisher = RecordingPublisher()
    item = store.pending_outbox()[0]
    mid = item.envelope.as_dict()["messageId"]
    publisher.fail_ids.add(mid)
    async with lane(store) as db:
        relay = OutboxRelay(db, publisher)
        failed = await relay.flush_once()
        assert (failed.scanned, failed.published, failed.failed) == (1, 0, 1)
        assert store.pending_outbox()[0].attempts == 1
        publisher.fail_ids.clear()
        passed = await OutboxRelay(db, publisher).flush_once()
        assert (passed.scanned, passed.published, passed.failed) == (1, 1, 0)
    assert store.pending_outbox() == []
    assert publisher.sent[0].wire == publisher.sent[1].wire
    assert store.claim(request(), "duplicate").status == "completed"


async def test_publish_before_mark_failure_remains_replayable(store, monkeypatch):
    complete(store)
    publisher = RecordingPublisher()
    original = store.mark_published
    def failed(item):
        raise RetryableError("STORAGE_BUSY")
    async with lane(store) as db:
        monkeypatch.setattr(store, "mark_published", failed)
        assert (await OutboxRelay(db, publisher).flush_once()).failed == 1
        monkeypatch.setattr(store, "mark_published", original)
        assert (await OutboxRelay(db, publisher).flush_once()).published == 1
    assert len(publisher.sent) == 2 and publisher.sent[0] == publisher.sent[1]
    assert not store.pending_outbox()


async def test_poison_publish_does_not_starve_later_pages(store, caplog):
    for n in range(5):
        complete(store, request(n), result(request(n), task_id=f"task-{n}"), key=f"effect-{n}")
    publisher = RecordingPublisher()
    poison = store.pending_outbox()[0].envelope.as_dict()["messageId"]
    publisher.fail_ids.add(poison)
    async with lane(store) as db:
        relay = OutboxRelay(db, publisher, batch_size=1)
        for _ in range(6):
            await relay.flush_once()
        remaining = store.pending_outbox()
        assert len(remaining) == 1 and remaining[0].envelope.as_dict()["messageId"] == poison
        assert len(publisher.sent) == 5
        publisher.fail_ids.clear()
        assert (await relay.flush_once()).published == 1
    assert "hello" not in caplog.text


async def test_cancelled_publish_keeps_pending_event(store):
    complete(store)
    entered = asyncio.Event()
    class Blocked:
        async def publish(self, event):
            entered.set()
            await asyncio.Event().wait()
    async with lane(store) as db:
        task = asyncio.create_task(OutboxRelay(db, Blocked()).flush_once())
        try:
            async with asyncio.timeout(5):
                await entered.wait()
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
            assert len(store.pending_outbox()) == 1
        finally:
            if not task.done():
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)


async def test_publish_timeout_keeps_event(store):
    complete(store)
    class Blocked:
        async def publish(self, event):
            await asyncio.Event().wait()
    async with lane(store) as db:
        async with asyncio.timeout(5):
            assert (await OutboxRelay(db, Blocked(), publish_timeout=0.01).flush_once()).failed == 1
    assert len(store.pending_outbox()) == 1


async def test_serialized_relay_calls_skip_published_rows(store):
    complete(store)
    publisher = RecordingPublisher()
    async with lane(store) as db:
        relay = OutboxRelay(db, publisher)
        responses = await asyncio.gather(relay.flush_once(), relay.flush_once())
        assert sum(r.published for r in responses) == 1
    assert len(publisher.sent) == 1


async def test_two_durable_observers_receive_same_snapshots_independently(store):
    observer_store = SQLiteStore(store.path, tenant="demo", consumer="observer-b", clock=lambda: NOW)
    async with lane(store) as first, lane(observer_store) as second:
        a, b = SnapshotObserver(first), SnapshotObserver(second)
        for seq, state in [(3, "COMPLETED"), (1, "SUBMITTED"), (2, "WORKING"), (3, "COMPLETED")]:
            event = result(sequence=seq, state=state)
            await a.handle(event)
            await b.handle(event)
    assert store.get_task("task-1") == observer_store.get_task("task-1")
    assert counts(store)["inbox"] == counts(observer_store)["inbox"] == 3


async def test_relay_loop_stops_and_recovers_from_read_error(store, monkeypatch):
    complete(store)
    original = store.pending_outbox
    count = 0
    def flaky(**kwargs):
        nonlocal count
        count += 1
        if count == 1:
            raise RetryableError("STORAGE_BUSY")
        return original(**kwargs)
    monkeypatch.setattr(store, "pending_outbox", flaky)
    publisher = RecordingPublisher()
    stop = asyncio.Event()
    async with lane(store) as db:
        task = asyncio.create_task(OutboxRelay(db, publisher).run(stop, interval=0.01))
        try:
            await until(lambda: bool(publisher.sent))
            stop.set()
            async with asyncio.timeout(5):
                await task
        finally:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
    assert count >= 2 and not original()


async def test_nonterminal_saved_outcome_is_not_echo_completion(store):
    complete(store, event=result(state="WORKING"))
    async with lane(store) as db:
        with pytest.raises(RetryableError, match="STORAGE_INTEGRITY_ERROR"):
            await DurableEchoWorker(db).handle(request())


@pytest.mark.parametrize("kind", ["bad", [], None, 1])
async def test_invalid_route_configuration_fails_closed(store, kind):
    async with lane(store) as db:
        with pytest.raises(PermanentError, match="INVALID_ROUTE"):
            DurableEchoWorker(db, target_kind=kind)


async def test_agent_route_requires_matching_logical_source(store):
    async with lane(store) as db:
        with pytest.raises(PermanentError, match="AGENT_SOURCE_MISMATCH"):
            DurableEchoWorker(db, target_kind="agent", target="a", source="b")
        worker = DurableEchoWorker(db, target_kind="agent", target="agent-a", source="agent-a")
        d = request().as_dict()
        d["target"] = {"agentId": "agent-a"}
        await worker.handle(Envelope.from_dict(d))
        assert store.pending_outbox()[0].envelope.as_dict()["source"] == "agent-a"


@pytest.mark.parametrize("value", [0, float("nan"), True, None, 301])
async def test_invalid_relay_deadline(store, value):
    async with lane(store) as db:
        with pytest.raises(PermanentError, match="INVALID_TIMEOUT"):
            OutboxRelay(db, RecordingPublisher(), publish_timeout=value)


async def test_cancelled_close_can_be_awaited_again_to_finish_drain(store):
    entered, release = threading.Event(), threading.Event()
    def blocked():
        entered.set()
        assert release.wait(5)
    db = AsyncSQLite(store)
    task = asyncio.create_task(db.call(blocked))
    try:
        await until(entered.is_set)
        shutdown = asyncio.create_task(db.aclose())
        await until(lambda: db._shutdown is not None)
        shutdown.cancel()
        with pytest.raises(asyncio.CancelledError):
            await shutdown
        assert db._shutdown is not None and not db._shutdown.cancelled()
    finally:
        release.set()
        await task
        await db.aclose()
    assert not db._pending
