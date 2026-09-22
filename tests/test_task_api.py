"""Real SQL facade contract, including synchronous waits and snapshot streaming."""
import asyncio
from dataclasses import replace

import pytest

from a2a_mq.domain import PermanentError, RetryableError, state_of
from a2a_mq.identity import Principal
from a2a_mq.runtime import AsyncSQLite
from a2a_mq.storage import SQLiteStore
from a2a_mq.tasks import TaskLedger
from a2a_mq.task_api import TaskAPI

USER = Principal("alice", "demo", frozenset({"a2a:send", "a2a:read", "a2a:cancel", "a2a:approve"}))


def request(mid="one", **config):
    return {"message": {"messageId": mid, "role": "ROLE_USER", "parts": [{"text": "hello"}]},
            "configuration": config}


@pytest.fixture
async def api(tmp_path):
    ledger = TaskLedger(SQLiteStore(tmp_path / "state", tenant="demo", consumer="workflow"))
    lane = AsyncSQLite(ledger.store)
    facade = TaskAPI(ledger, lane, wait_timeout=.2, poll_interval=.005, stream_timeout=.2)
    try:
        yield facade
    finally:
        await lane.aclose()


async def step(api, tid, name, state, **kwargs):
    return await api.database.call(api.ledger.step, tid, name, state, **kwargs)


async def finish(api, tid):
    await step(api, tid, "working", "WORKING")
    return await step(api, tid, "complete", "COMPLETED", artifacts=[{"artifactId": "result", "parts": [{"text": "done"}]}])


async def test_default_send_waits_for_completion(api):
    job = asyncio.create_task(api.send(USER, request()))
    try:
        while not api.ledger.list(USER)[0]:
            await asyncio.sleep(.001)
        tid = api.ledger.list(USER)[0][0]["id"]
        assert not job.done()
        await finish(api, tid)
        assert state_of(await job) == "COMPLETED"
    finally:
        job.cancel()
        await asyncio.gather(job, return_exceptions=True)


async def test_send_timeout_leaves_durable_admission(api):
    with pytest.raises(RetryableError, match="TASK_WAIT_TIMEOUT"):
        await api.send(USER, request())
    assert len(api.ledger.pending(("publish",))) == 1
    task = api.ledger.list(USER)[0][0]
    assert state_of(task) == "SUBMITTED"


async def test_immediate_send_history_zero_and_idempotency(api):
    req = request(returnImmediately=True, historyLength=0)
    task = await api.send(USER, req)
    assert "history" not in task and "taskId" not in req["message"]
    assert await api.send(USER, req) == task
    assert len((await api.get(USER, task["id"]))["history"]) == 1


async def test_owner_hidden_and_tenant_rejected(api):
    task = await api.send(USER, request(returnImmediately=True))
    with pytest.raises(PermanentError, match="TASK_NOT_FOUND"):
        await api.get(replace(USER, subject="bob"), task["id"])
    with pytest.raises(PermanentError, match="FORBIDDEN"):
        await api.get(replace(USER, tenant="other"), task["id"])


async def test_cancel_waits_for_owner_and_is_idempotent(api):
    tid = (await api.send(USER, request(returnImmediately=True)))["id"]
    operation = asyncio.create_task(api.cancel(USER, tid))
    try:
        while not api.ledger.pending(("cancel",)):
            await asyncio.sleep(.001)
        assert not operation.done()
        await step(api, tid, "canceled", "CANCELED")
        assert state_of(await operation) == "CANCELED"
        assert state_of(await api.cancel(USER, tid)) == "CANCELED"
    finally:
        operation.cancel()
        await asyncio.gather(operation, return_exceptions=True)


async def test_complete_wins_cancel_race(api):
    tid = (await api.send(USER, request(returnImmediately=True)))["id"]
    operation = asyncio.create_task(api.cancel(USER, tid))
    try:
        while not api.ledger.pending(("cancel",)):
            await asyncio.sleep(.001)
        await finish(api, tid)
        with pytest.raises(PermanentError, match="TASK_NOT_CANCELABLE"):
            await operation
    finally:
        operation.cancel()
        await asyncio.gather(operation, return_exceptions=True)


async def test_continuation_does_not_return_old_pause(api):
    tid = (await api.send(USER, request(returnImmediately=True)))["id"]
    await step(api, tid, "working", "WORKING")
    await step(api, tid, "pause", "INPUT_REQUIRED")
    req = request("second")
    req["message"]["taskId"] = tid
    operation = asyncio.create_task(api.send(USER, req))
    try:
        while not api.ledger.pending(("input",)):
            await asyncio.sleep(.001)
        assert not operation.done()
        await step(api, tid, "continued", "WORKING", message=req["message"])
        await step(api, tid, "approval", "AUTH_REQUIRED")
        paused = await operation
        assert state_of(paused) == "AUTH_REQUIRED"
        assert "approve" in paused["status"]["message"]["parts"][0]["text"]
        assert state_of(await api.send(USER, req)) == "AUTH_REQUIRED"
    finally:
        operation.cancel()
        await asyncio.gather(operation, return_exceptions=True)


async def test_one_input_and_approval_per_reference_wait(api):
    tid = (await api.send(USER, request(returnImmediately=True)))["id"]
    await step(api, tid, "working", "WORKING")
    await step(api, tid, "approval", "AUTH_REQUIRED")
    await api.approve(USER, tid, "a1", True)
    with pytest.raises(PermanentError, match="CONTROL_ALREADY_ACCEPTED"):
        await api.approve(USER, tid, "a2", False)
    assert len(api.ledger.pending(("auth",))) == 1


async def test_stream_capacity_cleanup_and_terminal(api):
    api.max_streams = 1
    tid = (await api.send(USER, request(returnImmediately=True)))["id"]
    stream = api.snapshots(USER, tid)
    assert state_of(await anext(stream)) == "SUBMITTED"
    extra = api.snapshots(USER, tid)
    with pytest.raises(RetryableError, match="STREAM_CAPACITY_EXCEEDED"):
        await anext(extra)
    await stream.aclose()
    assert api._streams == 0
    await finish(api, tid)
    terminal = api.snapshots(USER, tid)
    assert state_of(await anext(terminal)) == "COMPLETED"
    with pytest.raises(StopAsyncIteration):
        await anext(terminal)
    assert api._streams == 0


async def test_list_timestamp_order_filter_and_opaque_cursor(api):
    clock = [1790000000.0]
    api.ledger.store.clock = lambda: clock[0]
    tasks = []
    for i in range(5):
        tasks.append(await api.send(USER, request(str(i), returnImmediately=True)))
        clock[0] += 1
    await finish(api, tasks[0]["id"])
    first = await api.list(USER, page_size=2)
    assert first["totalSize"] == 5 and first["tasks"][0]["id"] == tasks[0]["id"]
    assert "artifacts" not in first["tasks"][0]
    second = await api.list(USER, page_size=2, page_token=first["nextPageToken"])
    third = await api.list(USER, page_size=2, page_token=second["nextPageToken"])
    assert len({t["id"] for t in first["tasks"] + second["tasks"] + third["tasks"]}) == 5
    assert third["nextPageToken"] == "" and third["pageSize"] == 2
    with pytest.raises(PermanentError, match="INVALID_PAGE_TOKEN"):
        await api.list(replace(USER, subject="bob"), page_token=first["nextPageToken"])
    completed = await api.list(USER, status="COMPLETED", include_artifacts=True, history_length=0)
    assert completed["totalSize"] == 1 and completed["tasks"][0]["artifacts"]
    assert "history" not in completed["tasks"][0]


@pytest.mark.parametrize("config", [{"returnImmediately":"yes"}, {"historyLength":True},
    {"historyLength":-1}, {"acceptedOutputModes":["audio/wav"]}, {"acceptedOutputModes":{}},
    {"taskPushNotificationConfig":{}}, {"unknown":True}])
async def test_invalid_configuration_never_admitted(api, config):
    with pytest.raises(PermanentError):
        await api.send(USER, request(**config))
    assert not api.ledger.pending(("publish",))


async def test_send_only_scope_can_admit_immediate_without_read_authority(api):
    sender = Principal("sender", "demo", frozenset({"a2a:send"}))
    task = await api.send(sender, request("send-only", returnImmediately=True))
    assert state_of(task) == "SUBMITTED"
    assert len(api.ledger.pending(("publish",))) == 1
    with pytest.raises(PermanentError, match="FORBIDDEN"):
        await api.get(sender, task["id"])


async def test_send_only_scope_cannot_request_waiting_mode_or_admit_side_effect(api):
    sender = Principal("sender", "demo", frozenset({"a2a:send"}))
    with pytest.raises(PermanentError, match="FORBIDDEN"):
        await api.send(sender, request("send-and-wait"))
    assert api.ledger.pending(("publish",)) == []


async def test_send_stream_capacity_is_reserved_before_durable_admission(api):
    api.max_streams = 1
    first = api.send_snapshots(USER, request("stream-one", returnImmediately=True))
    assert state_of(await anext(first)) == "SUBMITTED"
    assert api._streams == 1

    second = api.send_snapshots(USER, request("stream-two", returnImmediately=True))
    with pytest.raises(RetryableError, match="STREAM_CAPACITY_EXCEEDED"):
        await anext(second)
    await second.aclose()

    jobs = api.ledger.list(USER)[0]
    assert [task["history"][0]["messageId"] for task in jobs] == ["stream-one"]
    assert len(api.ledger.pending(("publish",))) == 1

    await first.aclose()
    assert api._streams == 0
