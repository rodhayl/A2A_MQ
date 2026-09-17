"""Real transactional admission, principal isolation and workflow control records."""
from dataclasses import replace
from concurrent.futures import ThreadPoolExecutor
import pytest

from a2a_mq.domain import PermanentError, RetryableError, state_of
from a2a_mq.identity import Principal
from a2a_mq.storage import SQLiteStore
from a2a_mq.tasks import TaskLedger

ALICE=Principal("alice","demo",frozenset({"a2a:send","a2a:read","a2a:cancel","a2a:approve"}))
BOB=replace(ALICE,subject="bob")


def request(mid="m1", text="hello"):
    return {"message":{"messageId":mid,"role":"ROLE_USER","parts":[{"text":text}]}}


@pytest.fixture
def ledger(tmp_path):
    return TaskLedger(SQLiteStore(tmp_path/"db",tenant="demo",consumer="workflow",clock=lambda: 1790000000))


def test_admission_restart_duplicate_and_conflict(ledger):
    task=ledger.submit(ALICE,request())
    assert state_of(task)=="SUBMITTED"
    assert len(ledger.pending(("publish",)))==1
    reopened=TaskLedger(SQLiteStore(ledger.store.path,tenant="demo",consumer="workflow"))
    assert reopened.submit(ALICE,request())==task
    assert reopened.get(ALICE,task["id"])==task
    with pytest.raises(PermanentError,match="MESSAGE_ID_CONFLICT"):
        reopened.submit(ALICE,request(text="changed"))


def test_owner_isolation_and_same_request_id(ledger):
    a=ledger.submit(ALICE,request());b=ledger.submit(BOB,request())
    assert a["id"] != b["id"]
    assert ledger.list(ALICE)[0]==[a]
    with pytest.raises(PermanentError,match="FORBIDDEN"):
        ledger.get(BOB,a["id"])
    with pytest.raises(PermanentError,match="FORBIDDEN"):
        ledger.control(BOB,a["id"],"cancel","c1")
    with pytest.raises(PermanentError,match="FORBIDDEN"):
        ledger.submit(replace(ALICE,tenant="other"),request())


def test_queued_control_and_idempotent_steps(ledger):
    task=ledger.submit(ALICE,request());tid=task["id"]
    ledger.accept(ledger.command(tid))
    assert len(ledger.pending(("start",)))==1
    ledger.step(tid,"working","WORKING")
    paused=ledger.step(tid,"input","INPUT_REQUIRED")
    message={**request("m2")["message"],"taskId":tid,"contextId":task["contextId"]}
    assert ledger.submit(ALICE,{"message":message})==paused
    ledger.step(tid,"resumed","WORKING")
    assert ledger.submit(ALICE,{"message":message})["id"]==tid
    ledger.step(tid,"auth","AUTH_REQUIRED")
    ledger.control(ALICE,tid,"auth","approval",True)
    ledger.step(tid,"authorized","WORKING")
    final=ledger.step(tid,"done","COMPLETED",artifacts=[{"artifactId":"result","parts":[{"text":"done"}]}])
    assert ledger.step(tid,"done","COMPLETED",artifacts=final["artifacts"])==final
    assert len(ledger.store.pending_outbox())==7
    with pytest.raises(PermanentError,match="WORKFLOW_STEP_CONFLICT"):
        ledger.step(tid,"done","FAILED")
    with pytest.raises(PermanentError,match="TERMINAL_TASK_REWRITE"):
        ledger.step(tid,"late","CANCELED")
    assert ledger.get(ALICE,tid)==final


def test_explicit_cancel_does_not_lie_about_completion(ledger):
    task=ledger.submit(ALICE,request());tid=task["id"]
    assert state_of(ledger.control(ALICE,tid,"cancel","cancel"))=="SUBMITTED"
    ledger.step(tid,"cancelled","CANCELED")
    assert state_of(ledger.control(ALICE,tid,"cancel","cancel"))=="CANCELED"
    with pytest.raises(PermanentError,match="TASK_NOT_CANCELABLE"):
        ledger.control(ALICE,tid,"cancel","new-cancel")


def test_quota_applies_only_to_unfinished_tasks(ledger):
    ledger.max_active=1
    task=ledger.submit(ALICE,request())
    assert ledger.submit(ALICE,request())==task
    with pytest.raises(RetryableError,match="ADMISSION_QUOTA_EXCEEDED"):
        ledger.submit(ALICE,request("m2"))
    ledger.step(task["id"],"cancelled","CANCELED")
    assert ledger.submit(ALICE,request("m2"))


def test_admission_race_has_one_task_and_one_command(ledger):
    with ThreadPoolExecutor(max_workers=8) as pool:
        tasks=list(pool.map(lambda n:ledger.submit(ALICE,request()),range(16)))
    assert all(t==tasks[0] for t in tasks)
    assert len(ledger.pending(("publish",)))==1
    assert len(ledger.store.pending_outbox())==1


def test_action_ack_idempotent_and_strict(ledger):
    ledger.submit(ALICE,request())
    item=ledger.pending(("publish",))[0]
    with pytest.raises(PermanentError,match="CONTROL_ID_CONFLICT"):
        ledger.delivered(replace(item,body="{}"))
    ledger.delivered(item);ledger.delivered(item)
    assert ledger.pending(("publish",))==[]


def test_control_conflict_after_consumption(ledger):
    task=ledger.submit(ALICE,request());tid=task["id"]
    ledger.step(tid,"working","WORKING");ledger.step(tid,"pause","INPUT_REQUIRED")
    msg={**request("m2")["message"],"taskId":tid}
    ledger.control(ALICE,tid,"input","m2",msg)
    with pytest.raises(PermanentError,match="CONTROL_ID_CONFLICT"):
        ledger.control(ALICE,tid,"input","m2",{**msg,"parts":[{"text":"changed"}]})


@pytest.mark.parametrize("state",["COMPLETED","INPUT_REQUIRED","AUTH_REQUIRED"])
def test_executor_adjacent_state_guards(ledger,state):
    task=ledger.submit(ALICE,request())
    with pytest.raises(PermanentError,match="INVALID_TASK_TRANSITION"):
        ledger.step(task["id"],"invalid",state)
    assert len(ledger.store.pending_outbox())==1


def test_pagination_filters_scope(ledger):
    for i in range(5):ledger.submit(ALICE,request(f"m{i}"))
    ledger.submit(BOB,request("m0"))
    first,cursor=ledger.list(ALICE,limit=2)
    second,cursor2=ledger.list(ALICE,limit=2,after=cursor)
    third,end=ledger.list(ALICE,limit=2,after=cursor2)
    assert len({t["id"] for t in first+second+third})==5 and end is None
    assert not ledger.list(ALICE,state="COMPLETED")[0]


def test_transaction_rolls_back_on_result_failure(ledger,monkeypatch):
    def fail(*args,**kwargs):raise RuntimeError("fixture")
    monkeypatch.setattr(ledger.store,"_project",fail)
    with pytest.raises(RuntimeError):ledger.submit(ALICE,request())
    assert not ledger.pending(("publish",))
    with ledger.store._connection() as db:
        assert db.execute("SELECT count(*) FROM a2amq_jobs").fetchone()[0]==0
