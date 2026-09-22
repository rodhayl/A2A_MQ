"""Dapr adapter contracts and generator decisions. Fake SDK is explicitly not a live engine."""
from concurrent.futures import ThreadPoolExecutor
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from a2a_mq.domain import PermanentError, RetryableError, canonical, state_of
from a2a_mq.identity import Principal
from a2a_mq.runtime import AsyncSQLite
from a2a_mq.storage import SQLiteStore
from a2a_mq.tasks import TaskLedger
from a2a_mq.workflows import ActionRelay, DaprActions, NAME, register

USER=Principal("alice","demo",frozenset({"a2a:send","a2a:read","a2a:cancel","a2a:approve"}))


def request(**opts):
    return {"message":{"messageId":"one","role":"ROLE_USER","parts":[{"text":"one"},{"text":"two"}]},
            "metadata":{"a2amq":opts}}


@pytest.fixture
def ledger(tmp_path):
    return TaskLedger(SQLiteStore(tmp_path/"state",tenant="demo",consumer="workflow"))


class Client:
    def __init__(self):
        self.states={};self.calls=[];self.events=[];self.ambiguous=False
    def get_workflow_state(self,instance_id,*,fetch_payloads=True):
        return self.states.get(instance_id)
    def schedule_new_workflow(self,name,*,input,instance_id):
        self.calls.append(instance_id)
        self.states[instance_id]=SimpleNamespace(name=name,serialized_input=canonical(input),
                                                runtime_status=SimpleNamespace(name="RUNNING"))
        if self.ambiguous:
            self.ambiguous=False
            raise OSError("simulated lost response")
        return instance_id
    def raise_workflow_event(self,instance_id,event_name,*,data=None):
        self.events.append((instance_id,event_name,data))


def test_start_concurrency_restart_and_lost_response(ledger):
    task=ledger.submit(USER,request());wire=ledger.command(task["id"])
    client=Client();client.ambiguous=True
    actions=DaprActions(ledger,client)
    with pytest.raises(RetryableError,match="WORKFLOW_UNAVAILABLE"):
        actions.ensure_started(wire)
    with ThreadPoolExecutor(max_workers=4) as pool:
        list(pool.map(lambda _:actions.ensure_started(wire), range(8)))
    assert len(client.calls)==1
    # Even a purge after durable SQL acceptance cannot restart a completed ID.
    client.states.clear();actions.ensure_started(wire)
    assert len(client.calls)==1
    pending=ledger.pending(("start",));assert len(pending)==1
    actions.dispatch(pending[0]);assert ledger.pending(("start",))==[]


def test_existing_id_wrong_input_rejected(ledger):
    task=ledger.submit(USER,request());client=Client()
    client.states[task["id"]]=SimpleNamespace(name=NAME,serialized_input='{}')
    with pytest.raises(PermanentError,match="WORKFLOW_INPUT_CONFLICT"):
        DaprActions(ledger,client).ensure_started(ledger.command(task["id"]))
    assert not client.calls


def test_control_waits_for_start_and_delivers_once(ledger):
    tid=ledger.submit(USER,request())["id"];client=Client();actions=DaprActions(ledger,client)
    ledger.control(USER,tid,"cancel","cancel-one")
    action=ledger.pending(("cancel",))[0]
    with pytest.raises(RetryableError,match="WORKFLOW_NOT_STARTED"):
        actions.dispatch(action)
    actions.ensure_started(ledger.command(tid));actions.dispatch(action);actions.dispatch(action)
    assert len(client.events)==1 and ledger.pending(("cancel",))==[]
    assert state_of(ledger.get(USER,tid))=="SUBMITTED"


def test_runtime_failure_reconciles_without_terminal_rewrite(ledger):
    tid=ledger.submit(USER,request())["id"];client=Client();actions=DaprActions(ledger,client)
    actions.ensure_started(ledger.command(tid))
    client.states[tid].runtime_status.name="FAILED"
    actions.reconcile(tid);actions.reconcile(tid)
    assert state_of(ledger.get(USER,tid))=="FAILED"
    assert len(ledger.store.pending_outbox())==2


async def test_publish_ambiguous_retries_identical_body(ledger):
    ledger.submit(USER,request());lane=AsyncSQLite(ledger.store)
    calls=[]
    class Publisher:
        async def publish(self,e):
            calls.append(e.wire)
            if len(calls)==1:
                raise OSError("fixture")
    try:
        relay=ActionRelay(ledger,lane,publisher=Publisher())
        assert await relay.flush_once()==0
        assert await relay.flush_once()==1
        assert calls[0]==calls[1] and not ledger.pending(("publish",))
    finally:
        await lane.aclose()


class Work:
    def __init__(self,kind,arg):self.kind,self.arg=kind,arg


class FakeSDK:
    RetryPolicy=Mock
    @staticmethod
    def when_any(items):return Work("any",items)
    @staticmethod
    def when_all(items):return Work("all",items)


class EngineFixture:
    """Test-only yield driver; it supplies decisions, not persistence/replay services."""
    def __init__(self,ledger,*,cancel_at=None,deny=False,fail=False):
        self.ledger=ledger;self.cancel_at=cancel_at;self.deny=deny;self.fail=fail
        self.activities={};self.saved=[];self.any_count=0
    def register_workflow(self,fn,*,name):self.fn=fn
    def register_activity(self,fn,*,name):self.activities[name]=fn
    def call_activity(self,name,*,input,retry_policy=None):return Work("activity",(name,input))
    def wait_for_external_event(self,name):return Work("event",name)
    def create_timer(self,delta):return Work("timer",delta)
    def value(self,work):
        if work.kind=="any":
            self.any_count+=1
            return work.arg[0] if self.cancel_at==self.any_count else work.arg[1]
        if work.kind=="all":return [self.value(item) for item in work.arg]
        if work.kind=="timer":return None
        if work.kind=="activity":
            name,inp=work.arg
            if name=="a2amq-save-v1":self.saved.append(inp["state"])
            if name=="a2amq-transform-v1" and self.fail:raise ValueError("fixture transform failure")
            return self.activities[name](None,inp)
        kind=work.arg
        data=({"messageId":"continuation","role":"ROLE_USER","taskId":self.tid,
               "parts":[{"text":"updated"}]} if kind=="input" else not self.deny if kind=="auth" else None)
        self.ledger.control(USER,self.tid,kind,"control-"+kind,data)
        return {"requestId":"control-"+kind,"kind":kind,"data":data}
    def run(self,tid):
        self.tid=tid;wire=self.ledger.command(tid)
        gen=self.fn(self,{"taskId":tid,"digest":wire.digest})
        try:
            work=next(gen)
            while True:
                try:result=self.value(work)
                except Exception as error:work=gen.throw(error)
                else:work=gen.send(result)
        except StopIteration as stop:
            return stop.value


@pytest.mark.parametrize("opts", [{},{"inputRequired":True},{"authRequired":True},
                                  {"inputRequired":True,"authRequired":True,"delaySeconds":2,"transform":"upper"}])
def test_generator_states_fanout_and_continuation(ledger,opts):
    tid=ledger.submit(USER,request(**opts))["id"]
    fixture=EngineFixture(ledger);register(fixture,ledger,sdk=FakeSDK)
    task=fixture.run(tid)
    assert state_of(task)=="COMPLETED"
    if opts.get("inputRequired"):
        assert task["artifacts"][0]["parts"][0]["text"] == ("UPDATED" if opts.get("transform")=="upper" else "updated")
        assert len(task["history"])==2
    else:assert len(task["artifacts"][0]["parts"])==2
    assert state_of(ledger.get(USER,tid))=="COMPLETED"


@pytest.mark.parametrize("cancel_at", [1,2,3,4])
def test_cancellation_at_each_wait(ledger,cancel_at):
    tid=ledger.submit(USER,request(inputRequired=True,authRequired=True,delaySeconds=2))["id"]
    fixture=EngineFixture(ledger,cancel_at=cancel_at);register(fixture,ledger,sdk=FakeSDK)
    assert state_of(fixture.run(tid))=="CANCELED"
    assert "COMPLETED" not in fixture.saved


def test_denied_approval(ledger):
    tid=ledger.submit(USER,request(authRequired=True))["id"]
    fixture=EngineFixture(ledger,deny=True);register(fixture,ledger,sdk=FakeSDK)
    assert state_of(fixture.run(tid))=="REJECTED"


def test_failed_activity_is_failed_task(ledger):
    tid=ledger.submit(USER,request())["id"]
    fixture=EngineFixture(ledger,fail=True);register(fixture,ledger,sdk=FakeSDK)
    assert state_of(fixture.run(tid))=="FAILED"


@pytest.mark.parametrize("opts", [{"unknown":True},{"delaySeconds":True},{"delaySeconds":-1},
                                  {"delaySeconds":float("inf")},{"inputRequired":"yes"},
                                  {"transform":[]},{"transform":"shell"}])
def test_invalid_options_never_admitted(ledger,opts):
    with pytest.raises(PermanentError):ledger.submit(USER,request(**opts))
    assert not ledger.store.pending_outbox()


def test_unsigned_external_control_is_not_authority(ledger):
    tid=ledger.submit(USER,request())["id"]
    with pytest.raises(PermanentError,match="UNAUTHORIZED_CONTROL"):
        ledger.verify_control(tid,"cancel",{"requestId":"fake","kind":"cancel","data":None})


@pytest.mark.upstream
def test_official_workflow_registration_shape(ledger):
    sdk=pytest.importorskip("dapr.ext.workflow")
    # No server is needed to build the official registration registry.
    runtime=sdk.WorkflowRuntime(host="127.0.0.1",port="50001")
    register(runtime,ledger,sdk=sdk)
