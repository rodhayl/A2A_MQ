"""Real transactional projection quotas and compact replay/terminal tombstones."""
from uuid import uuid4
import pytest
from a2a_mq.domain import Envelope, PermanentError, RetryableError, command
from a2a_mq.reference import echo_events
from a2a_mq.storage import SQLiteStore
from a2a_mq.projections import ProjectionPolicy


@pytest.fixture
def state(tmp_path):
    clock=[1000000.0]
    store=SQLiteStore(tmp_path/'db',tenant='demo',consumer='observer',clock=lambda:clock[0])
    events=echo_events(command('private-projection'))
    return store,ProjectionPolicy(store),events,clock


def test_retire_terminal_preserves_duplicate_identity_and_prevents_resurrection(state):
    store,policy,events,clock=state
    for e in events:
        policy.apply(e)
    tid=events[-1].as_dict()['taskId']
    clock[0]+=604801
    assert policy.retire()['taskIds']==[tid]
    assert store.get_task(tid)
    assert policy.retire(dry_run=False)['retired']==1
    assert store.get_task(tid) is None
    assert all(policy.apply(e) is False for e in events)
    with store._connection() as db:
        assert db.execute('SELECT COUNT(*) FROM a2amq_inbox').fetchone()[0]==0
    d=events[0].as_dict()
    d['messageId']=str(uuid4())
    assert policy.apply(Envelope.from_dict(d)) is False
    assert store.get_task(tid) is None


@pytest.mark.parametrize('change,code',[
 ('source','TASK_OWNER_CONFLICT'),('context','TASK_CONTEXT_CONFLICT'),
 ('payload','TASK_VERSION_CONFLICT'),('sequence','TERMINAL_TASK_REWRITE')])
def test_retired_task_rejects_conflicts(state,change,code):
    _,policy,events,clock=state
    policy.apply(events[-1])
    clock[0]+=604801
    policy.retire(dry_run=False)
    d=events[-1].as_dict()
    d['messageId']=str(uuid4())
    if change=='source':
        d['source']='another-owner'
    elif change=='context':
        d['contextId']=d['payload']['contextId']='other'
    elif change=='payload':
        d['payload']['artifacts']=[]
    else:
        d['sequence']+=1
    with pytest.raises(PermanentError,match=code):
        policy.apply(Envelope.from_dict(d))


def test_quota_remains_enforced_after_retirement(state):
    store,_,events,clock=state
    policy=ProjectionPolicy(store,max_records=1)
    policy.apply(events[-1])
    clock[0]+=604801
    policy.retire(dry_run=False)
    assert not policy.apply(events[-1])
    with pytest.raises(RetryableError,match='PROJECTION_QUOTA_EXCEEDED'):
        policy.apply(echo_events(command('new'))[-1])


def test_nonterminal_projection_never_deleted(state):
    store,policy,events,clock=state
    policy.apply(events[1])
    clock[0]+=604801
    assert policy.retire(dry_run=False)['retired']==0
    assert store.get_task(events[1].as_dict()['taskId'])


def test_scopes_retire_independently(state):
    store,policy,events,clock=state
    other=SQLiteStore(store.path,tenant='demo',consumer='observer-2',clock=lambda:clock[0])
    other_policy=ProjectionPolicy(other)
    policy.apply(events[-1])
    other_policy.apply(events[-1])
    clock[0]+=604801
    policy.retire(dry_run=False)
    assert other.get_task(events[-1].as_dict()['taskId'])


def test_changed_message_after_retirement_rejected(state):
    _,policy,events,clock=state
    policy.apply(events[-1])
    clock[0]+=604801
    policy.retire(dry_run=False)
    d=events[-1].as_dict()
    d['payload']['artifacts']=[]
    with pytest.raises(PermanentError,match='MESSAGE_ID_CONFLICT'):
        policy.apply(Envelope.from_dict(d))


@pytest.mark.parametrize('retired',[False,True])
def test_duplicate_with_missing_projection_evidence_is_not_acknowledged(state,retired):
    store,policy,events,clock=state
    policy.apply(events[-1])
    if retired:
        clock[0]+=604801
        policy.retire(dry_run=False)
    with store._transaction() as db:
        db.execute('DELETE FROM '+('a2amq_projection_retired' if retired else 'a2amq_tasks'))
    with pytest.raises(RetryableError,match='PROJECTION_INTEGRITY_ERROR'):
        policy.apply(events[-1])
