"""Real SQLite retirement, backup and DLQ replay; only publisher calls are fixtures."""
import base64
import hashlib

import pytest

from a2a_mq.sqlite_archive import backup_sqlite, restore_sqlite
from a2a_mq.deadletters import DeadLetters, ReplayService
from a2a_mq.domain import PermanentError, RetryableError, canonical, command
from a2a_mq.identity import Principal
from a2a_mq.maintenance import Operations
from a2a_mq.runtime import AsyncSQLite
from a2a_mq.storage import SQLiteStore
from a2a_mq.tasks import TaskLedger
from a2a_mq.transport import cloud_event


@pytest.fixture
def setup(tmp_path):
    clock = [1000000.0]
    store = SQLiteStore(tmp_path/'state.db',tenant='demo',consumer='workflow',clock=lambda:clock[0])
    ledger = TaskLedger(store)
    principal = Principal('user','demo',frozenset({'a2a:admin'}))
    return store,ledger,principal,clock


def finish(setup):
    store,ledger,p,clock = setup
    request = {'message': {'messageId':'request-1','role':'ROLE_USER','parts':[{'text':'secret-payload'}]}}
    task = ledger.submit(p,request)
    ledger.step(task['id'],'working','WORKING')
    ledger.step(task['id'],'completed','COMPLETED')
    for a in ledger.pending(('publish',)):
        ledger.delivered(a)
    for item in store.pending_outbox():
        store.record_publish_attempt(item)
        store.mark_published(item)
    with store._transaction() as db:
        db.execute('UPDATE a2amq_jobs SET started=1')
        db.execute('INSERT INTO a2amq_runtime_end VALUES (?,?,?,?)',(*store._keys,task['id'],clock[0]))
    clock[0] += 604801
    return request,task


def test_retire_dry_run_then_atomic_delete_and_tombstone(setup):
    store,ledger,p,_ = setup
    request,task=finish(setup)
    original=ledger.command(task['id'])
    ops=Operations(ledger)
    assert ops.retire(p)['taskIds']==[task['id']]
    assert store.get_task(task['id']) is not None
    assert ops.retire(p,dry_run=False)['retired']==1
    assert store.get_task(task['id']) is None
    with pytest.raises(PermanentError,match='TASK_RETIRED'):
        ledger.submit(p,request)
    assert ledger.accept(original) is False
    assert ops.backlog(p)['retiredIdentities']==1
    assert ops.retire(p,dry_run=False)['retired']==0


@pytest.mark.parametrize('blocker',['runtime','pending-publication','pending-action','nonterminal','young'])
def test_retirement_preserves_unfinished_or_unpublished(setup,blocker):
    store,ledger,p,clock=setup
    _,task=finish(setup)
    with store._transaction() as db:
        if blocker=='runtime':
            db.execute('DELETE FROM a2amq_runtime_end')
        elif blocker=='pending-publication':
            db.execute('UPDATE a2amq_outbox SET published_at=NULL')
        elif blocker=='pending-action':
            db.execute('UPDATE a2amq_actions SET delivered_at=NULL')
        elif blocker=='nonterminal':
            db.execute("UPDATE a2amq_tasks SET state='WORKING'")
        else:
            clock[0]-=604800
    assert Operations(ledger).retire(p,dry_run=False)['retired']==0
    assert store.get_task(task['id']) is not None


def test_retire_integrity_failure_rolls_back(setup):
    store,ledger,p,_=setup
    _,task=finish(setup)
    with store._transaction() as db:
        db.execute("UPDATE a2amq_outbox SET digest='tampered'")
    with pytest.raises(RetryableError,match='STORAGE_INTEGRITY_ERROR'):
        Operations(ledger).retire(p,dry_run=False)
    assert store.get_task(task['id']) is not None
    assert Operations(ledger).backlog(p)['retiredIdentities']==0


@pytest.mark.parametrize('tenant,scopes',[('other',{'a2a:admin'}),('demo',{'a2a:read'})])
def test_operations_authorization(setup,tenant,scopes):
    _,ledger,_,_=setup
    p=Principal('other',tenant,frozenset(scopes))
    for op in (Operations(ledger).backlog,Operations(ledger).retire):
        with pytest.raises(PermanentError,match='FORBIDDEN'):
            op(p)


@pytest.mark.parametrize('retention',[0,True,3599,315360001,'1000'])
def test_retention_bounds(setup,retention):
    with pytest.raises(PermanentError,match='INVALID_RETENTION'):
        Operations(setup[1]).retire(setup[2],retention_seconds=retention)


def test_retirement_scoped_to_one_consumer(setup):
    store,ledger,p,_=setup
    _,task=finish(setup)
    other=SQLiteStore(store.path,tenant='demo',consumer='other')
    event=store.pending_outbox()
    assert not event
    other_ledger=TaskLedger(other)
    assert Operations(other_ledger).retire(p,dry_run=False)['retired']==0
    assert store.get_task(task['id']) is not None


def test_backup_captures_wal_and_restore_new_path(setup,tmp_path):
    store,_,_,_=setup
    finish(setup)
    result=backup_sqlite(store.path,tmp_path/'backup.db')
    assert result['bytes']>0 and (tmp_path/'backup.db').stat().st_mode & 0o077==0
    restore_sqlite(tmp_path/'backup.db',tmp_path/'restore.db',expected_sha256=result['sha256'])
    recovered=SQLiteStore(tmp_path/'restore.db',tenant='demo',consumer='workflow')
    assert recovered.list_tasks()==store.list_tasks()
    with pytest.raises(PermanentError,match='BACKUP_DESTINATION_EXISTS'):
        backup_sqlite(store.path,tmp_path/'restore.db')
    with pytest.raises(PermanentError,match='BACKUP_DIGEST_MISMATCH'):
        restore_sqlite(tmp_path/'backup.db',tmp_path/'bad.db',expected_sha256='0'*64)
    assert not (tmp_path/'bad.db').exists()


def test_backup_rejects_symlink(setup,tmp_path):
    source=tmp_path/'link'
    source.symlink_to(setup[0].path)
    with pytest.raises(PermanentError,match='INVALID_BACKUP_SOURCE'):
        backup_sqlite(source,tmp_path/'backup')


def dlq_event(envelope=None):
    e=envelope or command('confidential')
    return {**cloud_event(e),'topic':'a2a.v1.demo.dlq','pubsubname':'bus'}


def test_dlq_capture_dedup_quota_and_metadata_only_list(setup):
    store,_,p,_=setup
    archive=DeadLetters(store,max_records=1)
    e=dlq_event()
    rid=archive.capture(e)
    assert archive.capture({**e,'deliveryAttempt':2})==rid
    assert 'confidential' not in canonical(archive.list(p))
    with pytest.raises(RetryableError,match='DLQ_QUOTA_EXCEEDED'):
        archive.capture(dlq_event())
    e['data']['payload']['message']['parts'][0]['text']='changed'
    with pytest.raises(PermanentError,match='DLQ_ID_CONFLICT'):
        archive.capture(e)


@pytest.mark.parametrize('field,value',[('topic','a2a.v1.other.dlq'),('pubsubname','other'),('specversion','0.3')])
def test_dlq_rejects_untrusted_delivery(setup,field,value):
    archive=DeadLetters(setup[0])
    e=dlq_event()
    e[field]=value
    with pytest.raises(PermanentError,match='INVALID_DLQ_DELIVERY'):
        archive.capture(e)


class Bus:
    def __init__(self):
        self.sent=[]
        self.available=False
    async def _post(self,route,event):
        self.sent.append((route,canonical(event)))
        if not self.available:
            raise RetryableError('UNAVAILABLE')


async def test_replay_retains_bytes_across_failure_restart_and_duplicate_request(setup):
    store,_,p,_=setup
    archive=DeadLetters(store)
    event=dlq_event()
    rid=archive.capture(event)
    database=AsyncSQLite(store)
    bus=Bus()
    try:
        service=ReplayService(archive,database,bus,allowed_routes=frozenset({'a2a.v1.demo.command.skill.echo'}))
        replay=await service.enqueue(p,rid,'replay-request')
        assert await service.enqueue(p,rid,'replay-request')==replay
        assert await service.flush_once()==0
        other=ReplayService(DeadLetters(store),database,bus,allowed_routes=service.allowed_routes)
        bus.available=True
        assert await other.flush_once()==1
        assert bus.sent[0]==bus.sent[1]
        assert await other.flush_once()==0
        assert await other.enqueue(p,rid,'replay-request')==replay
        assert await other.flush_once()==0
    finally:
        await database.aclose()


@pytest.mark.parametrize('mutator,code',[
    ('source','IDENTIFIER_MISMATCH'),('route','FORBIDDEN'),('expired','EXPIRED'),('truncated','TRUNCATED_DLQ_NOT_REPLAYABLE'),
    ('digest','DLQ_DIGEST_MISMATCH'),('scope','FORBIDDEN')])
async def test_unsafe_replay_is_not_queued(setup,mutator,code):
    store,_,p,_=setup
    event=dlq_event()
    if mutator=='source':
        event['source']='urn:a2a-mq:other'
    if mutator=='expired':
        event['data']['createdAt']='2000-01-01T00:00:00Z'
        event['data']['expiresAt']='2000-01-02T00:00:00Z'
    if mutator in {'truncated','digest'}:
        raw=canonical(event).encode()
        event={**event,'type':'a2a-mq.deadletter','data':{'reason':'INVALID_JSON','truncated':mutator=='truncated',
             'originalBase64':base64.b64encode(raw).decode(),'capturedBytesSha256':'bad' if mutator=='digest' else hashlib.sha256(raw).hexdigest()}}
    if mutator=='scope':
        p=Principal('other','other',frozenset({'a2a:admin'}))
    archive=DeadLetters(store)
    rid=archive.capture(event)
    database=AsyncSQLite(store)
    try:
        routes=frozenset({'a2a.v1.demo.command.agent.other'} if mutator=='route' else {'a2a.v1.demo.command.skill.echo'})
        service=ReplayService(archive,database,Bus(),allowed_routes=routes)
        with pytest.raises(PermanentError,match=code):
            await service.enqueue(p,rid,'request')
        assert archive.pending()==[]
    finally:
        await database.aclose()


def test_retirement_removes_cached_ciphertext_but_keeps_identity(setup):
    from a2a_mq.secure_transport import SealedCache
    store,ledger,p,_=setup
    _,task=finish(setup)
    command=ledger.command(task['id'])
    SealedCache(store).remember(command,'workers',b'ciphertext-fixture')
    Operations(ledger).retire(p,dry_run=False)
    with store._connection() as db:
        assert db.execute('SELECT COUNT(*) FROM a2amq_sealed').fetchone()[0]==0
        assert db.execute('SELECT COUNT(*) FROM a2amq_retired').fetchone()[0]==1


async def test_replay_queue_tampering_cannot_replace_original(setup):
    store,_,p,_=setup
    archive=DeadLetters(store)
    record=archive.capture(dlq_event())
    database=AsyncSQLite(store)
    bus=Bus()
    try:
        service=ReplayService(archive,database,bus,allowed_routes=frozenset({'a2a.v1.demo.command.skill.echo'}))
        await service.enqueue(p,record,'request')
        substituted=canonical(cloud_event(command('different-valid-command')))
        with store._transaction() as db:
            db.execute('UPDATE a2amq_delivery_replays SET wire=?',(substituted,))
        with pytest.raises(RetryableError,match='STORAGE_INTEGRITY_ERROR'):
            await service.flush_once()
        assert not bus.sent
    finally:
        await database.aclose()


def test_admission_repair_rearms_only_unstarted_confirmed_work(setup):
    from a2a_mq.recovery import AdmissionRepair
    store,ledger,p,clock=setup
    task=ledger.submit(p,{'message':{'messageId':'r','role':'ROLE_USER','parts':[{'text':'text'}]}})
    action=ledger.pending(('publish',))[0]
    ledger.delivered(action)
    assert AdmissionRepair(ledger).rearm()==[]
    clock[0]+=301
    assert AdmissionRepair(ledger).rearm()==[action.action_id]
    retried=ledger.pending(('publish',))[0]
    assert retried==action
    ledger.delivered(retried)
    with store._transaction() as db:
        db.execute('UPDATE a2amq_jobs SET started=1 WHERE task_id=?',(task['id'],))
    clock[0]+=301
    assert AdmissionRepair(ledger).rearm()==[]


def test_history_purge_requires_retirement_and_verified_runtime_identity(setup):
    from types import SimpleNamespace
    from a2a_mq.recovery import purge_history
    from a2a_mq.workflows import NAME
    store,ledger,p,_=setup
    _,task=finish(setup)
    command=ledger.command(task['id'])
    purged=[]
    state=SimpleNamespace(name=NAME,serialized_input=canonical({'taskId':task['id'],'digest':command.digest}),
                          runtime_status=SimpleNamespace(name='COMPLETED'))
    client=SimpleNamespace(get_workflow_state=lambda *a,**kw:state,
                           purge_workflow=lambda *a,**kw:purged.append((a,kw)))
    with pytest.raises(PermanentError,match='TASK_NOT_RETIRED'):
        purge_history(ledger,p,task['id'],client)
    Operations(ledger).retire(p,dry_run=False)
    assert purge_history(ledger,p,task['id'],client)['tombstoneRetained']
    assert purged[0][1]=={'recursive':False}
    state.name='not-our-workflow'
    with pytest.raises(PermanentError,match='WORKFLOW_INPUT_CONFLICT'):
        purge_history(ledger,p,task['id'],client)


def test_optional_schema_uses_backend_global_schema_lock(setup):
    from contextlib import contextmanager
    from a2a_mq.schema_lifecycle import schema_transaction
    store=setup[0]
    entered=[]
    @contextmanager
    def backend():
        entered.append(True)
        with store._transaction() as db:
            yield db
    store.schema_transaction=backend
    with schema_transaction(store) as db:
        assert db.execute('SELECT 1').fetchone()[0]==1
    assert entered==[True]


async def test_encrypted_replay_policy_rejects_plaintext_downgrade(setup):
    store,_,p,_=setup
    archive=DeadLetters(store)
    rid=archive.capture(dlq_event())
    database=AsyncSQLite(store)
    async def decode(ce):
        pytest.fail('plaintext cannot be accepted through the encryption decoder')
    service=ReplayService(archive,database,Bus(),allowed_routes=frozenset({'a2a.v1.demo.command.skill.echo'}),decoder=decode)
    try:
        with pytest.raises(PermanentError,match='PLAINTEXT_REPLAY_DISABLED'):
            await service.enqueue(p,rid,'downgrade')
        assert archive.pending()==[]
    finally:
        await database.aclose()
