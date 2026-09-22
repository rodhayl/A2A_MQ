"""Live shared-store gate; TLS is mandatory except for the explicit disposable CI fixture."""
import os
from concurrent.futures import ThreadPoolExecutor
from uuid import uuid4

import pytest

pytestmark=pytest.mark.postgres


def test_real_postgres_parallel_admission_control_and_projection():
    from a2a_mq.configuration import read_file
    from a2a_mq.postgres import PostgresStore
    from a2a_mq.identity import Principal
    from a2a_mq.tasks import TaskLedger
    from a2a_mq.maintenance import Operations
    from a2a_mq.deadletters import DeadLetters
    path=os.environ.get('A2AMQ_POSTGRES_DSN_FILE')
    assert path, 'A2AMQ_POSTGRES_DSN_FILE is required; missing infrastructure is not a skip'
    dsn=read_file(path,secret=True).strip()
    allow_insecure=os.environ.get('A2AMQ_POSTGRES_TEST_ALLOW_INSECURE') == '1'
    tenant='acceptance-'+uuid4().hex[:12]
    stores=[]
    def open_one(i):
        s=PostgresStore(dsn,tenant=tenant,consumer='tasks' if i<4 else 'observer-'+str(i),
                        allow_insecure=allow_insecure)
        stores.append(s)
        ledger=TaskLedger(s)
        Operations(ledger)
        DeadLetters(s)
        return ledger
    try:
        with ThreadPoolExecutor(max_workers=8) as pool:
            ledgers=list(pool.map(open_one,range(8)))
        p=Principal('acceptance',tenant,frozenset({'a2a:admin'}))
        request={'message':{'messageId':'one-request','role':'ROLE_USER','parts':[{'text':'pg-parity'}]}}
        with ThreadPoolExecutor(max_workers=4) as pool:
            tasks=list(pool.map(lambda item:item.submit(p,request),ledgers[:4]))
        assert len({t['id'] for t in tasks})==1
        t=tasks[0]
        ledger=ledgers[0]
        ledger.step(t['id'],'working','WORKING')
        ledger.step(t['id'],'completed','COMPLETED',artifacts=[{'artifactId':'out','parts':[{'text':'ok'}]}])
        events=ledger.store.pending_outbox()
        for observer in ledgers[4:]:
            for item in reversed(events):
                observer.store.apply_snapshot(item.envelope)
            assert observer.store.get_task(t['id'])['status']['state']=='TASK_STATE_COMPLETED'
        with ledger.store._transaction() as db:
            assert db.execute('SELECT COUNT(*) FROM a2amq_jobs WHERE tenant=? AND consumer=?',ledger.keys).fetchone()[0]==1
        reopened=PostgresStore(dsn,tenant=tenant,consumer='tasks',allow_insecure=allow_insecure)
        stores.append(reopened)
        assert reopened.get_task(t['id'])['status']['state']=='TASK_STATE_COMPLETED'
    finally:
        for store in stores:
            try:
                with store._transaction() as db:
                    for table in ('a2amq_delivery_replays','a2amq_delivery_dlq','a2amq_sealed','a2amq_actions','a2amq_steps',
                                  'a2amq_jobs','a2amq_runtime_end','a2amq_retired','a2amq_inbox','a2amq_effects',
                                  'a2amq_outbox','a2amq_tasks'):
                        db.execute('DELETE FROM '+table+' WHERE tenant=? AND consumer=?',store._keys)
            finally:
                store.close()
