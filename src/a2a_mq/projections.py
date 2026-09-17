"""Bounded observer projections with compact identity-preserving retirement.

This policy applies only to observer scopes. Worker-owned task outcomes use the
stricter coordinated SQL/Dapr/outbox retirement in operations.py. Tombstones count
against quotas: expiration does not silently reset the idempotency boundary.
"""
from __future__ import annotations

from datetime import datetime, timezone

from .domain import TERMINAL, require, RetryableError
from .schema_lifecycle import schema_transaction
from .storage import _page
from .tasks import digest

DDL = (
    "CREATE TABLE IF NOT EXISTS a2amq_projection_events (tenant TEXT NOT NULL,consumer TEXT NOT NULL,"
    "message_id TEXT NOT NULL,task_id TEXT NOT NULL,digest TEXT NOT NULL,retired INTEGER NOT NULL,"
    "PRIMARY KEY(tenant,consumer,message_id))",
    "CREATE INDEX IF NOT EXISTS a2amq_projection_event_task ON a2amq_projection_events(tenant,consumer,task_id)",
    "CREATE TABLE IF NOT EXISTS a2amq_projection_clock (tenant TEXT NOT NULL,consumer TEXT NOT NULL,"
    "task_id TEXT NOT NULL,observed_at DOUBLE PRECISION NOT NULL,PRIMARY KEY(tenant,consumer,task_id))",
    "CREATE TABLE IF NOT EXISTS a2amq_projection_retired (tenant TEXT NOT NULL,consumer TEXT NOT NULL,"
    "task_id TEXT NOT NULL,source TEXT NOT NULL,context_id TEXT NOT NULL,sequence BIGINT NOT NULL,"
    "payload_digest TEXT NOT NULL,retired_at DOUBLE PRECISION NOT NULL,PRIMARY KEY(tenant,consumer,task_id))",
)


class ProjectionPolicy:
    def __init__(self, store, *, max_records=1000000):
        require(type(max_records) is int and 1 <= max_records <= 10000000, "INVALID_QUOTA")
        self.store, self.max_records = store, max_records
        with schema_transaction(store) as db:
            for sql in DDL:
                db.execute(sql)

    def apply(self, event):
        e = self.store._envelope(event, "task.snapshot")
        require(type(e['sequence']) is int, "INVALID_SEQUENCE")
        keys = self.store._keys
        with self.store._transaction() as db:
            prior = db.execute("SELECT digest,task_id,retired FROM a2amq_projection_events WHERE tenant=? AND consumer=? AND message_id=?",
                               (*keys,e['messageId'])).fetchone()
            if prior:
                require(prior[0] == event.digest and prior[1] == e['taskId'], "MESSAGE_ID_CONFLICT")
                if prior[2]:
                    evidence = db.execute("SELECT source,context_id,sequence FROM a2amq_projection_retired "
                                          "WHERE tenant=? AND consumer=? AND task_id=?",(*keys,e['taskId'])).fetchone()
                else:
                    evidence = db.execute("SELECT source,context_id,sequence FROM a2amq_tasks "
                                          "WHERE tenant=? AND consumer=? AND task_id=?",(*keys,e['taskId'])).fetchone()
                    receipt = db.execute("SELECT digest,wire,status FROM a2amq_inbox WHERE tenant=? AND consumer=? AND message_id=?",
                                         (*keys,e['messageId'])).fetchone()
                    if receipt is None or tuple(receipt)!=(event.digest,event.wire,'done'):
                        raise RetryableError("PROJECTION_INTEGRITY_ERROR")
                if evidence is None or evidence[0]!=e['source'] or evidence[1]!=e['contextId'] or evidence[2]<e['sequence']:
                    raise RetryableError("PROJECTION_INTEGRITY_ERROR")
                return False
            legacy = db.execute("SELECT digest,wire,status FROM a2amq_inbox WHERE tenant=? AND consumer=? AND message_id=?",
                                (*keys,e['messageId'])).fetchone()
            if legacy:
                require(legacy[0] == event.digest and legacy[1] == event.wire and legacy[2] == 'done', "MESSAGE_ID_CONFLICT")
                return False
            count = db.execute("SELECT COUNT(*) FROM a2amq_inbox WHERE tenant=? AND consumer=?", keys).fetchone()[0]
            count += db.execute("SELECT COUNT(*) FROM a2amq_projection_events WHERE tenant=? AND consumer=? AND retired=1",keys).fetchone()[0]
            if count >= self.max_records:
                raise RetryableError('PROJECTION_QUOTA_EXCEEDED')
            retired = db.execute("SELECT * FROM a2amq_projection_retired WHERE tenant=? AND consumer=? AND task_id=?",
                                  (*keys,e['taskId'])).fetchone()
            event.check_expiry(datetime.fromtimestamp(self.store._now(),timezone.utc))
            if retired:
                require(retired['source']==e['source'],'TASK_OWNER_CONFLICT')
                require(retired['context_id']==e['contextId'],'TASK_CONTEXT_CONFLICT')
                require(e['sequence']<=retired['sequence'],'TERMINAL_TASK_REWRITE')
                if e['sequence']==retired['sequence']:
                    require(retired['payload_digest']==digest(e['payload']),'TASK_VERSION_CONFLICT')
                changed = False
            else:
                changed = self.store._project(db,event)
                db.execute("INSERT INTO a2amq_inbox (tenant,consumer,message_id,digest,wire,status,fence) VALUES (?,?,?,?,?,'done',1)",
                           (*keys,e['messageId'],event.digest,event.wire))
                if changed:
                    db.execute("INSERT INTO a2amq_projection_clock VALUES (?,?,?,?) ON CONFLICT(tenant,consumer,task_id) "
                               "DO UPDATE SET observed_at=excluded.observed_at",(*keys,e['taskId'],self.store._now()))
            db.execute("INSERT INTO a2amq_projection_events VALUES (?,?,?,?,?,?)",
                       (*keys,e['messageId'],e['taskId'],event.digest,1 if retired else 0))
            return changed

    def retire(self, *, retention_seconds=604800, limit=50, dry_run=True):
        require(type(retention_seconds) is int and 3600<=retention_seconds<=315360000,'INVALID_RETENTION')
        require(type(dry_run) is bool,'INVALID_DRY_RUN')
        _page(limit)
        keys, retired = self.store._keys, []
        with self.store._transaction() as db:
            rows=db.execute("SELECT t.* FROM a2amq_tasks t JOIN a2amq_projection_clock c ON "
                "t.tenant=c.tenant AND t.consumer=c.consumer AND t.task_id=c.task_id WHERE t.tenant=? AND t.consumer=? "
                "AND c.observed_at<=? AND t.state IN ('COMPLETED','FAILED','CANCELED','REJECTED') "
                "ORDER BY c.observed_at,t.task_id LIMIT ?",(*keys,self.store._now()-retention_seconds,limit)).fetchall()
            for row in rows:
                require(row['state'] in TERMINAL,'NONTERMINAL_RETIREMENT')
                task_id=row['task_id']
                from .domain import load_json, state_of
                payload=load_json(row['payload_json'])
                require(payload['id']==task_id and payload['contextId']==row['context_id']
                        and state_of(payload)==row['state'],'PROJECTION_INTEGRITY_ERROR')
                if not dry_run:
                    db.execute("INSERT INTO a2amq_projection_retired VALUES (?,?,?,?,?,?,?,?)",
                        (*keys,task_id,row['source'],row['context_id'],row['sequence'],digest(payload),self.store._now()))
                    db.execute("DELETE FROM a2amq_inbox WHERE tenant=? AND consumer=? AND message_id IN "
                        "(SELECT message_id FROM a2amq_projection_events WHERE tenant=? AND consumer=? AND task_id=?)",
                        (*keys,*keys,task_id))
                    db.execute("UPDATE a2amq_projection_events SET retired=1 WHERE tenant=? AND consumer=? AND task_id=?",(*keys,task_id))
                    db.execute("DELETE FROM a2amq_projection_clock WHERE tenant=? AND consumer=? AND task_id=?",(*keys,task_id))
                    db.execute("DELETE FROM a2amq_tasks WHERE tenant=? AND consumer=? AND task_id=?",(*keys,task_id))
                retired.append(task_id)
        return {'dryRun':dry_run,'taskIds':retired,'retired':0 if dry_run else len(retired),'tombstonesRetained':True}
