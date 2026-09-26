"""Scoped operational queries and transactional retirement of workflow task data.

Retirement keeps identity tombstones. It never makes an accepted command eligible
for execution again and never removes unfinished work or unpublished results.
"""
from __future__ import annotations

from .domain import Envelope, RetryableError, require
from .identity import Principal
from .storage import _page

TERMINALS = "('COMPLETED','FAILED','CANCELED','REJECTED')"


class Operations:
    def __init__(self, ledger):
        self.ledger, self.store = ledger, ledger.store
        from .secure_transport import SealedCache
        SealedCache(self.store)

    def backlog(self, principal: Principal) -> dict:
        principal.authorize("operate", self.store.scope.tenant)
        keys = self.store._keys
        with self.store._connection() as db:
            def count(table, condition="1=1"):
                return db.execute("SELECT COUNT(*) FROM " + table
                    + " WHERE tenant=? AND consumer=? AND " + condition, keys).fetchone()[0]
            oldest = db.execute("SELECT MIN(created_at) FROM a2amq_outbox WHERE tenant=? "
                                "AND consumer=? AND published_at IS NULL", keys).fetchone()[0]
            return {"activeTasks": count("a2amq_jobs", "task_id IN (SELECT task_id FROM a2amq_tasks "
                        "WHERE tenant=a2amq_jobs.tenant AND consumer=a2amq_jobs.consumer AND state NOT IN "
                        + TERMINALS + ")"),
                    "taskRecords": count("a2amq_jobs"), "retiredIdentities": count("a2amq_retired"),
                    "pendingPublications": count("a2amq_outbox", "published_at IS NULL"),
                    "pendingActions": count("a2amq_actions", "delivered_at IS NULL"),
                    "oldestPublicationAgeSeconds": 0 if oldest is None else max(0, self.store._now()-oldest)}

    def retire(self, principal: Principal, *, retention_seconds: int = 604800,
               limit: int = 50, dry_run: bool = True) -> dict:
        principal.authorize("operate", self.store.scope.tenant)
        require(type(retention_seconds) is int and 3600 <= retention_seconds <= 315360000,
                "INVALID_RETENTION")
        require(type(dry_run) is bool, "INVALID_DRY_RUN")
        _page(limit)
        keys, eligible = self.store._keys, []
        with self.store._transaction() as db:
            now = self.store._now()
            # Dapr has finished, SQL has a terminal result, and every action was
            # accepted. A paused or merely SQL-terminal instance is insufficient.
            rows = db.execute("SELECT j.* FROM a2amq_jobs j JOIN a2amq_tasks t ON "
                "j.tenant=t.tenant AND j.consumer=t.consumer AND j.task_id=t.task_id "
                "JOIN a2amq_runtime_end e ON j.tenant=e.tenant AND j.consumer=e.consumer "
                "AND j.task_id=e.task_id WHERE j.tenant=? AND j.consumer=? AND j.started=1 AND j.updated_at<=? "
                "AND e.finished_at<=? AND t.state IN " + TERMINALS + " AND NOT EXISTS "
                "(SELECT 1 FROM a2amq_actions a WHERE a.tenant=j.tenant AND a.consumer=j.consumer "
                "AND a.task_id=j.task_id AND a.delivered_at IS NULL) ORDER BY j.updated_at,j.task_id LIMIT ?",
                (*keys, now-retention_seconds, now-retention_seconds, limit)).fetchall()
            for job in rows:
                steps = db.execute("SELECT event_wire FROM a2amq_steps WHERE tenant=? AND consumer=? "
                                   "AND task_id=?", (*keys, job["task_id"])).fetchall()
                require(bool(steps), "MISSING_TASK_STEPS")
                events, pending = [], False
                for step in steps:
                    event = Envelope(step[0])
                    require(event.as_dict()["taskId"] == job["task_id"]
                            and event.as_dict()["source"] == self.ledger.source, "TASK_OWNER_CONFLICT")
                    eid = event.as_dict()["messageId"]
                    out = db.execute("SELECT wire,digest,published_at FROM a2amq_outbox "
                                     "WHERE tenant=? AND consumer=? AND event_id=?", (*keys, eid)).fetchone()
                    if out is None or out["wire"] != event.wire or out["digest"] != event.digest:
                        raise RetryableError("STORAGE_INTEGRITY_ERROR")
                    pending |= out["published_at"] is None
                    events.append(eid)
                if pending:
                    continue
                eligible.append(job["task_id"])
                if dry_run:
                    continue
                command = Envelope(job["wire"])
                db.execute("INSERT INTO a2amq_retired VALUES (?,?,?,?,?,?,?,?)",
                           (*keys, job["task_id"], job["subject"], job["request_id"], job["digest"], command.digest, now))
                for eid in events + [command.as_dict()["messageId"]]:
                    db.execute("DELETE FROM a2amq_sealed WHERE tenant=? AND consumer=? AND event_id=?", (*keys, eid))
                for eid in events:
                    db.execute("DELETE FROM a2amq_outbox WHERE tenant=? AND consumer=? AND event_id=?",
                               (*keys, eid))
                for table in ("a2amq_steps", "a2amq_actions", "a2amq_runtime_end", "a2amq_tasks", "a2amq_jobs"):
                    db.execute("DELETE FROM " + table + " WHERE tenant=? AND consumer=? AND task_id=?",
                               (*keys, job["task_id"]))
        return {"dryRun": dry_run, "taskIds": eligible, "retired": 0 if dry_run else len(eligible),
                "identityTombstonesRetained": True}
