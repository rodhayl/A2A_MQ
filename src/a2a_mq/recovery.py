"""Recover confirmed publications that were never durably admitted by Dapr.

This re-arms an existing immutable SQL publish action after a quiet interval. It
is not a scheduler: only the subscribed Dapr worker can start a workflow. The
same IDs, input bytes and encrypted publication cache are retained.
"""
import asyncio
import logging

from .domain import Envelope, RetryableError, load_json, require
from .storage import _key, _page
from .tasks import digest

LOG = logging.getLogger("a2a_mq.recovery")


class AdmissionRepair:
    def __init__(self, ledger, *, minimum_age=300):
        require(type(minimum_age) is int and 60 <= minimum_age <= 86400, "INVALID_RECOVERY_INTERVAL")
        self.ledger, self.minimum_age = ledger, minimum_age

    def rearm(self, *, after=None, limit=50):
        _page(limit)
        extra = " AND a.action_id>?" if after else ""
        keys = self.ledger.keys
        with self.ledger.store._transaction() as db:
            cutoff = self.ledger.store._now() - self.minimum_age
            params = (*keys, cutoff, _key(after), limit) if after else (*keys, cutoff, limit)
            rows = db.execute("SELECT a.*,j.wire FROM a2amq_actions a JOIN a2amq_jobs j ON "
                "a.tenant=j.tenant AND a.consumer=j.consumer AND a.task_id=j.task_id JOIN a2amq_tasks t ON "
                "t.tenant=j.tenant AND t.consumer=j.consumer AND t.task_id=j.task_id "
                "WHERE a.tenant=? AND a.consumer=? AND a.kind='publish' AND a.delivered_at<=? "
                "AND j.started=0 AND t.state='SUBMITTED'"+extra+" ORDER BY a.action_id LIMIT ?", params).fetchall()
            for row in rows:
                command = Envelope(row["wire"])
                if row["body"] != command.wire or row["digest"] != digest(load_json(row["body"])):
                    raise RetryableError("STORAGE_INTEGRITY_ERROR")
                db.execute("UPDATE a2amq_actions SET delivered_at=NULL WHERE tenant=? AND consumer=? AND action_id=?",
                           (*keys,row["action_id"]))
        return [r["action_id"] for r in rows]

    async def run(self, database, stop, *, interval=60):
        cursor = None
        while not stop.is_set():
            try:
                rows = await database.call(self.rearm, after=cursor)
                cursor = rows[-1] if len(rows) == 50 else None
            except Exception:
                LOG.warning('{"event":"admission_repair_unavailable"}')
            try:
                await asyncio.wait_for(stop.wait(), interval)
            except TimeoutError:
                pass


def purge_history(ledger, principal, task_id, client):
    """Administrator-only purge AFTER SQL retirement; preserve tombstones forever.

Run against the Dapr sidecar of the workflow application, not the API app. Purge
is not part of accepting/retrying a message and never changes a task identity.
"""
    from .workflows import NAME
    principal.authorize("operate", ledger.store.scope.tenant)
    with ledger.store._connection() as db:
        retired = db.execute("SELECT command_digest FROM a2amq_retired WHERE tenant=? AND consumer=? AND task_id=?",
                             (*ledger.keys,_key(task_id))).fetchone()
    require(retired is not None, "TASK_NOT_RETIRED")
    state = client.get_workflow_state(task_id, fetch_payloads=True)
    if state is None:
        return {"taskId": task_id, "runtimeHistory": "absent", "tombstoneRetained": True}
    require(state.name == NAME and load_json(state.serialized_input) == {"taskId": task_id, "digest": retired[0]},
            "WORKFLOW_INPUT_CONFLICT")
    require(state.runtime_status.name in {"COMPLETED", "FAILED", "TERMINATED"}, "WORKFLOW_NOT_FINISHED")
    client.purge_workflow(task_id, recursive=False)
    return {"taskId": task_id, "runtimeHistory": "purged", "tombstoneRetained": True}
