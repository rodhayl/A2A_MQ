"""Transactional task admission/control ledger shared by the A2A bridge and Workflow.

This is SQL bookkeeping, not a workflow engine. Dapr owns orchestration history,
external-event waits and activity retries. SQLite and PostgreSQL share this SQL.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
from typing import Any
from uuid import NAMESPACE_URL, uuid5

from .schema_lifecycle import schema_transaction

from .domain import (MAX_BYTES, STATES, TERMINAL, Envelope, PermanentError, RetryableError,
                     can_transition, canonical, load_json, require, state_of, token)
from .identity import Principal
from .storage import _key, _page
from .workflow_options import options, text_parts

DDL = (
    "CREATE TABLE IF NOT EXISTS a2amq_retired (tenant TEXT NOT NULL,consumer TEXT NOT NULL,"
    "task_id TEXT NOT NULL,subject TEXT NOT NULL,request_id TEXT NOT NULL,digest TEXT NOT NULL,"
    "command_digest TEXT NOT NULL,retired_at DOUBLE PRECISION NOT NULL,"
    "PRIMARY KEY(tenant,consumer,task_id),UNIQUE(tenant,consumer,subject,request_id))",
    "CREATE TABLE IF NOT EXISTS a2amq_runtime_end (tenant TEXT NOT NULL,consumer TEXT NOT NULL,"
    "task_id TEXT NOT NULL,finished_at DOUBLE PRECISION NOT NULL,PRIMARY KEY(tenant,consumer,task_id))",
    "CREATE TABLE IF NOT EXISTS a2amq_jobs (tenant TEXT NOT NULL,consumer TEXT NOT NULL,"
    "task_id TEXT NOT NULL,subject TEXT NOT NULL,request_id TEXT NOT NULL,digest TEXT NOT NULL,"
    "wire TEXT NOT NULL,created_at DOUBLE PRECISION NOT NULL,updated_at DOUBLE PRECISION NOT NULL,"
    "started INTEGER NOT NULL DEFAULT 0,PRIMARY KEY(tenant,consumer,task_id),"
    "UNIQUE(tenant,consumer,subject,request_id))",
    "CREATE TABLE IF NOT EXISTS a2amq_actions (tenant TEXT NOT NULL,consumer TEXT NOT NULL,"
    "action_id TEXT NOT NULL,task_id TEXT NOT NULL,kind TEXT NOT NULL,digest TEXT NOT NULL,"
    "body TEXT NOT NULL,created_at DOUBLE PRECISION NOT NULL,delivered_at DOUBLE PRECISION,"
    "PRIMARY KEY(tenant,consumer,action_id))",
    "CREATE INDEX IF NOT EXISTS a2amq_actions_pending ON a2amq_actions(tenant,consumer,delivered_at,action_id)",
    "CREATE TABLE IF NOT EXISTS a2amq_steps (tenant TEXT NOT NULL,consumer TEXT NOT NULL,"
    "task_id TEXT NOT NULL,step_id TEXT NOT NULL,digest TEXT NOT NULL,event_wire TEXT NOT NULL,"
    "PRIMARY KEY(tenant,consumer,task_id,step_id))",
)


def stable_id(*values: str) -> str:
    return str(uuid5(NAMESPACE_URL, canonical(list(values))))


def digest(value: Any) -> str:
    return hashlib.sha256(canonical(value).encode()).hexdigest()


@dataclass(frozen=True)
class Action:
    action_id: str
    task_id: str
    kind: str
    digest: str
    body: str


class TaskLedger:
    def __init__(self, store, *, source: str = "workflow-worker", max_active: int = 1000, max_records: int = 100000):
        self.store, self.source = store, token(source)
        require(type(max_active) is int and 1 <= max_active <= 1000000, "INVALID_QUOTA")
        require(type(max_records) is int and max_active <= max_records <= 10000000, "INVALID_QUOTA")
        self.max_active, self.max_records = max_active, max_records
        with schema_transaction(store) as db:
            for sql in DDL:
                db.execute(sql)

    @property
    def keys(self):
        return self.store._keys

    def _job(self, db, task_id):
        row = db.execute("SELECT * FROM a2amq_jobs WHERE tenant=? AND consumer=? AND task_id=?",
                         (*self.keys, _key(task_id))).fetchone()
        require(row is not None, "TASK_NOT_FOUND")
        return row

    def _task(self, db, task_id):
        row = db.execute("SELECT payload_json FROM a2amq_tasks WHERE tenant=? AND consumer=? AND task_id=?",
                         (*self.keys, task_id)).fetchone()
        if row is None:
            raise RetryableError("STORAGE_INTEGRITY_ERROR")
        return load_json(row[0])

    def _action(self, db, task_id, kind, request_id, body):
        aid = stable_id(*self.keys, task_id, kind, request_id)
        wire, hashed = canonical(body), digest(body)
        load_json(wire, MAX_BYTES)
        row = db.execute("SELECT digest,body FROM a2amq_actions WHERE tenant=? AND consumer=? AND action_id=?",
                         (*self.keys, aid)).fetchone()
        if row is not None:
            require((row["digest"], row["body"]) == (hashed, wire), "CONTROL_ID_CONFLICT")
        else:
            db.execute("INSERT INTO a2amq_actions "
                       "(tenant,consumer,action_id,task_id,kind,digest,body,created_at) VALUES (?,?,?,?,?,?,?,?)",
                       (*self.keys, aid, task_id, kind, hashed, wire, self.store._now()))
        return aid

    def _snapshot(self, db, request: Envelope, step_id: str, state: str, *, artifacts=None, message=None):
        require(state in STATES, "INVALID_STATE")
        _key(step_id)
        e = request.as_dict()
        task_id = e["taskId"]
        semantics = {"state": state, "artifacts": artifacts, "message": message}
        previous = db.execute("SELECT digest,event_wire FROM a2amq_steps "
                              "WHERE tenant=? AND consumer=? AND task_id=? AND step_id=?",
                              (*self.keys, task_id, step_id)).fetchone()
        if previous is not None:
            require(previous["digest"] == digest(semantics), "WORKFLOW_STEP_CONFLICT")
            return Envelope(previous["event_wire"]).as_dict()["payload"]
        current = db.execute("SELECT state,sequence,payload_json FROM a2amq_tasks "
                             "WHERE tenant=? AND consumer=? AND task_id=?", (*self.keys, task_id)).fetchone()
        if db.execute("SELECT COUNT(*) FROM a2amq_steps WHERE tenant=? AND consumer=? AND task_id=?",
                      (*self.keys, task_id)).fetchone()[0] >= 32:
            raise PermanentError("TASK_STEP_LIMIT")
        if current:
            require(current["state"] not in TERMINAL, "TERMINAL_TASK_REWRITE")
            require(can_transition(current["state"], state), "INVALID_TASK_TRANSITION")
            task = load_json(current["payload_json"])
            sequence = current["sequence"] + 1
        else:
            require(state == "SUBMITTED", "INVALID_TASK_TRANSITION")
            sequence = 1
            task = {"id": task_id, "contextId": e["contextId"], "history": [e["payload"]["message"]]}
        if message is not None:
            text_parts(message)
            require(message.get("taskId") == task_id and message.get("contextId",e["contextId"]) == e["contextId"],
                    "IDENTIFIER_MISMATCH")
            task["history"] = (task.get("history", []) + [message])[-32:]
        now = self.store._now()
        stamp = datetime.fromtimestamp(now, timezone.utc).isoformat().replace("+00:00", "Z")
        task["status"] = {"state": "TASK_STATE_" + state, "timestamp": stamp}
        if state == "AUTH_REQUIRED":
            task["status"]["message"] = {
                "messageId": stable_id(task_id, step_id, "approval-instructions"),
                "role": "ROLE_AGENT", "taskId": task_id, "contextId": e["contextId"],
                "parts": [{"text": "Approval required. Submit authenticated POST "
                           + f"/operations/tasks/{task_id}/approve with requestId and approved. "
                           + "The caller requires a2a:approve permission for this task."}],
            }
        if artifacts is not None:
            task["artifacts"] = artifacts
        out = Envelope.from_dict({"envelopeVersion": "1.0", "a2aVersion": "1.0", "tenantId": e["tenantId"],
                                  "messageId": stable_id(*self.keys, task_id, "step", step_id),
                                  "kind": "event", "operation": "task.snapshot", "source": self.source,
                                  "correlationId": e["correlationId"], "causationId": e["messageId"],
                                  "taskId": task_id, "contextId": e["contextId"], "sequence": sequence,
                                  "createdAt": stamp, "payload": task,
                                  **{k: e[k] for k in ("traceparent", "tracestate") if k in e}})
        self.store._project(db, out)
        db.execute("INSERT INTO a2amq_steps VALUES (?,?,?,?,?,?)",
                   (*self.keys, task_id, step_id, digest(semantics), out.wire))
        db.execute("INSERT INTO a2amq_outbox "
                   "(tenant,consumer,event_id,digest,wire,created_at) VALUES (?,?,?,?,?,?)",
                   (*self.keys, out.as_dict()["messageId"], out.digest, out.wire, now))
        db.execute("UPDATE a2amq_jobs SET updated_at=? WHERE tenant=? AND consumer=? AND task_id=?",
                   (now, *self.keys, task_id))
        return task

    def submit(self, principal: Principal, request: dict[str, Any]) -> dict[str, Any]:
        principal.authorize("send", self.store.scope.tenant)
        request = load_json(canonical(request))
        require(isinstance(request, dict) and set(request) <= {"message", "configuration", "metadata"},
                "INVALID_REQUEST")
        message = request.get("message")
        require(isinstance(message, dict), "INVALID_PAYLOAD")
        assert isinstance(message, dict)
        rid = _key(message.get("messageId"))
        if message.get("taskId"):
            return self.control(principal, message["taskId"], "input", rid, message)
        options(request)
        semantic = digest(request)
        task_id = stable_id(*self.keys, principal.subject, rid)
        with self.store._transaction() as db:
            old = db.execute("SELECT task_id,digest FROM a2amq_jobs WHERE tenant=? AND consumer=? "
                             "AND subject=? AND request_id=?", (*self.keys, principal.subject, rid)).fetchone()
            if old:
                require(old["digest"] == semantic, "MESSAGE_ID_CONFLICT")
                return self._task(db, old["task_id"])
            retired = db.execute("SELECT digest FROM a2amq_retired WHERE tenant=? AND consumer=? "
                                 "AND subject=? AND request_id=?", (*self.keys, principal.subject, rid)).fetchone()
            if retired:
                require(retired["digest"] == semantic, "MESSAGE_ID_CONFLICT")
                raise PermanentError("TASK_RETIRED")
            total = sum(db.execute("SELECT COUNT(*) FROM " + name + " WHERE tenant=? AND consumer=?",
                                   self.keys).fetchone()[0] for name in ("a2amq_jobs", "a2amq_retired"))
            if total >= self.max_records:
                raise RetryableError("ADMISSION_RECORD_QUOTA_EXCEEDED")
            count = db.execute("SELECT COUNT(*) FROM a2amq_jobs j JOIN a2amq_tasks t "
                               "ON j.tenant=t.tenant AND j.consumer=t.consumer AND j.task_id=t.task_id "
                               "WHERE j.tenant=? AND j.consumer=? AND t.state NOT IN ('COMPLETED','FAILED','CANCELED','REJECTED')",
                               self.keys).fetchone()[0]
            if count >= self.max_active:
                raise RetryableError("ADMISSION_QUOTA_EXCEEDED")
            context_id = message.get("contextId") or stable_id(task_id, "context")
            message.update(taskId=task_id, contextId=context_id)
            now = self.store._now()
            envelope = Envelope.from_dict({"envelopeVersion": "1.0", "a2aVersion": "1.0",
                "tenantId": self.store.scope.tenant, "messageId": stable_id(task_id, "command"),
                "kind": "command", "operation": "message.send", "source": "a2a-bridge",
                "target": {"agentId": self.source}, "correlationId": task_id,
                "taskId": task_id, "contextId": context_id,
                "createdAt": datetime.fromtimestamp(now, timezone.utc).isoformat().replace("+00:00", "Z"),
                "payload": request})
            db.execute("INSERT INTO a2amq_jobs "
                       "(tenant,consumer,task_id,subject,request_id,digest,wire,created_at,updated_at) "
                       "VALUES (?,?,?,?,?,?,?,?,?)", (*self.keys, task_id, principal.subject, rid,
                                                     semantic, envelope.wire, now, now))
            result = self._snapshot(db, envelope, "submitted", "SUBMITTED")
            self._action(db, task_id, "publish", rid, envelope.as_dict())
            return result

    def get(self, principal: Principal, task_id: str) -> dict[str, Any]:
        principal.authorize("read", self.store.scope.tenant)
        with self.store._connection() as db:
            job = self._job(db, task_id)
            principal.authorize("read", self.store.scope.tenant, job["subject"])
            return self._task(db, task_id)

    def list(self, principal: Principal, *, limit=50, after=None, context_id=None, state=None):
        principal.authorize("read", self.store.scope.tenant)
        _page(limit)
        clauses, params = ["j.tenant=?", "j.consumer=?"], list(self.keys)
        if "a2a:admin" not in principal.scopes:
            clauses.append("j.subject=?")
            params.append(principal.subject)
        for column, value in (("j.task_id", after), ("t.context_id", context_id), ("t.state", state)):
            if value is not None:
                _key(value)
                if column == "t.state":
                    require(value in STATES, "INVALID_STATE")
                clauses.append(column + (">?" if column == "j.task_id" else "=?"))
                params.append(value)
        with self.store._connection() as db:
            rows = db.execute("SELECT j.task_id,t.payload_json FROM a2amq_jobs j JOIN a2amq_tasks t "
                              "ON j.tenant=t.tenant AND j.consumer=t.consumer AND j.task_id=t.task_id WHERE "
                              + " AND ".join(clauses) + " ORDER BY j.task_id LIMIT ?", (*params, limit+1)).fetchall()
        return [load_json(r["payload_json"]) for r in rows[:limit]], rows[limit-1]["task_id"] if len(rows)>limit else None

    def control(self, principal: Principal, task_id: str, kind: str, request_id: str, data=None):
        require(kind in {"input", "auth", "cancel"}, "INVALID_CONTROL")
        principal.authorize({"input":"send", "auth":"approve", "cancel":"cancel"}[kind], self.store.scope.tenant)
        _key(request_id)
        with self.store._transaction() as db:
            job = self._job(db, task_id)
            principal.authorize({"input":"send", "auth":"approve", "cancel":"cancel"}[kind],
                                self.store.scope.tenant, job["subject"])
            task = self._task(db, task_id)
            aid = stable_id(*self.keys, task_id, kind, request_id)
            prior = db.execute("SELECT action_id FROM a2amq_actions WHERE tenant=? AND consumer=? AND action_id=?",
                               (*self.keys, aid)).fetchone()
            body = {"requestId": request_id, "kind": kind, "data": data}
            if prior is None:
                queued = db.execute("SELECT action_id FROM a2amq_actions WHERE tenant=? AND consumer=? "
                                    "AND task_id=? AND kind=? LIMIT 1", (*self.keys, task_id, kind)).fetchone()
                state = state_of(task)
                require(state not in TERMINAL, "TASK_NOT_CANCELABLE" if kind == "cancel" else "TASK_TERMINAL")
                require(queued is None, "CONTROL_ALREADY_ACCEPTED")
                if kind != "cancel":
                    require(state == ("INPUT_REQUIRED" if kind == "input" else "AUTH_REQUIRED"), "TASK_NOT_WAITING")

                if kind == "input":
                    require(isinstance(data, dict) and data.get("taskId") == task_id
                            and data.get("contextId", task["contextId"]) == task["contextId"], "IDENTIFIER_MISMATCH")
                    probe = Envelope(job["wire"]).as_dict()
                    probe["payload"]["message"] = {**data, "contextId": task["contextId"]}
                    Envelope.from_dict(probe)
                    text_parts(data)
                if kind == "auth":
                    require(type(data) is bool, "INVALID_APPROVAL")
            self._action(db, task_id, kind, request_id, body)
            return task

    def command(self, task_id):
        with self.store._connection() as db:
            return Envelope(self._job(db, task_id)["wire"])

    def accept(self, envelope: Envelope):
        e = envelope.as_dict()
        require(e["tenantId"] == self.store.scope.tenant and envelope.route ==
                f"a2a.v1.{self.store.scope.tenant}.command.agent.{self.source}", "UNAUTHORIZED_ROUTE")
        with self.store._transaction() as db:
            retired = db.execute("SELECT command_digest FROM a2amq_retired "
                                 "WHERE tenant=? AND consumer=? AND task_id=?",
                                 (*self.keys, _key(e.get("taskId")))).fetchone()
            if retired:
                require(retired["command_digest"] == envelope.digest, "MESSAGE_ID_CONFLICT")
                return False
            job = self._job(db, e.get("taskId"))
            require(job["wire"] == envelope.wire, "MESSAGE_ID_CONFLICT")
            self._action(db, job["task_id"], "start", e["messageId"], e)
            return True

    def step(self, task_id, step_id, state, *, artifacts=None, message=None):
        with self.store._transaction() as db:
            return self._snapshot(db, Envelope(self._job(db, task_id)["wire"]), step_id, state, artifacts=artifacts, message=message)

    def pending(self, kinds, *, limit=50, after=None):
        require(isinstance(kinds, tuple) and bool(kinds) and set(kinds) <= {"publish","start","input","auth","cancel"},
                "INVALID_CONTROL")
        _page(limit)
        params = [*self.keys, *kinds]
        extra = ""
        if after is not None:
            extra = " AND action_id>?"
            params.append(_key(after))
        with self.store._connection() as db:
            rows = db.execute("SELECT * FROM a2amq_actions WHERE tenant=? AND consumer=? "
                              "AND delivered_at IS NULL AND kind IN (" + ",".join("?" for _ in kinds) + ")"
                              + extra + " ORDER BY action_id LIMIT ?", (*params, limit)).fetchall()
        for row in rows:
            if digest(load_json(row["body"])) != row["digest"]:
                raise RetryableError("STORAGE_INTEGRITY_ERROR")
        return [Action(r["action_id"],r["task_id"],r["kind"],r["digest"],r["body"]) for r in rows]

    def delivered(self, action: Action):
        with self.store._transaction() as db:
            row = db.execute("SELECT * FROM a2amq_actions WHERE tenant=? AND consumer=? AND action_id=?",
                             (*self.keys, action.action_id)).fetchone()
            require(row is not None and (row["task_id"],row["kind"],row["digest"],row["body"]) ==
                    (action.task_id,action.kind,action.digest,action.body), "CONTROL_ID_CONFLICT")
            db.execute("UPDATE a2amq_actions SET delivered_at=COALESCE(delivered_at,?) "
                       "WHERE tenant=? AND consumer=? AND action_id=?",
                       (self.store._now(),*self.keys,action.action_id))
            if action.kind == "start":
                db.execute("UPDATE a2amq_jobs SET started=1 WHERE tenant=? AND consumer=? AND task_id=?",
                           (*self.keys, action.task_id))

    def finish(self, task_id: str) -> None:
        """Record terminal runtime completion idempotently for retention/recovery gates."""
        task_id = _key(task_id)
        with self.store._transaction() as db:
            task = self._task(db, task_id)
            require(state_of(task) in TERMINAL, "TASK_NOT_TERMINAL")
            db.execute("INSERT INTO a2amq_runtime_end VALUES (?,?,?,?) "
                       "ON CONFLICT(tenant,consumer,task_id) DO NOTHING",
                       (*self.keys, task_id, self.store._now()))

    def verify_control(self, task_id, kind, body):
        require(isinstance(body,dict) and set(body) == {"requestId","kind","data"}
                and body["kind"] == kind, "INVALID_CONTROL")
        aid = stable_id(*self.keys, _key(task_id), kind, _key(body["requestId"]))
        with self.store._connection() as db:
            row = db.execute("SELECT digest,body FROM a2amq_actions WHERE tenant=? AND consumer=? AND action_id=?",
                             (*self.keys,aid)).fetchone()
            require(row is not None and row["digest"] == digest(body) and row["body"] == canonical(body),
                    "UNAUTHORIZED_CONTROL")
        return body["data"]

    def running(self, *, limit=50, after=None):
        _page(limit)
        extra = " AND j.task_id>?" if after is not None else ""
        values = (*self.keys, _key(after)) if after is not None else self.keys
        with self.store._connection() as db:
            return [r[0] for r in db.execute("SELECT j.task_id FROM a2amq_jobs j JOIN a2amq_tasks t "
                "ON j.tenant=t.tenant AND j.consumer=t.consumer AND j.task_id=t.task_id "
                "WHERE j.tenant=? AND j.consumer=? AND j.started=1 "
                "AND t.state NOT IN ('COMPLETED','FAILED','CANCELED','REJECTED')" + extra +
                " ORDER BY j.task_id LIMIT ?", (*values,limit))]

    def unconfirmed(self, *, limit=50, after=None):
        _page(limit)
        extra = " AND j.task_id>?" if after is not None else ""
        params = (*self.keys, _key(after)) if after is not None else self.keys
        with self.store._connection() as db:
            return [r[0] for r in db.execute(
                "SELECT j.task_id FROM a2amq_jobs j WHERE j.tenant=? AND j.consumer=? AND j.started=1 "
                "AND NOT EXISTS (SELECT 1 FROM a2amq_runtime_end e WHERE e.tenant=j.tenant "
                "AND e.consumer=j.consumer AND e.task_id=j.task_id)" + extra +
                " ORDER BY j.task_id LIMIT ?", (*params, limit))]
