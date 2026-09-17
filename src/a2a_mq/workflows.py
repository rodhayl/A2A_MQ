"""Official Dapr Workflow orchestration and durable SQL dispatch integration.

Reference activities are pure text transforms; no arbitrary external effect is
covered by their idempotency. Dapr owns durable waits, timers, fan-out and replay.
"""
from __future__ import annotations

import asyncio
from datetime import timedelta
import logging

from .domain import Envelope, PermanentError, RetryableError, canonical, load_json, require, state_of, TERMINAL
from .tasks import Action, TaskLedger
from .workflow_options import options, text_parts

LOG = logging.getLogger("a2a_mq.workflow")
NAME = "a2amq-reference-v1"


def make_workflow(sdk):
    # Each runtime receives fresh function objects: SDK registration mutates them.
    def workflow(ctx, admission):
        task_id = admission["taskId"]
        policy = sdk.RetryPolicy(first_retry_interval=timedelta(seconds=1), max_number_of_attempts=5)

        def save(step, state, artifacts=None, message=None):
            return ctx.call_activity("a2amq-save-v1", input={"taskId": task_id, "step": step,
                "state": state, "artifacts": artifacts, "message": message}, retry_policy=policy)

        cancel = ctx.wait_for_external_event("cancel")

        def wait_or_cancel(work):
            winner = yield sdk.when_any([cancel, work])
            if winner == cancel:
                body = yield cancel
                yield ctx.call_activity("a2amq-control-v1", input={"taskId":task_id,"kind":"cancel","body":body},
                                        retry_policy=policy)
                return True, None
            return False, (yield work)

        # Activity identity/digest binds Dapr's durable input to the SQL admission.
        initial = yield ctx.call_activity("a2amq-load-v1", input=admission, retry_policy=policy)
        config, parts = initial["options"], initial["parts"]
        yield save("working", "WORKING")
        try:
            if config["inputRequired"]:
                yield save("input-required", "INPUT_REQUIRED")
                canceled, event = yield from wait_or_cancel(ctx.wait_for_external_event("input"))
                if canceled:
                    return (yield save("canceled", "CANCELED"))
                message = yield ctx.call_activity("a2amq-control-v1", input={"taskId":task_id,"kind":"input","body":event},
                                                  retry_policy=policy)
                parts = text_parts(message)
                yield save("input-received", "WORKING", message=message)
            if config["authRequired"]:
                yield save("auth-required", "AUTH_REQUIRED")
                canceled, event = yield from wait_or_cancel(ctx.wait_for_external_event("auth"))
                if canceled:
                    return (yield save("canceled", "CANCELED"))
                approved = yield ctx.call_activity("a2amq-control-v1", input={"taskId":task_id,"kind":"auth","body":event},
                                                   retry_policy=policy)
                if approved is not True:
                    return (yield save("rejected", "REJECTED"))
                yield save("auth-received", "WORKING")
            if config["delaySeconds"]:
                canceled, _ = yield from wait_or_cancel(ctx.create_timer(timedelta(seconds=config["delaySeconds"])))
                if canceled:
                    return (yield save("canceled", "CANCELED"))
            work = [ctx.call_activity("a2amq-transform-v1", input={"text": part, "transform": config["transform"]},
                                      retry_policy=policy) for part in parts]
            canceled, results = yield from wait_or_cancel(sdk.when_all(work))
            if canceled:
                return (yield save("canceled", "CANCELED"))
            artifacts = [{"artifactId": "result", "parts": [{"text": text} for text in results]}]
            return (yield save("completed", "COMPLETED", artifacts))
        except Exception:
            # A failed required save remains a failed Dapr instance, not a fake SQL success.
            return (yield save("failed", "FAILED"))
    return workflow


def register(runtime, ledger: TaskLedger, sdk=None):
    if sdk is None:
        import dapr.ext.workflow as sdk

    def load(ctx, admission):
        command = ledger.command(admission["taskId"])
        require(admission["digest"] == command.digest, "WORKFLOW_INPUT_CONFLICT")
        payload = command.as_dict()["payload"]
        return {"options": options(payload), "parts": text_parts(payload["message"])}

    def save(ctx, update):
        return ledger.step(update["taskId"], update["step"], update["state"], artifacts=update.get("artifacts"), message=update.get("message"))

    def transform(ctx, item):
        require(isinstance(item, dict) and isinstance(item.get("text"), str)
                and item.get("transform") in {"echo", "upper"}, "INVALID_ACTIVITY")
        return item["text"].upper() if item["transform"] == "upper" else item["text"]

    def control(ctx, event):
        return ledger.verify_control(event["taskId"], event["kind"], event["body"])

    runtime.register_workflow(make_workflow(sdk), name=NAME)
    for fn, name in ((load, "a2amq-load-v1"), (save, "a2amq-save-v1"), (transform, "a2amq-transform-v1"), (control, "a2amq-control-v1")):
        runtime.register_activity(fn, name=name)


class DaprActions:
    """Serialize schedule/reconcile in the SQL scope transaction; bounded SDK RPCs.

    Dapr 1.18 ignores reuse_id_policy. We inspect prior input and persist acceptance
    under a database writer lock instead. Retain Dapr history until SQL acceptance;
    automatic Dapr history purging is deliberately outside this dispatcher.
    """

    def __init__(self, ledger: TaskLedger, client):
        self.ledger, self.client = ledger, client

    def _start(self, db, job):
        request = Envelope(job["wire"])
        task_id = job["task_id"]
        expected = {"taskId": task_id, "digest": request.digest}
        if job["started"] or state_of(self.ledger._task(db, task_id)) in TERMINAL:
            return
        state = self.client.get_workflow_state(task_id, fetch_payloads=True)
        if state is None:
            returned = self.client.schedule_new_workflow(NAME, input=expected, instance_id=task_id)
            require(returned == task_id, "WORKFLOW_ID_MISMATCH")
        else:
            require(state.name == NAME and load_json(state.serialized_input) == expected,
                    "WORKFLOW_INPUT_CONFLICT")
        db.execute("UPDATE a2amq_jobs SET started=1 WHERE tenant=? AND consumer=? AND task_id=?",
                   (*self.ledger.keys, task_id))

    def ensure_started(self, envelope: Envelope):
        self.ledger.accept(envelope)
        try:
            with self.ledger.store._transaction() as db:
                self._start(db, self.ledger._job(db, envelope.as_dict()["taskId"]))
        except (PermanentError, RetryableError):
            raise
        except Exception:
            raise RetryableError("WORKFLOW_UNAVAILABLE") from None

    def dispatch(self, action: Action):
        try:
            with self.ledger.store._transaction() as db:
                row = db.execute("SELECT * FROM a2amq_actions WHERE tenant=? AND consumer=? AND action_id=?",
                                 (*self.ledger.keys, action.action_id)).fetchone()
                require(row is not None and (row["task_id"],row["kind"],row["digest"],row["body"]) ==
                        (action.task_id,action.kind,action.digest,action.body), "CONTROL_ID_CONFLICT")
                if row["delivered_at"] is not None:
                    return
                job = self.ledger._job(db, action.task_id)
                if action.kind == "start":
                    self._start(db, job)
                else:
                    require(action.kind in {"input", "auth", "cancel"}, "INVALID_CONTROL")
                    if state_of(self.ledger._task(db, action.task_id)) not in TERMINAL:
                        if not job["started"]:
                            raise RetryableError("WORKFLOW_NOT_STARTED")
                        self.client.raise_workflow_event(action.task_id, action.kind, data=load_json(action.body))
                db.execute("UPDATE a2amq_actions SET delivered_at=? WHERE tenant=? AND consumer=? AND action_id=?",
                           (self.ledger.store._now(),*self.ledger.keys, action.action_id))
        except (PermanentError, RetryableError):
            raise
        except Exception:
            raise RetryableError("WORKFLOW_UNAVAILABLE") from None

    def reconcile(self, task_id):
        # Runtime failure/termination is separate from an accepted schedule. Only
        # terminal runtime failures can close an otherwise nonterminal SQL task.
        try:
            state = self.client.get_workflow_state(task_id, fetch_payloads=False)
            if state is not None and state.runtime_status.name in {"FAILED", "TERMINATED"}:
                with self.ledger.store._transaction() as db:
                    task = self.ledger._task(db, task_id)
                    if state_of(task) not in TERMINAL:
                        return self.ledger._snapshot(db, Envelope(self.ledger._job(db, task_id)["wire"]),
                                                     "runtime-failed", "FAILED")
        except (PermanentError, RetryableError):
            raise
        except Exception:
            raise RetryableError("WORKFLOW_UNAVAILABLE") from None


class ActionRelay:
    def __init__(self, ledger, database, *, publisher=None, actions=None, batch=50, timeout=30):
        require(publisher is not None or actions is not None, "DISPATCHER_REQUIRED")
        require(type(batch) is int and 1 <= batch <= 200, "INVALID_PAGE_SIZE")
        require(type(timeout) in {int,float} and 0 < timeout <= 300, "INVALID_TIMEOUT")
        self.ledger, self.database, self.publisher, self.actions = ledger,database,publisher,actions
        self.batch, self.timeout, self.cursor = batch,timeout,None
        self.lock = asyncio.Lock()

    async def flush_once(self):
        kinds = (("publish",) if self.publisher is not None else ()) + (("start","input","auth","cancel") if self.actions else ())
        async with self.lock:
            rows = await self.database.call(self.ledger.pending, kinds, limit=self.batch, after=self.cursor)
            sent = 0
            for action in rows:
                try:
                    async with asyncio.timeout(self.timeout):
                        if action.kind == "publish":
                            await self.publisher.publish(Envelope(action.body))
                            await self.database.call(self.ledger.delivered, action)
                        else:
                            await self.database.call(self.actions.dispatch, action)
                    sent += 1
                except Exception:
                    LOG.warning(canonical({"event":"workflow_dispatch_retry","actionId":action.action_id}))
            self.cursor = rows[-1].action_id if len(rows) == self.batch else None
            return sent

    async def run(self, stop, interval=1):
        require(type(interval) in {int,float} and 0.01 <= interval <= 60, "INVALID_RELAY_INTERVAL")
        while not stop.is_set():
            try:
                await self.flush_once()
            except Exception:
                LOG.warning('{"event":"workflow_dispatch_unavailable"}')
            try:
                await asyncio.wait_for(stop.wait(), interval)
            except TimeoutError:
                pass
