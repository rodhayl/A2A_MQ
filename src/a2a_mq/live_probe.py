"""Opt-in disposable end-to-end acceptance using real Dapr, brokers and SDK clients.

This program creates a new, randomly named Compose project. It never attaches to
an existing project. A failure is retained as a failed gate, not retried as a pass.
"""
from __future__ import annotations

import argparse
import asyncio
import json
from pathlib import Path
import shutil
import socket
import ssl
import subprocess
import time
from uuid import uuid4

import httpx

from .configuration import read_file
from .deployment import compose
from .domain import require


LIVE_REQUIRED_CHECKS = frozenset({
    "offline-admission-publish-confirmation-api-restart",
    "competing-workers-single-committed-completion",
    "actual-broker-fanout-two-independent-projections",
    "broker-outage-recovery-without-sidecar-restart",
    "input-approval-continuation",
    "native-workflow-fanout-fanin",
    "native-workflow-abrupt-worker-restart",
    "owner-coordinated-cancellation",
    "official-jsonrpc-rest-sse-client",
    "cross-tenant-and-owner-rejection",
    "live-dlq-archive-exact-replay-and-publish-scopes",
    "official-mcp-http-authenticated-query",
})


class ComposeRun:
    def __init__(self, root, broker, source):
        self.root = Path(root).absolute()
        self.project = "a2amq-acceptance-" + uuid4().hex[:12]
        self.file = compose(self.root, broker=broker, source_root=Path(source))
        self.commands = []

    def run(self, *args, input=None, timeout=240, capture=False):
        argv = ["docker", "compose", "-p", self.project, "-f", str(self.file), *args]
        log = self.root / ("command-%03d.log" % len(self.commands))
        with log.open("wb") as stream:
            result = subprocess.run(argv, input=input, stdout=subprocess.PIPE if capture else stream,
                stderr=stream, timeout=timeout, check=False)
        self.commands.append({"command": list(args[:4]), "returnCode": result.returncode})
        require(result.returncode == 0, "COMPOSE_COMMAND_FAILED")
        return result.stdout if capture else b""

    def scrub_secrets(self):
        """Remove generated disposable credentials after containers are stopped."""
        path = self.root / "secrets"
        if path.is_symlink() or not path.is_dir():
            raise RuntimeError("disposable secret directory unavailable or unsafe")
        shutil.rmtree(path)
        if path.exists():
            raise RuntimeError("disposable secret cleanup failed")

    def inspect(self, query):
        code = ("import json; from a2a_mq.configuration import Settings,store; "
                "s=store(Settings.from_env()); "
                "db=s._connection(); c=db.__enter__(); "
                "print(json.dumps([list(r) for r in c.execute(" + repr(query) + ")])); "
                "db.__exit__(None,None,None); s.close()")
        raw = self.run("exec", "-T", "api", "python", "-c", code, capture=True, timeout=30)
        return json.loads(raw)

    def publish(self, instance, route, event, *, expected_status=None):
        code = '''import asyncio,json,sys
from a2a_mq.configuration import Settings
from a2a_mq.transport import DaprPublisher
from a2a_mq.vault import FileToken
async def run():
 s=Settings.from_env(); b=DaprPublisher(s.dapr_http,component=s.component,api_token=FileToken(s.dapr_token_file)())
 try:
  from a2a_mq.domain import canonical
  status=await b._status("POST",f"/v1.0/publish/{b.component}/{sys.argv[1]}",
      {**b.headers,"content-type":"application/cloudevents+json"},canonical(json.load(sys.stdin)).encode())
  print(json.dumps({"status":status}))
 finally:
  await b.close()
asyncio.run(run())
'''
        raw = self.run("exec", "-T", instance, "python", "-c", code, route,
                       input=json.dumps(event).encode(), timeout=30, capture=True)
        return checked_publish_status(json.loads(raw)["status"], expected_status)


def broker_service(broker: str) -> str:
    require(broker in {"jetstream", "rabbitmq"}, "INVALID_BROKER")
    return "nats" if broker == "jetstream" else "rabbitmq"


def broker_outage_hold_seconds(broker: str) -> int:
    """Hold outages long enough to exhaust bounded client reconnect windows."""
    broker_service(broker)
    return 135 if broker == "jetstream" else 5


def checked_publish_status(status, expected=None):
    require(type(status) is int, "INVALID_PUBLISH_STATUS")
    require(status == expected if expected is not None else 200 <= status < 300,
            "UNEXPECTED_PUBLISH_STATUS")
    return status


def require_access_denial(error, not_found_type):
    if isinstance(error, not_found_type):
        return
    if isinstance(error, httpx.HTTPStatusError) and error.response.status_code in {401, 403, 404}:
        return
    cause = getattr(error, "__cause__", None)
    if isinstance(cause, httpx.HTTPStatusError) and cause.response.status_code in {401, 403, 404}:
        return
    raise error


def token(root, *, subject="acceptance", tenant="demo", audience="https://localhost:18443"):
    import jwt
    now = int(time.time())
    return jwt.encode({"iss":"https://local-idp.example", "aud":audience, "sub":subject,
                       "tenant":tenant, "scope":"a2a:admin", "iat":now, "nbf":now, "exp":now+1800},
                      read_file(str(root / "secrets/jwt-private.pem"), secret=True),
                      algorithm="RS256", headers={"kid":"local"})


async def wait_until(function, *, timeout=120):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            value = await function()
            if value:
                return value
        except (httpx.HTTPError, OSError):
            pass
        await asyncio.sleep(.2)
    raise TimeoutError("LIVE_ACCEPTANCE_DEADLINE")


async def run_probe(fixture, *, broker):
    from google.protobuf.json_format import ParseDict
    from a2a.client.client import ClientConfig
    from a2a.client.client_factory import ClientFactory
    from a2a.types import a2a_pb2 as pb
    root = fixture.root
    context = ssl.create_default_context(cafile=str(root / "secrets/tls.crt"))
    facts = {"broker":broker, "checks":[], "realInfrastructure":True, "fixtureUsesModel":False}
    async with httpx.AsyncClient(verify=context, timeout=35, trust_env=False,
                                headers={"Authorization":"Bearer " + token(root)}) as http:
        async def ready():
            r=await http.get("https://localhost:18443/health/ready", headers={
                "dapr-api-token":read_file(str(root/"secrets/api-app"),secret=True)})
            return r.status_code==200
        await wait_until(ready)
        card=ParseDict((await http.get("https://localhost:18443/.well-known/agent-card.json")).json(),pb.AgentCard())
        def factory(binding):
            return ClientFactory(ClientConfig(streaming=False, polling=True, httpx_client=http,
                supported_protocol_bindings=[binding], use_client_preference=True)).create(card)
        client=factory("JSONRPC")
        async def send(rid, text="acceptance", *, parts=None, **opts):
            req=ParseDict({"message":{"messageId":rid,"role":"ROLE_USER","parts":
                [{"text":value} for value in (parts if parts is not None else [text])]},
                "configuration":{"returnImmediately":True},"metadata":{"a2amq":opts}},pb.SendMessageRequest())
            responses=[r async for r in client.send_message(req)]
            require(len(responses)==1 and responses[0].HasField('task'),"A2A_TASK_RESPONSE_REQUIRED")
            return responses[0].task
        async def state(task_id,wanted):
            t=await client.get_task(pb.GetTaskRequest(id=task_id))
            return t if pb.TaskState.Name(t.status.state).removeprefix("TASK_STATE_") in wanted else None
        task=await send("offline-1")
        require(task.status.state==pb.TASK_STATE_SUBMITTED,"OFFLINE_SUBMISSION_NOT_DURABLE")
        async def published():
            rows=await asyncio.to_thread(fixture.inspect,"SELECT COUNT(*) FROM a2amq_actions WHERE kind='publish' AND delivered_at IS NOT NULL")
            return rows[0][0]==1
        await wait_until(published)
        await asyncio.to_thread(fixture.run,"restart","api")
        await asyncio.to_thread(fixture.run,"restart","api-dapr")
        await wait_until(ready)
        require((await client.get_task(pb.GetTaskRequest(id=task.id))).status.state==pb.TASK_STATE_SUBMITTED,
                "OFFLINE_RESTART_LOST_TASK")
        facts["checks"].append("offline-admission-publish-confirmation-api-restart")
        await asyncio.to_thread(fixture.run,"up","-d","workflow-a-dapr","workflow-b-dapr","observer-a-dapr","observer-b-dapr","operator-dapr")
        complete=await wait_until(lambda:state(task.id,{"COMPLETED"}))
        require(complete.artifacts[0].parts[0].text=="acceptance","ARTIFACT_MISMATCH")
        same=await send("offline-1")
        require(same.id==task.id,"DUPLICATE_CREATED_NEW_TASK")
        rows=await asyncio.to_thread(fixture.inspect,"SELECT COUNT(*) FROM a2amq_steps WHERE step_id='completed'")
        require(rows[0][0]==1,"DUPLICATE_COMMITTED_RESULT")
        facts["checks"].append("competing-workers-single-committed-completion")
        async with httpx.AsyncClient(timeout=5,trust_env=False) as internal:
            async def observed(name,port):
                r=await internal.get(f"http://127.0.0.1:{port}/internal/tasks",headers={
                    "dapr-api-token":read_file(str(root/f"secrets/{name}-app"),secret=True)})
                return r.status_code==200 and any(t['id']==task.id for t in r.json()['tasks'])
            await wait_until(lambda:observed("observer-a",18001))
            await wait_until(lambda:observed("observer-b",18002))
        facts["checks"].append("actual-broker-fanout-two-independent-projections")

        # A healthy-at-start broker is insufficient production evidence. Keep all
        # daprd sidecars alive while the broker disappears and returns, then prove
        # a durably admitted task drains without sidecar/application restart.
        broker_name=broker_service(broker)
        await asyncio.to_thread(fixture.run,"stop",broker_name)
        outage=await send("broker-outage-recovery",text="broker recovers")
        require(outage.status.state==pb.TASK_STATE_SUBMITTED,"BROKER_OUTAGE_NOT_DURABLY_ADMITTED")
        require((await client.get_task(pb.GetTaskRequest(id=outage.id))).status.state!=pb.TASK_STATE_COMPLETED,
                "BROKER_OUTAGE_COMPLETED_WITHOUT_BROKER")
        # JetStream's upstream nats.go default is 60 reconnect attempts at 2s.
        # Exceed that budget so this gate catches a permanently closed component,
        # rather than only testing a short transient disconnect.
        await asyncio.sleep(broker_outage_hold_seconds(broker))
        await asyncio.to_thread(fixture.run,"up","-d",broker_name)
        recovered=await wait_until(lambda:state(outage.id,{"COMPLETED"}),timeout=180)
        require(recovered.artifacts[0].parts[0].text=="broker recovers","BROKER_RECOVERY_ARTIFACT_MISMATCH")
        facts["checks"].append("broker-outage-recovery-without-sidecar-restart")
        paused=await send("input-auth",inputRequired=True,authRequired=True,transform="upper")
        await wait_until(lambda:state(paused.id,{"INPUT_REQUIRED"}))
        req=ParseDict({"message":{"messageId":"continuation-1","role":"ROLE_USER","taskId":paused.id,
             "contextId":paused.context_id,"parts":[{"text":"continued"}]},
             "configuration":{"returnImmediately":True}},pb.SendMessageRequest())
        _=[r async for r in client.send_message(req)]
        await wait_until(lambda:state(paused.id,{"AUTH_REQUIRED"}))
        r=await http.post(f"https://localhost:18443/operations/tasks/{paused.id}/approve",
                          json={"requestId":"approve-1","approved":True})
        require(r.status_code in {200,202},"APPROVAL_NOT_ACCEPTED")
        finished=await wait_until(lambda:state(paused.id,{"COMPLETED"}))
        require(finished.artifacts[0].parts[0].text=="CONTINUED","CONTINUATION_OUTPUT_MISMATCH")
        facts["checks"].append("input-approval-continuation")
        multipart=await send("fanout-1",parts=["one","two","three"],transform="upper")
        gathered=await wait_until(lambda:state(multipart.id,{"COMPLETED"}))
        require([p.text for p in gathered.artifacts[0].parts]==["ONE","TWO","THREE"],"FANIN_ORDER_MISMATCH")
        facts["checks"].append("native-workflow-fanout-fanin")
        delayed=await send("restart-workflow",delaySeconds=15)
        await wait_until(lambda:state(delayed.id,{"WORKING"}))
        await asyncio.to_thread(fixture.run,"kill","-s","SIGKILL","workflow-a","workflow-b")
        await asyncio.sleep(0.5)
        await asyncio.to_thread(fixture.run,"up","-d","--force-recreate","workflow-a","workflow-b","workflow-a-dapr","workflow-b-dapr")
        await wait_until(lambda:state(delayed.id,{"COMPLETED"}),timeout=180)
        facts["checks"].append("native-workflow-abrupt-worker-restart")
        cancel=await send("cancel-workflow",delaySeconds=120)
        await wait_until(lambda:state(cancel.id,{"WORKING"}))
        canceled=await client.cancel_task(pb.CancelTaskRequest(id=cancel.id))
        require(canceled.status.state==pb.TASK_STATE_CANCELED,"CANCEL_NOT_CONFIRMED")
        facts["checks"].append("owner-coordinated-cancellation")
        rest=factory("HTTP+JSON")
        require((await rest.get_task(pb.GetTaskRequest(id=task.id))).id==task.id,"REST_INTEROP_FAILED")
        client._config.streaming = True
        try:
            streamed=[r async for r in client.subscribe(pb.SubscribeToTaskRequest(id=task.id))]
        finally:
            client._config.streaming = False
        require(bool(streamed) and streamed[0].task.status.state==pb.TASK_STATE_COMPLETED,"SSE_RECONNECT_FAILED")
        facts["checks"].append("official-jsonrpc-rest-sse-client")
        for subject,tenant in (("intruder","demo"),("acceptance","foreign")):
            bearer=token(root,subject=subject,tenant=tenant)
            if subject=="intruder":
                import jwt
                decoded=jwt.decode(bearer,options={"verify_signature":False})
                decoded['scope']='a2a:read'
                bearer=jwt.encode(decoded,read_file(str(root/'secrets/jwt-private.pem'),secret=True),
                                  algorithm='RS256',headers={'kid':'local'})
            http.headers['Authorization']='Bearer '+bearer
            denied=False
            try:
                await client.get_task(pb.GetTaskRequest(id=task.id))
            except Exception as error:
                from a2a.utils.errors import TaskNotFoundError
                require_access_denial(error, TaskNotFoundError)
                denied=True
            require(denied,"CROSS_PRINCIPAL_ACCESS")
        http.headers['Authorization']='Bearer '+token(root)
        facts["checks"].append("cross-tenant-and-owner-rejection")
        from .domain import Envelope
        from .transport import cloud_event
        rows=await asyncio.to_thread(fixture.inspect,"SELECT event_wire FROM a2amq_steps WHERE step_id='completed' ORDER BY task_id LIMIT 1")
        saved=Envelope.from_dict({**Envelope(rows[0][0]).as_dict(),"messageId":str(uuid4())})
        event=cloud_event(saved)
        await asyncio.to_thread(fixture.publish,"workflow-a","a2a.v1.demo.dlq",event)
        async def archived():
            r=await http.get("https://localhost:18443/operations/deadletters")
            require(r.status_code==200,"ARCHIVE_QUERY_FAILED")
            return r.json()['records']
        async def original_archived():
            return next((row for row in await archived() if row["reason"]=="BROKER_DEADLETTER"), None)
        original_record=await wait_until(original_archived,timeout=180)
        records=[original_record]
        replay=await http.post(f"https://localhost:18443/operations/deadletters/{records[0]['id']}/replay",
                              json={'requestId':'native-replay-1'})
        require(replay.status_code==202,"REPLAY_NOT_QUEUED")
        async def replayed():
            rows=await asyncio.to_thread(fixture.inspect,"SELECT COUNT(*) FROM a2amq_delivery_replays WHERE published_at IS NOT NULL")
            return rows[0][0]>=1
        await wait_until(replayed,timeout=180)
        await asyncio.to_thread(fixture.publish,"observer-a","a2a.v1.demo.dlq",event)
        await asyncio.to_thread(fixture.publish,"observer-a",saved.route,event,expected_status=403)
        malformed={**event,"id":str(uuid4())}
        await asyncio.to_thread(fixture.publish,"workflow-a",saved.route,malformed)
        async def quarantined():
            return any(row["reason"]=="IDENTIFIER_MISMATCH" for row in await archived())
        await wait_until(quarantined,timeout=180)
        facts["checks"].append("live-dlq-archive-exact-replay-and-publish-scopes")
        import httpx2
        from mcp import Client
        from mcp.client.streamable_http import streamable_http_client
        async with httpx2.AsyncClient(verify=context,timeout=30,follow_redirects=False,
            headers={'Authorization':'Bearer '+token(root,audience='https://localhost:18443/mcp')}) as mhttp:
            async with Client(streamable_http_client('https://localhost:18443/mcp',http_client=mhttp)) as mclient:
                listed=await mclient.list_tools()
                require(len(listed.tools)==6,"MCP_TOOLS_MISSING")
                result=await mclient.call_tool('get_task',{'task_id':task.id})
                require(not result.is_error,"MCP_OWNED_QUERY_FAILED")
        facts['checks'].append('official-mcp-http-authenticated-query')
    completed=set(facts["checks"])
    require(completed==LIVE_REQUIRED_CHECKS and len(facts["checks"])==len(completed),
            "INCOMPLETE_LIVE_MATRIX")
    return facts


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--broker',choices=['jetstream','rabbitmq'],required=True)
    parser.add_argument('--source',type=Path,required=True)
    parser.add_argument('--output',type=Path,required=True)
    parser.add_argument('--allow-disposable-containers',action='store_true')
    parser.add_argument('--keep-failed',action='store_true')
    args=parser.parse_args()
    require(args.allow_disposable_containers,'DISPOSABLE_OPT_IN_REQUIRED')
    require(not args.output.exists(),'OUTPUT_ALREADY_EXISTS')
    import a2a.client.client_factory
    import mcp
    del a2a, mcp
    subprocess.run(['docker','compose','version'],check=True,stdout=subprocess.DEVNULL,timeout=10)
    for port in (18000,18011,18012,18001,18002,18003,18443):
        with socket.socket() as probe:
            probe.bind(('127.0.0.1',port))
    fixture=ComposeRun(args.output,args.broker,args.source)
    report={'status':'FAILED','broker':args.broker,'productionCertified':False}
    try:
        fixture.run('build',timeout=1200)
        fixture.run('up','-d','api-dapr','gateway')
        report.update(asyncio.run(run_probe(fixture,broker=args.broker)),status='PASSED')
    except Exception as error:
        report['errorType']=type(error).__name__
        report['errorMessage']=str(error)
        import traceback
        report['traceback']=traceback.format_exc()
    finally:
        cleaned = False
        if report['status']=='PASSED' or not args.keep_failed:
            try:
                fixture.run('down','--volumes','--remove-orphans',timeout=120)
                cleaned = True
            except Exception:
                report['cleanupFailed']=True
                report['status']='FAILED'
        if cleaned:
            try:
                fixture.scrub_secrets()
            except Exception:
                report['secretCleanupFailed']=True
                report['status']='FAILED'
        report['commands']=fixture.commands
        (args.output/'live-verdict.json').write_text(json.dumps(report,indent=2)+'\n')
    print(json.dumps({k:v for k,v in report.items() if k!='commands'},indent=2))
    raise SystemExit(0 if report['status']=='PASSED' else 1)


if __name__=='__main__':
    main()
