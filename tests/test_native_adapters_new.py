"""Required native SDK gates. Missing dependencies FAIL when this scope is selected."""
import asyncio
import time

import httpx
import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa
import jwt

from a2a_mq.identity import JWTVerifier
from a2a_mq.runtime import AsyncSQLite
from a2a_mq.storage import SQLiteStore
from a2a_mq.task_api import TaskAPI
from a2a_mq.tasks import TaskLedger


def identity(audience):
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    pem = key.public_key().public_bytes(serialization.Encoding.PEM,
                                        serialization.PublicFormat.SubjectPublicKeyInfo)
    now = int(time.time())
    def issue(subject="alice", tenant="demo", scope="a2a:send a2a:read a2a:cancel a2a:approve"):
        return jwt.encode(dict(iss="https://issuer.example", aud=audience, sub=subject,
                              tenant=tenant, scope=scope, iat=now, nbf=now, exp=now+300),
                          key, algorithm="RS256", headers={"kid": "test"})
    return JWTVerifier({"test": pem}, issuer="https://issuer.example", audience=audience), issue


@pytest.mark.upstream
@pytest.mark.parametrize("binding", ["JSONRPC", "HTTP+JSON"])
async def test_official_a2a_client_routes_and_owner_isolation(tmp_path, binding):
    from google.protobuf.json_format import ParseDict
    from a2a.client.client import ClientConfig
    from a2a.client.client_factory import ClientFactory
    from a2a.types import a2a_pb2 as pb
    from a2a_mq.bridge import create_bridge

    store = SQLiteStore(tmp_path / "state.db", tenant="demo", consumer="tasks")
    database = AsyncSQLite(store)
    ledger = TaskLedger(store)
    api = TaskAPI(ledger, database, poll_interval=.005)
    verifier, issue = identity("https://api.example")
    app = create_bridge(api, verifier, public_url="https://api.example")
    try:
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app),
                                     base_url="https://api.example", timeout=10,
                                     headers={"Authorization": "Bearer " + issue()}) as http:
            card_response = await http.get("/.well-known/agent-card.json")
            assert card_response.status_code == 200
            card = ParseDict(card_response.json(), pb.AgentCard())
            client = ClientFactory(ClientConfig(streaming=False, polling=True, httpx_client=http,
                supported_protocol_bindings=[binding], use_client_preference=True)).create(card)
            request = ParseDict({"message": {"messageId": "native-test", "role": "ROLE_USER",
                        "parts": [{"text": "native codec test"}]},
                        "configuration": {"returnImmediately": True}}, pb.SendMessageRequest())
            responses = [item async for item in client.send_message(request)]
            assert len(responses) == 1 and responses[0].HasField("task")
            task = responses[0].task
            assert task.status.state == pb.TASK_STATE_SUBMITTED
            same = [item async for item in client.send_message(request)]
            assert same[0].task.id == task.id
            queried = await client.get_task(pb.GetTaskRequest(id=task.id))
            assert queried.id == task.id
            listed = await client.list_tasks(pb.ListTasksRequest(page_size=10, include_artifacts=True))
            assert [value.id for value in listed.tasks] == [task.id]
            ledger.step(task.id, "work", "WORKING")
            ledger.step(task.id, "done", "COMPLETED", artifacts=[{"artifactId":"result","parts":[{"text":"ok"}]}])
            client._config.streaming = True
            stream = client.subscribe(pb.SubscribeToTaskRequest(id=task.id))
            items = [item async for item in stream]
            assert items and items[0].task.status.state == pb.TASK_STATE_COMPLETED
            client._config.streaming = False

            cancel_request = ParseDict({"message": {"messageId": "native-cancel",
                        "role": "ROLE_USER", "parts": [{"text": "cancel me"}]},
                        "configuration": {"returnImmediately": True}}, pb.SendMessageRequest())
            cancel_target = [item async for item in client.send_message(cancel_request)][0].task

            # a2a-sdk 1.1.2's default live-task registry has had owner-isolation
            # regressions on cancel/subscribe. Our custom handler must keep both
            # paths owner-scoped before delegating to the durable TaskAPI.
            from a2a.utils.errors import TaskNotFoundError
            from a2a_mq.live_probe import require_access_denial
            http.headers["Authorization"] = "Bearer " + issue("mallory")
            with pytest.raises(Exception) as denied:
                await client.cancel_task(pb.CancelTaskRequest(id=cancel_target.id))
            require_access_denial(denied.value, TaskNotFoundError)
            client._config.streaming = True
            intruder_stream = client.subscribe(pb.SubscribeToTaskRequest(id=cancel_target.id))
            with pytest.raises(Exception) as denied:
                await anext(intruder_stream)
            require_access_denial(denied.value, TaskNotFoundError)
            client._config.streaming = False
            http.headers["Authorization"] = "Bearer " + issue()

            async def owner_cancel():
                while not ledger.pending(("cancel",)):
                    await asyncio.sleep(.005)
                ledger.step(cancel_target.id, "canceled", "CANCELED")

            owner_job = asyncio.create_task(owner_cancel())
            try:
                async with asyncio.timeout(5):
                    canceled = await client.cancel_task(pb.CancelTaskRequest(id=cancel_target.id))
                    await owner_job
            finally:
                if not owner_job.done():
                    owner_job.cancel()
                    await asyncio.gather(owner_job, return_exceptions=True)
            assert canceled.id == cancel_target.id
            assert canceled.status.state == pb.TASK_STATE_CANCELED

            http.headers["Authorization"] = "Bearer " + issue("mallory")
            assert not (await client.list_tasks(pb.ListTasksRequest(page_size=10))).tasks
            with pytest.raises(Exception) as denied:
                await client.get_task(pb.GetTaskRequest(id=task.id))
            require_access_denial(denied.value, TaskNotFoundError)
            http.headers["Authorization"] = "Bearer " + issue(tenant="other")
            with pytest.raises(Exception) as denied:
                await client.list_tasks(pb.ListTasksRequest(page_size=10))
            require_access_denial(denied.value, TaskNotFoundError)
            await client.close()
    finally:
        await database.aclose()


@pytest.mark.upstream
def test_official_workflow_registration(tmp_path):
    import dapr.ext.workflow as sdk
    from a2a_mq.workflows import register
    store = SQLiteStore(tmp_path / "wf.db", tenant="demo", consumer="tasks")
    runtime = sdk.WorkflowRuntime(host="127.0.0.1", port="50001", worker_ready_timeout=.1)
    try:
        register(runtime, TaskLedger(store), sdk=sdk)
    finally:
        runtime.shutdown()


@pytest.mark.mcp
async def test_official_mcp_http_tools_and_authenticated_task(tmp_path):
    import httpx2
    from mcp import Client
    from mcp.client.streamable_http import streamable_http_client
    from a2a_mq.mcp_server import create_mcp
    store = SQLiteStore(tmp_path / "mcp.db", tenant="demo", consumer="tasks")
    database = AsyncSQLite(store)
    api = TaskAPI(TaskLedger(store), database)
    verifier, issue = identity("https://api.example/mcp")
    server, app = create_mcp(api, verifier, resource_url="https://api.example/mcp")
    assert server.settings.auth.validate_token_resource is False
    try:
        async with server.session_manager.run():
            async with httpx2.AsyncClient(transport=httpx2.ASGITransport(app=app),
                    headers={"Authorization":"Bearer " + issue()}, timeout=10,
                    follow_redirects=False) as http:
                async with asyncio.timeout(20):
                    transport = streamable_http_client("https://api.example/mcp", http_client=http)
                    async with Client(transport) as client:
                        listed = await client.list_tools()
                        names = {tool.name for tool in listed.tools}
                        assert names == {"submit_task", "get_task", "list_tasks", "continue_task", "cancel_task", "approve_task"}
                        result = await client.call_tool("list_tasks", {})
                        assert not result.is_error
                        absent = await client.call_tool("get_task", {"task_id":"missing"})
                        assert absent.is_error
                http.headers["Authorization"] = "Bearer invalid-token"
                denied = await http.post("/mcp", json={"jsonrpc":"2.0","id":"denied","method":"tools/list"})
                assert denied.status_code == 401
    finally:
        await database.aclose()
