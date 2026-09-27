"""Official SDK route contract. Selected explicitly; absence is not a passing gate."""
import asyncio
import time

import httpx
import jwt
import pytest
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.primitives.serialization import Encoding, PublicFormat

pytestmark = pytest.mark.upstream


@pytest.mark.parametrize("url", [
    "https://:443",
    "https://agent.example:70000",
    "https://agent.example:bad",
    "https://agent.example\n",
    "https://agent.example\t",
])
def test_bridge_rejects_malformed_public_url_before_sdk_import(url):
    from a2a_mq.bridge import create_bridge
    from a2a_mq.domain import PermanentError

    with pytest.raises(PermanentError, match="PUBLIC_HTTPS_URL_REQUIRED"):
        create_bridge(object(), object(), public_url=url)


@pytest.fixture
async def environment(tmp_path):
    pytest.importorskip("a2a.server.routes.jsonrpc_routes")
    from a2a_mq.bridge import create_bridge
    from a2a_mq.identity import JWTVerifier, Principal
    from a2a_mq.storage import SQLiteStore
    from a2a_mq.runtime import AsyncSQLite
    from a2a_mq.tasks import TaskLedger
    from a2a_mq.task_api import TaskAPI
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    verifier = JWTVerifier({"k": key.public_key().public_bytes(Encoding.PEM, PublicFormat.SubjectPublicKeyInfo)},
                           issuer="issuer", audience="a2amq")
    ledger = TaskLedger(SQLiteStore(tmp_path/"db", tenant="demo", consumer="workflow"))
    lane = AsyncSQLite(ledger.store)
    api = TaskAPI(ledger, lane, wait_timeout=5.0, poll_interval=.005)
    app = create_bridge(api, verifier, public_url="https://agent.example")
    user = Principal("alice", "demo", frozenset({"a2a:send", "a2a:read", "a2a:cancel"}))
    def headers(subject="alice", tenant="demo"):
        now = int(time.time())
        encoded = jwt.encode({"iss":"issuer", "aud":"a2amq", "sub":subject, "tenant":tenant,
                              "scope":"a2a:send a2a:read a2a:cancel", "iat":now, "nbf":now, "exp":now+300},
                             key, algorithm="RS256", headers={"kid":"k"})
        return {"Authorization":"Bearer "+encoded, "A2A-Version":"1.0"}
    try:
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app), base_url="https://agent.example") as client:
            yield client, ledger, headers, user
    finally:
        await lane.aclose()


async def test_official_jsonrpc_send_get_list(environment):
    client, ledger, headers, user = environment
    req = {"message":{"messageId":"one", "role":"ROLE_USER", "parts":[{"text":"hello"}]},
           "configuration":{"returnImmediately":True}}
    response = await client.post("/rpc", headers=headers(), json={"jsonrpc":"2.0", "id":1, "method":"SendMessage", "params":req})
    assert response.status_code == 200, response.text
    value = response.json()
    assert "error" not in value, value
    task = value["result"]["task"]
    tid = task["id"]
    ledger.step(tid, "working", "WORKING")
    ledger.step(tid, "completed", "COMPLETED", artifacts=[{"artifactId":"out", "parts":[{"text":"done"}]}])
    got = await client.post("/rpc", headers=headers(), json={"jsonrpc":"2.0", "id":2, "method":"GetTask", "params":{"id":tid}})
    assert got.json()["result"]["status"]["state"] == "TASK_STATE_COMPLETED"
    listed = await client.post("/rpc", headers=headers(), json={"jsonrpc":"2.0", "id":3, "method":"ListTasks", "params":{"pageSize":10}})
    assert len(listed.json()["result"]["tasks"]) == 1
    assert not listed.json()["result"]["tasks"][0].get("artifacts")


async def test_discovery_auth_and_rest(environment):
    client, ledger, headers, user = environment
    card = await client.get("/.well-known/agent-card.json")
    assert card.status_code == 200 and card.json()["capabilities"]["streaming"] is True
    assert len(card.json()["supportedInterfaces"]) == 2
    assert (await client.get("/tasks")).status_code == 401
    response = await client.get("/tasks", headers=headers())
    assert response.status_code == 200
    assert (await client.get("/tasks", headers=headers(tenant="other"))).status_code == 403


async def test_public_cancel_rejects_already_terminal_task(environment):
    client, ledger, headers, user = environment
    task = ledger.submit(user, {
        "message": {
            "messageId": "cancel-terminal",
            "role": "ROLE_USER",
            "parts": [{"text": "hello"}],
        }
    })
    ledger.step(task["id"], "already-canceled", "CANCELED")
    response = await client.post(
        "/rpc",
        headers=headers(),
        json={"jsonrpc": "2.0", "id": 5, "method": "CancelTask",
              "params": {"id": task["id"]}},
    )
    assert response.status_code == 200
    assert response.json()["error"]["code"] == -32002


async def test_terminal_task_message_returns_unsupported_operation(environment):
    client, ledger, headers, user = environment
    task = ledger.submit(user, {
        "message": {
            "messageId": "terminal-parent",
            "role": "ROLE_USER",
            "parts": [{"text": "hello"}],
        }
    })
    ledger.step(task["id"], "terminal-working", "WORKING")
    ledger.step(
        task["id"],
        "terminal-completed",
        "COMPLETED",
        artifacts=[{"artifactId": "out", "parts": [{"text": "done"}]}],
    )
    continuation = {
        "message": {
            "messageId": "terminal-continuation",
            "role": "ROLE_USER",
            "taskId": task["id"],
            "contextId": task["contextId"],
            "parts": [{"text": "again"}],
        },
        "configuration": {"returnImmediately": True},
    }
    response = await client.post(
        "/rpc",
        headers=headers(),
        json={"jsonrpc": "2.0", "id": 6, "method": "SendMessage", "params": continuation},
    )
    assert response.status_code == 200
    assert response.json()["error"]["code"] == -32004


async def test_official_send_streaming_message_sse(environment):
    client, ledger, headers, user = environment

    async def complete_after_admission():
        from a2a_mq.domain import Envelope
        for _ in range(200):
            pending = ledger.pending(("publish",))
            if pending:
                task_id = Envelope(pending[0].body).as_dict()["taskId"]
                ledger.step(task_id, "stream-working", "WORKING")
                await asyncio.sleep(0.01)
                ledger.step(
                    task_id,
                    "stream-completed",
                    "COMPLETED",
                    artifacts=[{"artifactId": "out", "parts": [{"text": "done"}]}],
                )
                return
            await asyncio.sleep(0.005)
        raise AssertionError("streaming task was not admitted")

    finisher = asyncio.create_task(complete_after_admission())
    req = {
        "message": {
            "messageId": "stream-one",
            "role": "ROLE_USER",
            "parts": [{"text": "hello"}],
        }
    }
    async with client.stream(
        "POST",
        "/rpc",
        headers={**headers(), "Accept": "text/event-stream"},
        json={"jsonrpc": "2.0", "id": 7, "method": "SendStreamingMessage", "params": req},
    ) as response:
        body = await response.aread()
        assert response.status_code == 200
        assert response.headers["content-type"].startswith("text/event-stream")
    await finisher
    assert b"TASK_STATE_COMPLETED" in body


async def test_official_subscribe_to_nonterminal_task_sse(environment):
    client, ledger, headers, user = environment
    task = ledger.submit(user, {
        "message": {
            "messageId": "subscribe-live",
            "role": "ROLE_USER",
            "parts": [{"text": "hello"}],
        }
    })

    async def complete():
        await asyncio.sleep(0.02)
        ledger.step(task["id"], "subscribe-working", "WORKING")
        await asyncio.sleep(0.02)
        ledger.step(
            task["id"],
            "subscribe-completed",
            "COMPLETED",
            artifacts=[{"artifactId": "out", "parts": [{"text": "done"}]}],
        )

    finisher = asyncio.create_task(complete())
    async with client.stream(
        "POST",
        "/rpc",
        headers={**headers(), "Accept": "text/event-stream"},
        json={"jsonrpc": "2.0", "id": 8, "method": "SubscribeToTask",
              "params": {"id": task["id"]}},
    ) as response:
        body = await response.aread()
        assert response.status_code == 200
        assert response.headers["content-type"].startswith("text/event-stream")
    await finisher
    assert b"TASK_STATE_COMPLETED" in body


async def test_subscribe_rejects_terminal_task(environment):
    client, ledger, headers, user = environment
    task = ledger.submit(user, {
        "message": {"messageId": "terminal-sub", "role": "ROLE_USER", "parts": [{"text": "hello"}]}
    })
    ledger.step(task["id"], "working-sub", "WORKING")
    ledger.step(task["id"], "completed-sub", "COMPLETED",
                artifacts=[{"artifactId": "out", "parts": [{"text": "done"}]}])
    response = await client.post(
        "/rpc",
        headers=headers(),
        json={"jsonrpc": "2.0", "id": 9, "method": "SubscribeToTask",
              "params": {"id": task["id"]}},
    )
    assert response.status_code == 200
    assert response.json()["error"]["code"] == -32004


async def test_owner_and_unsupported_push(environment):
    client, ledger, headers, user = environment
    task = ledger.submit(user, {"message":{"messageId":"one","role":"ROLE_USER","parts":[{"text":"hello"}]}})
    missing = await client.post("/rpc", headers=headers(subject="bob"), json={"jsonrpc":"2.0","id":1,"method":"GetTask","params":{"id":task["id"]}})
    assert missing.json()["error"]["code"] == -32001
    push = await client.post("/rpc", headers=headers(), json={"jsonrpc":"2.0","id":2,"method":"ListTaskPushNotificationConfigs","params":{"taskId":task["id"]}})
    assert push.json()["error"]["code"] == -32003
