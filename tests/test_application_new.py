"""Real SQL/HTTP lifecycle composition; external SDK runtime is an explicit seam."""
import asyncio
from contextlib import asynccontextmanager
from dataclasses import replace
import json
from types import SimpleNamespace

import httpx
import pytest
from starlette.applications import Starlette
from starlette.responses import JSONResponse
from starlette.routing import Route

from a2a_mq.application import Dependencies, ServiceApplication, _protectors
from a2a_mq.configuration import Settings, read_file
from a2a_mq.domain import PermanentError
from a2a_mq.identity import Principal
from a2a_mq.storage import SQLiteStore
from a2a_mq.transport import DaprPublisher, cloud_event


class Verifier:
    def __init__(self, audience):
        self.audience, self.issuer = audience, "https://issuer.example"
    def verify(self, encoded):
        if encoded != "fixture-token":
            raise PermanentError("UNAUTHENTICATED")
        return Principal("alice", "demo", frozenset({"a2a:admin"}))


class Runtime:
    def __init__(self, fail=False):
        self.started, self.stopped, self.fail = False, False, fail
    def start(self):
        self.started = True
        if self.fail:
            raise RuntimeError("fixture startup failed")
    def shutdown(self):
        self.stopped = True
    def wait_for_worker_ready(self, timeout):
        return self.started and not self.stopped


class Client:
    def __init__(self):
        self.closed, self.instances, self.events = False, {}, []
    def get_workflow_state(self, task_id, fetch_payloads=True):
        return self.instances.get(task_id)
    def schedule_new_workflow(self, name, input, instance_id):
        self.instances[instance_id] = SimpleNamespace(name=name, serialized_input=json.dumps(input),
                                                    runtime_status=SimpleNamespace(name="RUNNING"))
        return instance_id
    def raise_workflow_event(self, task_id, kind, data):
        self.events.append((task_id, kind, data))
    def close(self):
        self.closed = True


@pytest.fixture
def settings(tmp_path):
    for name in ("app", "dapr"):
        path = tmp_path / name
        path.write_text("a" * 32 if name == "app" else "d" * 32)
        path.chmod(0o600)
    return Settings(role="workflow", tenant="demo", consumer="tasks", sqlite_path=str(tmp_path / "db"),
                    app_token_file=str(tmp_path / "app"), dapr_token_file=str(tmp_path / "dapr"),
                    development=True, relay_interval=.01, public_url="https://api.example",
                    issuer="https://issuer.example", jwt_keys_file="unused")


def dependencies(settings, runtime=None):
    state = SimpleNamespace(sent=[], bodies=[], runtime=runtime or Runtime(), client=Client(), closed=False,
                            mcp_started=False, mcp_stopped=False, bridge_calls=[], ready=True)
    def bus(url, **kwargs):
        async def respond(request):
            if request.method == "POST":
                state.sent.append(json.loads(request.content))
            return httpx.Response(204 if state.ready else 503)
        client = httpx.AsyncClient(transport=httpx.MockTransport(respond))
        publisher = DaprPublisher(url, client=client, **kwargs)
        async def close():
            state.closed = True
            await client.aclose()
        publisher.close = close
        return publisher
    def bridge(api, verifier, public_url):
        async def view(request):
            state.bridge_calls.append(request.url.path)
            return JSONResponse({"bridge": True})
        return Starlette(routes=[Route("/rpc", view, methods=["POST"])])
    def mcp(api, verifier, resource_url):
        @asynccontextmanager
        async def manager():
            state.mcp_started = True
            try:
                yield
            finally:
                state.mcp_stopped = True
        async def app(scope, receive, send):
            await JSONResponse({"mcpPath": scope["path"], "audience": verifier.audience})(scope, receive, send)
        return SimpleNamespace(session_manager=SimpleNamespace(run=manager)), app
    deps = Dependencies(bus=bus, workflow=lambda settings, ledger, token: (state.runtime, state.client),
                        bridge=bridge, mcp=mcp, verifier=lambda settings, audience=None:
                        Verifier(audience or settings.public_url))
    return deps, state


@pytest.mark.parametrize("changes,code", [
    ({"role": "bad"}, "INVALID_ROLE"), ({"development": False}, "PRODUCTION_POSTGRES_REQUIRED"),
    ({"mcp_enabled": True}, "MCP_API_ROLE_REQUIRED"), ({"dapr_http": "http://remote:3500"}, "COLOCATED_DAPR_REQUIRED"),
    ({"dapr_http": "http://127.0.0.1/?x=1"}, "COLOCATED_DAPR_REQUIRED"),
    ({"dapr_http": "http://127.0.0.1:3500?"}, "COLOCATED_DAPR_REQUIRED"),
    ({"dapr_http": "http://127.0.0.1:3500#"}, "COLOCATED_DAPR_REQUIRED"),
    ({"dapr_grpc_port": True}, "INVALID_DAPR_PORT"), ({"max_active": 0}, "INVALID_QUOTA"),
    ({"callback_timeout": float("nan")}, "INVALID_TIMEOUT"), ({"concurrency": 0}, "INVALID_CONCURRENCY"),
    ({"role": "api", "public_url": "http://api"}, "PUBLIC_HTTPS_URL_REQUIRED"),
    ({"role": "api", "public_url": "https://bad host"}, "PUBLIC_HTTPS_URL_REQUIRED"),
    ({"role": "api", "public_url": "https://api.example", "otlp_endpoint": "https://bad host"}, "OTLP_HTTPS_REQUIRED"),
    ({"role": "operator", "issuer": "http://issuer"}, "HTTPS_ISSUER_REQUIRED"),
    ({"subscription_queue": "x;*"}, "INVALID_SUBSCRIPTION_QUEUE"),
    ({"postgres_dsn_file": "x"}, "EXACTLY_ONE_STORAGE_BACKEND_REQUIRED"),
])
def test_configuration_fails_before_io(settings, changes, code):
    with pytest.raises(PermanentError, match=code):
        replace(settings, **changes)


def test_secret_file_permissions_and_symlinks(tmp_path):
    path = tmp_path / "secret"
    path.write_text("password")
    path.chmod(0o644)
    with pytest.raises(PermanentError, match="UNSAFE_CONFIG_FILE"):
        read_file(str(path), secret=True)
    path.chmod(0o600)
    link = tmp_path / "link"
    link.symlink_to(path)
    with pytest.raises(PermanentError, match="CONFIG_FILE_UNAVAILABLE"):
        read_file(str(link), secret=True)
    assert read_file(str(path), secret=True) == "password"


def test_env_configuration_requires_explicit_boolean(settings):
    env = {"A2A_ROLE": "workflow", "A2A_SQLITE_PATH": settings.sqlite_path,
           "APP_API_TOKEN_FILE": settings.app_token_file, "DAPR_API_TOKEN_FILE": settings.dapr_token_file,
           "A2A_DEVELOPMENT": "1"}
    assert Settings.from_env(env).development
    env["A2A_ENABLE_MCP"] = "yes"
    with pytest.raises(PermanentError, match="INVALID_CONFIGURATION"):
        Settings.from_env(env)


async def test_worker_ack_means_dapr_acceptance_and_reconcile_terminal_sql(settings):
    deps, state = dependencies(settings)
    application = ServiceApplication(settings, dependencies=deps)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=application), base_url="http://app") as http:
        assert (await http.post("/events", json={})).status_code == 503
        async with application.app.router.lifespan_context(application.app):
            async with asyncio.timeout(10):
                while not application.workflow_ready:
                    await asyncio.sleep(.01)
            assert state.runtime.started
            req = {"message": {"messageId": "one", "role": "ROLE_USER", "parts": [{"text": "hello"}]}}
            caller = Principal("alice", "demo", frozenset({"a2a:admin"}))
            task = await application.database.call(application.ledger.submit, caller, req)
            cmd = await application.database.call(application.ledger.command, task["id"])
            event = {**cloud_event(cmd), "topic": cmd.route, "pubsubname": "bus"}
            headers = {"dapr-api-token": "a" * 32}
            assert (await http.post("/events", json=event)).status_code == 401
            assert (await http.post("/events", json=event, headers=headers)).json() == {"status": "SUCCESS"}
            assert task["id"] in state.client.instances
            await application.database.call(application.ledger.step, task["id"], "working", "WORKING")
            await application.database.call(application.ledger.step, task["id"], "completed", "COMPLETED")
            state.client.instances[task["id"]].runtime_status.name = "COMPLETED"
            async with asyncio.timeout(10):
                while await application.database.call(application.ledger.unconfirmed):
                    await asyncio.sleep(.01)
            assert (await http.get("/health/ready", headers=headers)).status_code == 200
            state.ready = False
            assert (await http.get("/health/ready", headers=headers)).status_code == 503
        assert state.runtime.stopped and state.client.closed and state.closed
        assert (await http.post("/events", json=event, headers=headers)).status_code == 503


async def test_startup_failure_closes_clients_and_does_not_serve(settings):
    deps, state = dependencies(settings, Runtime(fail=True))
    app = ServiceApplication(settings, dependencies=deps)
    async with app.app.router.lifespan_context(app.app):
        async with asyncio.timeout(10):
            while not any(t.done() for t in app.jobs):
                await asyncio.sleep(.01)
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://app") as http:
            assert (await http.get("/health/live")).status_code == 503
            assert not app.workflow_ready
    assert state.runtime.stopped and state.client.closed and state.closed and not app.running


async def test_api_mounts_mcp_without_path_stripping_and_owns_manager(settings):
    settings = replace(settings, role="api", mcp_enabled=True)
    deps, state = dependencies(settings)
    app = ServiceApplication(settings, dependencies=deps)
    async with app.app.router.lifespan_context(app.app):
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://app") as http:
            assert state.mcp_started
            response = await http.post("/mcp", json={})
            assert response.json() == {"mcpPath": "/mcp", "audience": "https://api.example/mcp"}
            assert (await http.get("/.well-known/oauth-protected-resource/mcp")).status_code == 200
            assert (await http.post("/rpc", json={})).json()["bridge"]
            assert (await http.get("/operations/status")).status_code == 401
            status = await http.get("/operations/status", headers={"authorization": "Bearer fixture-token"})
            assert status.json()["activeTasks"] == 0
            assert (await http.get("/dapr/subscribe", headers={"dapr-api-token": "a" * 32})).json() == []
    assert state.mcp_stopped and state.closed


async def test_observer_separate_projection_and_quorum_queue(settings):
    from a2a_mq.domain import command
    from a2a_mq.reference import echo_events
    settings = replace(settings, role="observer", consumer="observer-a", subscription_queue="observer-a-task")
    deps, state = dependencies(settings)
    app = ServiceApplication(settings, dependencies=deps)
    async with app.app.router.lifespan_context(app.app):
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://app",
                                    headers={"dapr-api-token": "a" * 32}) as http:
            discovery = (await http.get("/dapr/subscribe")).json()
            assert discovery[0]["metadata"] == {"queueName": "observer-a-task", "queueType": "quorum"}
            event = echo_events(command("x"))[-1]
            delivered = {**cloud_event(event), "pubsubname": "bus", "topic": event.route}
            assert (await http.post("/events", json=delivered)).json()["status"] == "SUCCESS"
            assert (await http.get("/internal/tasks")).json()["tasks"][0]["id"] == event.as_dict()["taskId"]
    other = SQLiteStore(settings.sqlite_path, tenant="demo", consumer="observer-b")
    assert other.list_tasks()[0] == []
    assert not state.runtime.started


async def test_operator_starts_only_dlq_replay_and_discovery(settings):
    settings = replace(settings, role="operator", subscription_queue="operator-dlq")
    deps, state = dependencies(settings)
    app = ServiceApplication(settings, dependencies=deps)
    async with app.app.router.lifespan_context(app.app):
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://app") as http:
            assert (await http.get("/dapr/subscribe")).status_code == 401
            value = (await http.get("/dapr/subscribe", headers={"dapr-api-token": "a" * 32})).json()[0]
            assert "deadLetterTopic" not in value and value["metadata"]["queueType"] == "quorum"
            assert (await http.post("/events", json={}, headers={"dapr-api-token": "a" * 32})).json()["status"] == "RETRY"
    assert state.closed and not state.runtime.started


@pytest.mark.parametrize("config", [
    {"tokenFile": "/tmp/token", "command": {}, "event": {}},
    {"vaultUrl": "https://vault.example", "command": {}, "event": {}},
    {
        "vaultUrl": "https://vault.example",
        "tokenFile": "/tmp/token",
        "command": {"receiver": "workers", "active": "k", "keys": [{"name": "key", "writeVersion": 1}]},
        "event": {"receiver": "observers", "active": "k", "keys": [{"id": "k", "name": "key", "writeVersion": 1}]},
    },
])
async def test_crypto_config_missing_required_fields_fails_deterministically(settings, tmp_path, config):
    from contextlib import AsyncExitStack

    path = tmp_path / "crypto.json"
    path.write_text(json.dumps(config))
    protected = replace(settings, crypto_file=str(path))
    async with AsyncExitStack() as cleanup:
        with pytest.raises(PermanentError, match="INVALID_CRYPTO_CONFIG"):
            await _protectors(protected, cleanup)


@pytest.mark.parametrize("change,code", [
    (lambda config: config["command"]["keys"][0].update(writeVersion=0), "INVALID_KEY_VERSION"),
    (lambda config: config["command"].update(active="missing-key"), "UNKNOWN_KEY"),
    (lambda config: config.update(vaultUrl="http://vault.example"), "VAULT_HTTPS_REQUIRED"),
])
async def test_crypto_static_validation_precedes_missing_token(settings, tmp_path, change, code):
    from contextlib import AsyncExitStack

    config = {
        "vaultUrl": "https://vault.example",
        "tokenFile": str(tmp_path / "missing-vault-token"),
        "command": {"receiver": "workers", "active": "command-key",
                    "keys": [{"id": "command-key", "name": "commands", "writeVersion": 1}]},
        "event": {"receiver": "observers", "active": "event-key",
                  "keys": [{"id": "event-key", "name": "events", "writeVersion": 1}]},
    }
    change(config)
    path = tmp_path / "crypto-static-invalid.json"
    path.write_text(json.dumps(config))
    protected = replace(settings, crypto_file=str(path))
    async with AsyncExitStack() as cleanup:
        with pytest.raises(PermanentError, match=code):
            await _protectors(protected, cleanup)


async def test_crypto_startup_validates_vault_token_file_before_readiness(settings, tmp_path):
    from contextlib import AsyncExitStack

    config = {
        "vaultUrl": "https://vault.example",
        "tokenFile": str(tmp_path / "missing-vault-token"),
        "command": {"receiver": "workers", "active": "command-key",
                    "keys": [{"id": "command-key", "name": "commands", "writeVersion": 1}]},
        "event": {"receiver": "observers", "active": "event-key",
                  "keys": [{"id": "event-key", "name": "events", "writeVersion": 1}]},
    }
    path = tmp_path / "crypto-complete.json"
    path.write_text(json.dumps(config))
    protected = replace(settings, crypto_file=str(path))
    async with AsyncExitStack() as cleanup:
        with pytest.raises(PermanentError, match="SECRET_UNAVAILABLE"):
            await _protectors(protected, cleanup)
