import pytest

from a2a_mq.domain import Envelope, PermanentError, state_of
from a2a_mq.identity import Principal
from a2a_mq.local_lite import (_close_local_resources, LocalLiteActions, LocalLiteSettings,
                               LocalLiteTaskAPI)
from a2a_mq.runtime import AsyncSQLite
from a2a_mq.storage import SQLiteStore
from a2a_mq.tasks import TaskLedger


def principal():
    return Principal("owner", "demo", frozenset({"a2a:send", "a2a:read", "a2a:cancel"}))


def request(message_id="request-1", **a2amq):
    value = {"message": {"messageId": message_id, "role": "ROLE_USER", "parts": [{"text": "hello"}]},
             "configuration": {"returnImmediately": True}}
    if a2amq:
        value["metadata"] = {"a2amq": a2amq}
    return value


def test_local_lite_executes_existing_ledger_without_workflow(tmp_path):
    store = SQLiteStore(tmp_path / "local.db", tenant="demo", consumer="local")
    ledger = TaskLedger(store, source="local-worker")
    task = ledger.submit(principal(), request(transform="upper"))
    publish = ledger.pending(("publish",))[0]
    command = Envelope(publish.body)
    ledger.delivered(publish)
    assert ledger.accept(command) is True
    start = next(a for a in ledger.pending(("start",)) if a.task_id == task["id"])
    LocalLiteActions(ledger).dispatch(start)
    completed = ledger.get(principal(), task["id"])
    assert state_of(completed) == "COMPLETED"
    assert completed["artifacts"][0]["parts"][0]["text"] == "HELLO"
    with store._connection() as db:
        assert db.execute("SELECT COUNT(*) FROM a2amq_runtime_end WHERE task_id=?", (task["id"],)).fetchone()[0] == 1


def test_local_lite_cancel_before_start_is_terminal_and_idempotent(tmp_path):
    store = SQLiteStore(tmp_path / "local.db", tenant="demo", consumer="local")
    ledger = TaskLedger(store, source="local-worker")
    task = ledger.submit(principal(), request())
    publish = ledger.pending(("publish",))[0]
    command = Envelope(publish.body)
    ledger.delivered(publish)
    ledger.control(principal(), task["id"], "cancel", "cancel-1")
    cancel = ledger.pending(("cancel",))[0]
    actions = LocalLiteActions(ledger)
    actions.dispatch(cancel)
    assert state_of(ledger.get(principal(), task["id"])) == "CANCELED"
    assert ledger.accept(command) is True
    start = next(a for a in ledger.pending(("start",)) if a.task_id == task["id"])
    actions.dispatch(start)
    assert state_of(ledger.get(principal(), task["id"])) == "CANCELED"


def test_local_lite_rejects_workflow_only_options():
    LocalLiteTaskAPI._validate_local_request(request(transform="echo"))
    for item in (request(inputRequired=True), request(authRequired=True), request(delaySeconds=1)):
        try:
            LocalLiteTaskAPI._validate_local_request(item)
        except PermanentError as exc:
            assert exc.code == "WORKFLOW_FEATURE_UNAVAILABLE"
        else:
            raise AssertionError("workflow-only option accepted")


def test_local_lite_issuer_uses_strict_https_validation(tmp_path):
    base = dict(
        sqlite_path=str(tmp_path / "db"),
        app_token_file=str(tmp_path / "app"),
        dapr_token_file=str(tmp_path / "dapr"),
        jwt_keys_file=str(tmp_path / "keys"),
        public_url="http://127.0.0.1:18000",
    )
    assert LocalLiteSettings(**base, issuer="https://local-a2amq.invalid").issuer
    for issuer in (
        "http://local-a2amq.invalid",
        "https://user:pass@local-a2amq.invalid",
        "https://local-a2amq.invalid?key=value",
        "https://local-a2amq.invalid#fragment",
    ):
        with pytest.raises(PermanentError):
            LocalLiteSettings(**base, issuer=issuer)


async def test_combined_action_relay_handles_publish_and_start_in_one_lane(tmp_path):
    from a2a_mq.workflows import ActionRelay

    class Publisher:
        def __init__(self):
            self.events = []

        async def publish(self, envelope):
            self.events.append(envelope)

    store = SQLiteStore(tmp_path / "local.db", tenant="demo", consumer="local")
    ledger = TaskLedger(store, source="local-worker")
    task = ledger.submit(principal(), request(transform="upper"))
    database = AsyncSQLite(store, capacity=4)
    publisher = Publisher()
    actions = LocalLiteActions(ledger)
    relay = ActionRelay(ledger, database, publisher=publisher, actions=actions)
    try:
        assert await relay.flush_once() == 1
        assert len(publisher.events) == 1
        assert ledger.accept(publisher.events[0]) is True
        assert await relay.flush_once() == 1
        assert state_of(ledger.get(principal(), task["id"])) == "COMPLETED"
    finally:
        await database.aclose()


async def test_local_resource_cleanup_always_closes_publisher():
    events = []

    class Database:
        async def aclose(self):
            events.append("database")
            raise RuntimeError("database close failed")

    class Bus:
        async def close(self):
            events.append("bus")

    with pytest.raises(RuntimeError, match="database close failed"):
        await _close_local_resources(Database(), Bus())
    assert events == ["database", "bus"]


def test_local_app_exposes_lazy_owned_bus_for_resource_probe(tmp_path, monkeypatch):
    import json
    from starlette.applications import Starlette
    from a2a_mq.local_lite import create_app

    app_token = tmp_path / "app-token"
    dapr_token = tmp_path / "dapr-token"
    keys = tmp_path / "keys.json"
    for path, value in ((app_token, "a" * 32), (dapr_token, "d" * 32)):
        path.write_text(value)
        path.chmod(0o600)
    keys.write_text(json.dumps({"local": "unused-fixture-key"}))
    keys.chmod(0o600)

    class Identity:
        audience = "http://127.0.0.1:18000"
        issuer = "https://local-a2amq.invalid"

    monkeypatch.setattr("a2a_mq.local_lite.verifier", lambda settings, audience=None: Identity())
    monkeypatch.setattr("a2a_mq.local_lite.create_bridge", lambda *a, **k: Starlette())
    settings = LocalLiteSettings(
        sqlite_path=str(tmp_path / "local.db"),
        app_token_file=str(app_token),
        dapr_token_file=str(dapr_token),
        jwt_keys_file=str(keys),
        issuer="https://local-a2amq.invalid",
    )
    app = create_app(settings)
    assert app.state.a2amq_bus._owns_client is True
    assert app.state.a2amq_bus.client is None
