"""Operational retention, DLQ, metrics and backup contracts."""
from pathlib import Path
import pytest

from a2a_mq.domain import PermanentError, command
from a2a_mq.identity import Principal
from a2a_mq.operations import OperationsManager
from a2a_mq.storage import SQLiteStore
from a2a_mq.tasks import TaskLedger


@pytest.fixture
def setup(tmp_path):
    store = SQLiteStore(tmp_path / "state.db", tenant="demo", consumer="workflow", clock=lambda: 1000.0)
    ledger = TaskLedger(store)
    ops = OperationsManager(ledger)
    admin = Principal("operator", "demo", frozenset({"a2a:admin"}))
    user = Principal("alice", "demo", frozenset({"a2a:send", "a2a:read"}))
    return store, ledger, ops, admin, user


class Publisher:
    def __init__(self):
        self.items = []
    async def publish(self, envelope):
        self.items.append(envelope)


async def test_dlq_is_deduplicated_verified_and_replayed(setup):
    store, ledger, ops, admin, user = setup
    raw = command("hello", tenant="demo", skill="echo").wire.encode()
    entry = ops.record_dlq(raw, "INVALID_PAYLOAD", captured_at=10)
    assert ops.record_dlq(raw, "INVALID_PAYLOAD", captured_at=11) == entry
    rows, cursor = ops.list_dlq(admin)
    assert cursor is None and rows[0]["attempts"] == 0 and ops.dlq_payload(admin, entry) == raw
    publisher = Publisher()
    envelope = await ops.replay_dlq(admin, entry, publisher)
    assert publisher.items == [envelope]
    rows, _ = ops.list_dlq(admin, include_replayed=True)
    assert rows[0]["attempts"] == 1 and rows[0]["replayedAt"] == 1000.0


def _terminal_job(ledger, store, user):
    task = ledger.submit(user, {"message": {"messageId": "request-1", "role": "ROLE_USER",
                                                    "parts": [{"text": "hello"}]}})
    task_id = task["id"]
    ledger.step(task_id, "working", "WORKING")
    ledger.step(task_id, "completed", "COMPLETED",
                artifacts=[{"artifactId": "result", "parts": [{"text": "hello"}]}])
    for action in ledger.pending(("publish",), limit=50):
        ledger.delivered(action)
    with store._transaction() as db:
        db.execute("INSERT INTO a2amq_runtime_end VALUES (?,?,?,?)", (*ledger.keys, task_id, 100.0))
    return task_id


def test_retention_waits_for_outbox_then_preserves_tombstone(setup):
    store, ledger, ops, admin, user = setup
    task_id = _terminal_job(ledger, store, user)
    assert ops.retire(admin, retention_seconds=10) == []
    for item in store.pending_outbox(limit=50):
        assert store.record_publish_attempt(item)
        store.mark_published(item)
    assert ops.retire(admin, retention_seconds=10) == [task_id]
    assert store.get_task(task_id) is None
    with store._connection() as db:
        row = db.execute("SELECT task_id,subject,request_id FROM a2amq_retired "
                         "WHERE tenant=? AND consumer=?", ledger.keys).fetchone()
    assert tuple(row) == (task_id, "alice", "request-1")


def test_metrics_and_sqlite_backup_are_machine_verifiable(setup, tmp_path):
    store, ledger, ops, admin, user = setup
    text = ops.prometheus(admin)
    assert "# TYPE a2amq_pending_outbox gauge" in text
    assert "# TYPE a2amq_dlq_captured_total counter" in text
    destination = tmp_path / "backup.sqlite3"
    created = ops.backup_sqlite(admin, destination)
    checked = OperationsManager.verify_sqlite_backup(destination)
    assert checked["sha256"] == created["sha256"]
    assert checked["bytes"] == created["bytes"] > 0
    assert checked["schemaVersion"] == created["schemaVersion"] == 1


def test_admin_operations_reject_non_admin(setup):
    store, ledger, ops, admin, user = setup
    for call in (lambda: ops.stats(user), lambda: ops.list_dlq(user),
                 lambda: ops.backup_sqlite(user, Path(store.path).with_suffix(".copy"))):
        with pytest.raises(PermanentError, match="FORBIDDEN"):
            call()


@pytest.mark.parametrize("value", [float("nan"), float("inf"), -1, True, "1"])
def test_dlq_time_is_strict(setup, value):
    store, ledger, ops, admin, user = setup
    with pytest.raises(PermanentError, match="INVALID_TIME"):
        ops.record_dlq(b"x", "bad", captured_at=value)
