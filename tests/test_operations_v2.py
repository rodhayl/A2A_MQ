"""Central DLQ provenance, two-phase replay, and offline restore contracts."""
import pytest

from a2a_mq.backup import restore_sqlite_backup
from a2a_mq.central_dlq import CentralDLQ
from a2a_mq.domain import PermanentError, command
from a2a_mq.identity import Principal
from a2a_mq.operations import OperationsManager
from a2a_mq.storage import SQLiteStore
from a2a_mq.tasks import TaskLedger


@pytest.fixture
def setup(tmp_path):
    store = SQLiteStore(tmp_path / "state.db", tenant="demo", consumer="operations", clock=lambda: 1000.0)
    dlq = CentralDLQ(store)
    admin = Principal("operator", "demo", frozenset({"a2a:admin"}))
    return store, dlq, admin


def test_dlq_preserves_origin_and_split_replay_state(setup):
    store, dlq, admin = setup
    raw = command("hello", tenant="demo", skill="echo").wire.encode()
    entry = dlq.record_dlq(raw, "HANDLER_FAILED", origin_consumer="observer-a", truncated=True)
    item = dlq.list_dlq(admin)[0][0]
    assert item["originConsumer"] == "observer-a" and item["truncated"] is True
    dlq.prepare_replay(admin, entry)
    attempted = dlq.list_dlq(admin)[0][0]
    assert attempted["attempts"] == 1 and attempted["replayedAt"] is None
    dlq.mark_replayed(admin, entry)
    replayed = dlq.list_dlq(admin, include_replayed=True)[0][0]
    assert replayed["replayedAt"] == 1000.0


def test_verified_sqlite_backup_restores_to_new_file(setup, tmp_path):
    store, dlq, admin = setup
    operations = OperationsManager(TaskLedger(store, source="workflow-worker"))
    backup = tmp_path / "backup.sqlite3"
    operations.backup_sqlite(admin, backup)
    restored = tmp_path / "restored.sqlite3"
    result = restore_sqlite_backup(backup, restored)
    assert result == OperationsManager.verify_sqlite_backup(restored)
    reopened = SQLiteStore(restored, tenant="demo", consumer="operations", clock=lambda: 1000.0)
    assert reopened.scope.consumer == "operations"


def test_legacy_backup_and_restore_refuse_dangling_symlink_destinations(setup, tmp_path):
    store, _, admin = setup
    operations = OperationsManager(TaskLedger(store, source="workflow-worker"))

    backup_link = tmp_path / "backup-link.sqlite3"
    backup_target = tmp_path / "unexpected-backup-target.sqlite3"
    backup_link.symlink_to(backup_target)
    with pytest.raises(PermanentError):
        operations.backup_sqlite(admin, backup_link)
    assert backup_link.is_symlink() and not backup_target.exists()

    source = tmp_path / "valid-backup.sqlite3"
    operations.backup_sqlite(admin, source)
    restore_link = tmp_path / "restore-link.sqlite3"
    restore_target = tmp_path / "unexpected-restore-target.sqlite3"
    restore_link.symlink_to(restore_target)
    with pytest.raises(PermanentError):
        restore_sqlite_backup(source, restore_link)
    assert restore_link.is_symlink() and not restore_target.exists()
