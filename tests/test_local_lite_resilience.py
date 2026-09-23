import hashlib
import importlib.util
from pathlib import Path
import sqlite3
import pytest

from a2a_mq.domain import PermanentError
from a2a_mq.sqlite_archive import backup_sqlite, restore_sqlite
from a2a_mq.backup import restore_sqlite_backup


def _gate_module():
    path = Path(__file__).resolve().parents[1] / "scripts/local_resilience_gate.py"
    spec = importlib.util.spec_from_file_location("local_resilience_gate", path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_resilience_stack_scrubs_parent_fault_environment(tmp_path, monkeypatch):
    gate = _gate_module()
    for name in gate.FAULT_ENV_NAMES:
        monkeypatch.setenv(name, "parent-value")
    tools = tmp_path / "bin"
    tools.mkdir()

    env = gate._stack_environment(tools, {"A2AMQ_FAULT_RECORD_FILE": "/tmp/explicit.jsonl"})

    assert env["A2AMQ_FAULT_RECORD_FILE"] == "/tmp/explicit.jsonl"
    for name in gate.FAULT_ENV_NAMES - {"A2AMQ_FAULT_RECORD_FILE"}:
        assert name not in env
    assert env["PATH"].split(":")[0] == str(tools)


def test_resilience_evidence_paths_must_be_fresh_and_distinct(tmp_path):
    gate = _gate_module()
    json_path = tmp_path / "result.json"
    log_path = tmp_path / "result.log"
    gate._prepare_evidence_paths(json_path, log_path)

    existing = tmp_path / "existing.json"
    existing.write_text("{}")
    with pytest.raises(PermanentError, match="EVIDENCE_OUTPUT_EXISTS"):
        gate._prepare_evidence_paths(existing, log_path)

    target = tmp_path / "target.log"
    target.write_text("target")
    link = tmp_path / "link.log"
    link.symlink_to(target)
    with pytest.raises(PermanentError, match="EVIDENCE_OUTPUT_EXISTS"):
        gate._prepare_evidence_paths(json_path, link)

    with pytest.raises(PermanentError, match="EVIDENCE_PATH_CONFLICT"):
        gate._prepare_evidence_paths(tmp_path / "same", tmp_path / "same")


def test_sqlite_integrity_checker_verifies_clean_schema(tmp_path):
    from a2a_mq.storage import SQLiteStore
    from a2a_mq.tasks import TaskLedger
    from a2a_mq.deadletters import DeadLetters
    db_path = tmp_path / "test.db"
    store = SQLiteStore(db_path, tenant="demo", consumer="local")
    TaskLedger(store)
    DeadLetters(store)
    with store._transaction() as conn:
        conn.execute("INSERT INTO a2amq_tasks VALUES ('demo', 'local', 't1', 'c1', 'test', 'COMPLETED', 1, '{}')")
        conn.execute("INSERT INTO a2amq_runtime_end VALUES ('demo', 'local', 't1', 1.0)")

    check = _gate_module()._check_sqlite_integrity(db_path)
    assert check["clean"] is True
    assert check["tasksCount"] == 1
    assert check["runtimeEndsCount"] == 1
    assert check["noDuplicateTerminalEnds"] is True
    assert check["endStatesConsistent"] is True


def test_sqlite_integrity_checker_detects_inconsistent_end_state(tmp_path):
    from a2a_mq.storage import SQLiteStore
    from a2a_mq.tasks import TaskLedger
    from a2a_mq.deadletters import DeadLetters
    db_path = tmp_path / "inconsistent.db"
    store = SQLiteStore(db_path, tenant="demo", consumer="local")
    TaskLedger(store)
    DeadLetters(store)
    with store._transaction() as conn:
        conn.execute("INSERT INTO a2amq_tasks VALUES ('demo', 'local', 't1', 'c1', 'test', 'WORKING', 1, '{}')")
        conn.execute("INSERT INTO a2amq_runtime_end VALUES ('demo', 'local', 't1', 1.0)")

    check = _gate_module()._check_sqlite_integrity(db_path)
    assert check["clean"] is False
    assert check["endStatesConsistent"] is False


def test_backup_restore_roundtrip_and_error_handling(tmp_path):
    from a2a_mq.storage import SQLiteStore
    db_path = tmp_path / "source.db"
    store = SQLiteStore(db_path, tenant="demo", consumer="test")
    with store._transaction() as conn:
        conn.execute("CREATE TABLE items (id INT, val TEXT)")
        conn.execute("INSERT INTO items VALUES (1, 'hello'), (2, 'world')")

    backup_dest = tmp_path / "dest.backup"
    info = backup_sqlite(db_path, backup_dest)
    assert backup_dest.is_file()
    assert "sha256" in info

    # Restoring to new destination
    restored_dest = tmp_path / "restored.db"
    restore_info = restore_sqlite_backup(backup_dest, restored_dest)
    assert restored_dest.is_file()
    assert restore_info["sha256"] == hashlib.sha256(restored_dest.read_bytes()).hexdigest()

    with sqlite3.connect(str(restored_dest)) as conn:
        rows = conn.execute("SELECT * FROM items ORDER BY id").fetchall()
        assert rows == [(1, 'hello'), (2, 'world')]

    # Restore fails on wrong expected sha
    with pytest.raises(PermanentError, match="BACKUP_DIGEST_MISMATCH"):
        restore_sqlite(backup_dest, tmp_path / "bad.db", expected_sha256="a" * 64)
