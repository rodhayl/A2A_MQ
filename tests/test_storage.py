"""Real SQLite reference contract tests. No mock domain and no model calls."""
import itertools
import multiprocessing
import os
import sqlite3
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path
from threading import Barrier
from uuid import NAMESPACE_URL, uuid5

import pytest

from a2a_mq.domain import Envelope, MAX_BYTES, PermanentError, RetryableError, command, snapshot
from a2a_mq.storage import SQLiteStore, StoreScope

NOW = datetime(2026, 9, 17, tzinfo=timezone.utc).timestamp()


def request(n=1, tenant="demo", text="hello"):
    e = command(text, tenant).as_dict()
    e.update(messageId=str(uuid5(NAMESPACE_URL, f"command/{n}")), correlationId=f"correlation-{n}",
             createdAt="2026-09-17T00:00:00Z")
    e["payload"]["message"]["messageId"] = f"inner-{n}"
    return Envelope.from_dict(e)


def result(req=None, sequence=1, state="COMPLETED", task_id="task-1", source="echo-worker", context="ctx-1"):
    req = req or request()
    e = snapshot(req, {"id": task_id, "contextId": context,
                       "status": {"state": "TASK_STATE_" + state},
                       "artifacts": [{"artifactId": "output", "parts": [{"text": "hello"}]}]},
                 sequence, source, str(uuid5(NAMESPACE_URL, f"event/{req.digest}/{task_id}/{sequence}/{state}")))
    d = e.as_dict()
    d["createdAt"] = "2026-09-17T00:00:00Z"
    return Envelope.from_dict(d)


@pytest.fixture
def store(tmp_path):
    return SQLiteStore(tmp_path / "state.db", tenant="demo", consumer="workers", clock=lambda: NOW)


def complete(store, req=None, event=None, key="effect-1"):
    req = req or request()
    claim = store.claim(req, "worker-1")
    store.commit_claim(claim, effect_key=key, effect={"echo": "hello"}, result=event or result(req))
    return claim


def counts(store):
    with closing(sqlite3.connect(store.path)) as db, db:
        return {name: db.execute(f"SELECT count(*) FROM a2amq_{name} WHERE tenant=? AND consumer=?",
                                (store.scope.tenant, store.scope.consumer)).fetchone()[0]
                for name in ("inbox", "effects", "tasks", "outbox")}


def test_real_commit_restart_and_duplicate(store):
    complete(store)
    reopened = SQLiteStore(store.path, tenant="demo", consumer="workers", clock=lambda: NOW)
    duplicate = reopened.claim(request(), "another-worker")
    assert duplicate.status == "completed"
    assert duplicate.result == result()
    assert reopened.get_task("task-1") == result().as_dict()["payload"]
    assert reopened.get_effect("effect-1") == {"echo": "hello"}
    assert counts(reopened) == dict(inbox=1, effects=1, tasks=1, outbox=1)
    with closing(sqlite3.connect(store.path)) as db, db:
        assert db.execute("PRAGMA journal_mode").fetchone()[0] == "wal"
    with store._connection() as db:
        assert db.execute("PRAGMA synchronous").fetchone()[0] == 2


@pytest.mark.parametrize("tenant,consumer", [("other", "workers"), ("demo", "observer")])
def test_all_tables_isolated_by_tenant_and_consumer(store, tenant, consumer):
    complete(store)
    other = SQLiteStore(store.path, tenant=tenant, consumer=consumer, clock=lambda: NOW)
    assert other.get_task("task-1") is None
    assert other.get_effect("effect-1") is None
    assert other.pending_outbox() == []
    assert other.list_tasks() == ([], None)
    req = request(tenant=tenant)
    complete(other, req=req, event=result(req))
    assert counts(other) == counts(store) == dict(inbox=1, effects=1, tasks=1, outbox=1)


def test_two_independent_projections_same_database(store):
    a = SQLiteStore(store.path, tenant="demo", consumer="observer-a", clock=lambda: NOW)
    b = SQLiteStore(store.path, tenant="demo", consumer="observer-b", clock=lambda: NOW)
    event = result()
    assert a.apply_snapshot(event)
    assert b.get_task("task-1") is None
    assert b.apply_snapshot(event)
    assert not a.apply_snapshot(event)
    assert a.get_task("task-1") == b.get_task("task-1")
    assert store.get_task("task-1") is None


def test_lease_expiry_fence_and_renew(store):
    clock = [NOW]
    store.clock = lambda: clock[0]
    first = store.claim(request(), "worker-1", lease_seconds=10)
    assert first.fence == 1 and first.status == "acquired"
    assert store.claim(request(), "worker-1").status == "busy"
    clock[0] += 5
    store.renew(first, lease_seconds=10)
    clock[0] += 6
    assert store.claim(request(), "worker-2").status == "busy"
    clock[0] += 4  # exactly at expiration
    with pytest.raises(RetryableError, match="LEASE_EXPIRED"):
        store.commit_claim(first, effect_key="key", effect=1, result=result())
    with pytest.raises(RetryableError, match="LEASE_EXPIRED"):
        store.renew(first)
    second = store.claim(request(), "worker-2")
    assert second.fence == 2
    with pytest.raises(RetryableError, match="STALE_FENCE"):
        store.commit_claim(first, effect_key="key", effect=1, result=result())
    store.commit_claim(second, effect_key="key", effect=2, result=result())
    assert store.get_effect("key") == 2


def test_renew_cannot_shorten_lease(store):
    claim = store.claim(request(), "w", lease_seconds=60)
    store.renew(claim, lease_seconds=1)
    store.clock = lambda: NOW + 30
    assert store.claim(request(), "other").status == "busy"


@pytest.mark.parametrize("mutator", [
    lambda c: replace(c, scope=StoreScope("other", "workers")),
    lambda c: replace(c, scope=StoreScope("demo", "other")),
    lambda c: replace(c, owner="other"),
    lambda c: replace(c, message_id="absent"),
    lambda c: replace(c, digest="changed"),
    lambda c: replace(c, status="busy"),
])
def test_wrong_claim_cannot_commit(store, mutator):
    claim = mutator(store.claim(request(), "w"))
    with pytest.raises(PermanentError, match="CLAIM_NOT_OWNED"):
        store.commit_claim(claim, effect_key="key", effect=1, result=result())
    assert counts(store) == dict(inbox=1, effects=0, tasks=0, outbox=0)


@pytest.mark.parametrize("finished", [False, True])
def test_changed_message_id_content_is_permanent(store, finished):
    if finished:
        complete(store)
    else:
        store.claim(request(), "w")
    with pytest.raises(PermanentError, match="MESSAGE_ID_CONFLICT"):
        store.claim(request(text="different"), "w")


@pytest.mark.parametrize("field,value", [("causationId", "other"), ("correlationId", "other")])
def test_result_bound_to_command(store, field, value):
    claim = store.claim(request(), "w")
    d = result().as_dict()
    d[field] = value
    with pytest.raises(PermanentError, match="RESULT_COMMAND_MISMATCH"):
        store.commit_claim(claim, effect_key="key", effect=1, result=Envelope.from_dict(d))
    assert store.get_effect("key") is None


@pytest.mark.parametrize("field", ["taskId", "contextId"])
def test_result_honors_payload_identifiers_even_without_outer_ids(store, field):
    d = request().as_dict()
    d["payload"]["message"][field] = "requested-id"
    req = Envelope.from_dict(d)
    claim = store.claim(req, "w")
    with pytest.raises(PermanentError, match="RESULT_COMMAND_MISMATCH"):
        store.commit_claim(claim, effect_key="key", effect=1, result=result(req))


def test_effect_and_task_rollback_when_outbox_conflicts(store):
    complete(store)
    req = request(2)
    event = result(req, task_id="task-2").as_dict()
    event["messageId"] = result().as_dict()["messageId"]
    claim = store.claim(req, "w")
    with pytest.raises(PermanentError, match="OUTBOX_ID_CONFLICT"):
        store.commit_claim(claim, effect_key="second-effect", effect=2, result=Envelope.from_dict(event))
    assert store.get_effect("second-effect") is None
    assert store.get_task("task-2") is None
    assert store.claim(req, "other").status == "busy"
    assert counts(store) == dict(inbox=2, effects=1, tasks=1, outbox=1)


def test_rollback_when_effect_conflicts(store):
    complete(store)
    req = request(2)
    claim = store.claim(req, "w")
    with pytest.raises(PermanentError, match="EFFECT_CONFLICT"):
        store.commit_claim(claim, effect_key="effect-1", effect=999, result=result(req, task_id="task-2"))
    assert counts(store) == dict(inbox=2, effects=1, tasks=1, outbox=1)


def test_same_effect_key_same_value_not_inserted_twice(store):
    complete(store)
    req = request(2)
    complete(store, req, result(req, task_id="task-2"))
    assert counts(store) == dict(inbox=2, effects=1, tasks=2, outbox=2)


@pytest.mark.parametrize("crash", [RuntimeError, KeyboardInterrupt, SystemExit])
def test_rollback_even_baseexception_after_effect_insert(store, monkeypatch, crash):
    claim = store.claim(request(), "w")
    def fail(db, event):
        assert db.execute("SELECT count(*) FROM a2amq_effects").fetchone()[0] == 1
        raise crash("injected")
    monkeypatch.setattr(store, "_project", fail)
    with pytest.raises(crash):
        store.commit_claim(claim, effect_key="key", effect=1, result=result())
    assert counts(store) == dict(inbox=1, effects=0, tasks=0, outbox=0)


@pytest.mark.parametrize("order", list(itertools.permutations([1, 2, 3])))
def test_projection_converges_under_reordering(store, order):
    events = {n: result(sequence=n, state=s) for n, s in enumerate(["SUBMITTED", "WORKING", "COMPLETED"], 1)}
    for n in order:
        store.apply_snapshot(events[n])
    assert store.get_task("task-1")["status"]["state"] == "TASK_STATE_COMPLETED"
    assert counts(store) == dict(inbox=3, effects=0, tasks=1, outbox=0)


@pytest.mark.parametrize("terminal", ["COMPLETED", "FAILED", "CANCELED", "REJECTED"])
def test_terminal_snapshot_cannot_be_rewritten(store, terminal):
    store.apply_snapshot(result(state=terminal))
    with pytest.raises(PermanentError, match="TERMINAL_TASK_REWRITE"):
        store.apply_snapshot(result(sequence=2, state=terminal))
    assert counts(store)["inbox"] == 1


@pytest.mark.parametrize("change,code", [("source", "TASK_OWNER_CONFLICT"),
                                         ("context", "TASK_CONTEXT_CONFLICT"),
                                         ("payload", "TASK_VERSION_CONFLICT")])
def test_conflicting_snapshot_rejected_atomically(store, change, code):
    original = result(state="WORKING")
    store.apply_snapshot(original)
    d = original.as_dict()
    d["messageId"] = str(uuid5(NAMESPACE_URL, "conflict"))
    if change == "source":
        d["source"] = "other"
    elif change == "context":
        d["contextId"] = d["payload"]["contextId"] = "other"
    else:
        d["payload"]["artifacts"] = []
    with pytest.raises(PermanentError, match=code):
        store.apply_snapshot(Envelope.from_dict(d))
    assert store.get_task("task-1") == original.as_dict()["payload"]
    assert counts(store)["inbox"] == 1


def test_same_version_same_payload_new_event_is_idempotent(store):
    event = result()
    store.apply_snapshot(event)
    d = event.as_dict()
    d["messageId"] = str(uuid5(NAMESPACE_URL, "second-event"))
    assert not store.apply_snapshot(Envelope.from_dict(d))
    assert counts(store)["inbox"] == 2


def test_conflicting_event_id_rejected(store):
    event = result()
    store.apply_snapshot(event)
    d = event.as_dict()
    d["payload"]["artifacts"] = []
    with pytest.raises(PermanentError, match="MESSAGE_ID_CONFLICT"):
        store.apply_snapshot(Envelope.from_dict(d))


def test_stale_executor_result_rolls_back_effect(store):
    store.apply_snapshot(result(sequence=2, state="WORKING"))
    with pytest.raises(PermanentError, match="STALE_TASK_RESULT"):
        complete(store)
    assert store.get_effect("effect-1") is None
    assert store.pending_outbox() == []


def test_projection_rejects_unreachable_state(store):
    store.apply_snapshot(result(state="WORKING"))
    with pytest.raises(PermanentError, match="INVALID_TASK_TRANSITION"):
        store.apply_snapshot(result(sequence=2, state="SUBMITTED"))


def test_outbox_preserves_wire_and_republication_after_restart(store):
    complete(store)
    item = store.pending_outbox()[0]
    with pytest.raises(PermanentError, match="OUTBOX_NOT_ATTEMPTED"):
        store.mark_published(item)
    assert store.record_publish_attempt(item)
    reopened = SQLiteStore(store.path, tenant="demo", consumer="workers", clock=lambda: NOW)
    retried = reopened.pending_outbox()[0]
    assert retried.envelope.wire == item.envelope.wire
    assert retried.attempts == 1
    assert reopened.record_publish_attempt(retried)
    reopened.mark_published(retried)
    assert reopened.pending_outbox() == []
    assert not reopened.record_publish_attempt(retried)
    reopened.clock = lambda: NOW + 20
    reopened.mark_published(retried)
    with closing(sqlite3.connect(store.path)) as db, db:
        assert db.execute("SELECT published_at FROM a2amq_outbox").fetchone()[0] == NOW


@pytest.mark.parametrize("change", ["scope", "payload", "id"])
def test_outbox_rejects_wrong_item(store, change):
    complete(store)
    item = store.pending_outbox()[0]
    if change == "scope":
        item = replace(item, scope=StoreScope("demo", "other"))
    else:
        d = item.envelope.as_dict()
        if change == "payload":
            d["payload"]["artifacts"] = []
        else:
            d["messageId"] = str(uuid5(NAMESPACE_URL, "other-event"))
        item = replace(item, envelope=Envelope.from_dict(d))
    for operation in (store.record_publish_attempt, store.mark_published):
        with pytest.raises(PermanentError, match="OUTBOX_"):
            operation(item)
    assert len(store.pending_outbox()) == 1


def test_pagination_filters_and_defensive_copies(store):
    for i in range(5):
        store.apply_snapshot(result(task_id=f"task-{i}", context=f"ctx-{i % 2}",
                                    state="WORKING" if i % 2 else "COMPLETED"))
    page, cursor = store.list_tasks(limit=2)
    assert [t["id"] for t in page] == ["task-0", "task-1"] and cursor == "task-1"
    second, cursor = store.list_tasks(after=cursor, limit=2)
    last, cursor = store.list_tasks(after=cursor, limit=2)
    assert [t["id"] for t in second + last] == ["task-2", "task-3", "task-4"] and cursor is None
    assert len(store.list_tasks(context_id="ctx-0", state="COMPLETED")[0]) == 3
    assert store.list_tasks(context_id="' OR 1=1 --")[0] == []
    page[0]["artifacts"].clear()
    assert store.get_task("task-0")["artifacts"]


@pytest.mark.parametrize("limit", [0, -1, 201, True, 1.0, "1", None])
def test_invalid_page_sizes(store, limit):
    for operation in (store.pending_outbox, store.list_tasks):
        with pytest.raises(PermanentError, match="INVALID_PAGE_SIZE"):
            operation(limit=limit)


@pytest.mark.parametrize("lease", [0, -1, 3601, float("nan"), float("inf"), True, None, "1"])
def test_invalid_leases(store, lease):
    with pytest.raises(PermanentError, match="INVALID_LEASE"):
        store.claim(request(), "w", lease_seconds=lease)


@pytest.mark.parametrize("now", [float("nan"), float("inf"), -1, True, None, "1", 10**1000])
def test_invalid_clock(store, now):
    store.clock = lambda: now
    with pytest.raises(PermanentError, match="INVALID_TIME"):
        store.claim(request(), "w")


@pytest.mark.parametrize("key", ["", "x" * 257, "\x00", "\ud800", None, 5])
def test_invalid_storage_keys(store, key):
    for operation in (store.get_task, store.get_effect):
        with pytest.raises(PermanentError, match="INVALID_STORAGE_KEY"):
            operation(key)


@pytest.mark.parametrize("path", ["", ":memory:", "file:state.db?mode=memory"])
def test_reject_nonpersistent_paths(path):
    with pytest.raises(PermanentError, match="INVALID_STORAGE_PATH"):
        SQLiteStore(path, tenant="demo", consumer="workers")


@pytest.mark.parametrize("timeout", [0, -1, 60001, True, 1.2, None])
def test_invalid_sqlite_timeout(tmp_path, timeout):
    with pytest.raises(PermanentError, match="INVALID_STORAGE_TIMEOUT"):
        SQLiteStore(tmp_path / "db", tenant="demo", consumer="w", busy_timeout_ms=timeout)


def test_reject_legacy_schema_without_migrating_unknown_ownership(tmp_path):
    path = tmp_path / "old.db"
    with closing(sqlite3.connect(path)) as db, db:
        db.execute("CREATE TABLE inbox (message_id TEXT)")
    with pytest.raises(PermanentError, match="LEGACY_UNSCOPED_STORAGE"):
        SQLiteStore(path, tenant="demo", consumer="workers")
    with closing(sqlite3.connect(path)) as db, db:
        assert db.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall() == [("inbox",)]


def test_future_version_rejected_without_creating_tables(tmp_path):
    path = tmp_path / "future.db"
    with closing(sqlite3.connect(path)) as db, db:
        db.execute("CREATE TABLE a2amq_schema(singleton INTEGER PRIMARY KEY,version INTEGER)")
        db.execute("INSERT INTO a2amq_schema VALUES (1,999)")
    with pytest.raises(PermanentError, match="UNSUPPORTED_SCHEMA_VERSION"):
        SQLiteStore(path, tenant="demo", consumer="workers")
    with closing(sqlite3.connect(path)) as db, db:
        assert db.execute("SELECT count(*) FROM sqlite_master WHERE type='table'").fetchone()[0] == 1


def test_missing_table_is_not_silently_recreated(store):
    with closing(sqlite3.connect(store.path)) as db, db:
        db.execute("DROP TABLE a2amq_outbox")
    with pytest.raises(PermanentError, match="INCOMPLETE_STORAGE_SCHEMA"):
        SQLiteStore(store.path, tenant="demo", consumer="workers")


def test_expired_new_command_rejected_but_done_redelivery_accepted(store):
    d = request().as_dict()
    d["expiresAt"] = "2026-09-17T00:00:01Z"
    req = Envelope.from_dict(d)
    complete(store, req, result(req))
    store.clock = lambda: NOW + 1
    assert store.claim(req, "w").status == "completed"
    d["messageId"] = str(uuid5(NAMESPACE_URL, "expired-new"))
    with pytest.raises(PermanentError, match="EXPIRED"):
        store.claim(Envelope.from_dict(d), "w")


def test_busy_database_is_retryable(store):
    contender = SQLiteStore(store.path, tenant="demo", consumer="workers", busy_timeout_ms=10)
    with store._transaction():
        with pytest.raises(RetryableError, match="STORAGE_BUSY"):
            contender.claim(request(), "w")


def test_thread_replicas_have_one_winner(store):
    barrier = Barrier(8)
    def claim(n):
        barrier.wait(timeout=10)
        return store.claim(request(), f"worker-{n}").status
    with ThreadPoolExecutor(max_workers=8) as pool:
        outcomes = list(pool.map(claim, range(8)))
    assert outcomes.count("acquired") == 1 and outcomes.count("busy") == 7


def _process_claim(path, wire, start, output):
    store = SQLiteStore(path, tenant="demo", consumer="workers", clock=lambda: NOW)
    start.wait(10)
    output.put(store.claim(Envelope(wire), str(os.getpid())).status)


def test_process_replicas_have_one_winner(store):
    ctx = multiprocessing.get_context("spawn")
    start, output = ctx.Event(), ctx.Queue()
    processes = [ctx.Process(target=_process_claim, args=(store.path, request().wire, start, output))
                 for _ in range(4)]
    try:
        for p in processes:
            p.start()
        start.set()
        statuses = [output.get(timeout=20) for _ in processes]
        for p in processes:
            p.join(timeout=10)
            assert p.exitcode == 0
        assert statuses.count("acquired") == 1 and statuses.count("busy") == 3
    finally:
        for p in processes:
            if p.is_alive():
                p.terminate()
            p.join(timeout=5)
        output.close()
        output.join_thread()


@pytest.mark.parametrize("stage", ["claim", "effect", "commit"])
def test_abrupt_process_death_and_recovery(store, stage):
    code = '''
import os, sys
from a2a_mq.domain import Envelope
from a2a_mq.storage import SQLiteStore
store = SQLiteStore(sys.argv[1], tenant="demo", consumer="workers", clock=lambda: float(sys.argv[4]))
claim = store.claim(Envelope(sys.argv[2]), "crashing-worker", lease_seconds=1)
if sys.argv[5] == "claim":
    os._exit(17)
if sys.argv[5] == "effect":
    def crash(db, event):
        assert db.execute("SELECT count(*) FROM a2amq_effects").fetchone()[0] == 1
        os._exit(17)
    store._project = crash
store.commit_claim(claim, effect_key="crash-effect", effect=1, result=Envelope(sys.argv[3]))
os._exit(17)
'''
    env = {**os.environ, "PYTHONPATH": str(Path(__file__).resolve().parents[1] / "src")}
    child = subprocess.run([sys.executable, "-c", code, store.path, request().wire,
                            result().wire, str(NOW), stage], env=env, capture_output=True, timeout=15)
    assert child.returncode == 17, child.stderr.decode()
    store.clock = lambda: NOW + 2
    recovered = store.claim(request(), "recovering-worker")
    if stage == "commit":
        assert recovered.status == "completed" and recovered.result == result()
    else:
        assert counts(store) == dict(inbox=1, effects=0, tasks=0, outbox=0)
        assert recovered.status == "acquired" and recovered.fence == 2
        store.commit_claim(recovered, effect_key="crash-effect", effect=1, result=result())
    assert counts(store) == dict(inbox=1, effects=1, tasks=1, outbox=1)


@pytest.mark.parametrize("effect", [float("nan"), "x" * MAX_BYTES, {"x": [[[[[None]]]]]}])
def test_effect_bounds_or_valid_json(store, effect):
    claim = store.claim(request(), "w")
    if isinstance(effect, dict):
        store.commit_claim(claim, effect_key="k", effect=effect, result=result())
        assert store.get_effect("k") == effect
    else:
        with pytest.raises(PermanentError):
            store.commit_claim(claim, effect_key="k", effect=effect, result=result())
        assert counts(store)["effects"] == 0


def test_integral_float_sequence_rejected_by_storage(store):
    d = result().as_dict()
    d["sequence"] = 1.0
    e = Envelope.from_dict(d)
    with pytest.raises(PermanentError, match="INVALID_SEQUENCE"):
        store.apply_snapshot(e)


def test_tenant_and_operation_rejections(store):
    with pytest.raises(PermanentError, match="STORAGE_TENANT_MISMATCH"):
        store.claim(request(tenant="other"), "w")
    with pytest.raises(PermanentError, match="STORAGE_TENANT_MISMATCH"):
        store.apply_snapshot(result(request(tenant="other")))
    with pytest.raises(PermanentError, match="UNSUPPORTED_STORAGE_OPERATION"):
        store.claim(result(), "w")
    with pytest.raises(PermanentError, match="UNSUPPORTED_STORAGE_OPERATION"):
        store.apply_snapshot(request())
    with pytest.raises(PermanentError, match="INVALID_ENVELOPE"):
        store.claim({}, "w")
    with pytest.raises(PermanentError, match="INVALID_STATE"):
        store.list_tasks(state="unknown")


def test_missing_parent_directory_reports_safe_storage_error(tmp_path):
    with pytest.raises(RetryableError, match="^STORAGE_UNAVAILABLE$"):
        SQLiteStore(tmp_path / "missing" / "db", tenant="demo", consumer="workers")


def test_non_lock_database_error_is_sanitized(store):
    with pytest.raises(RetryableError, match="^STORAGE_UNAVAILABLE$"):
        with store._connection() as db:
            db.execute("SELECT * FROM a_table_that_does_not_exist")


def test_missing_schema_version_fails_closed(tmp_path):
    path = tmp_path / "db"
    with closing(sqlite3.connect(path)) as db, db:
        db.execute("CREATE TABLE a2amq_inbox(value TEXT)")
    with pytest.raises(PermanentError, match="MISSING_SCHEMA_VERSION"):
        SQLiteStore(path, tenant="demo", consumer="workers")
    with closing(sqlite3.connect(path)) as db:
        assert db.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall() == [("a2amq_inbox",)]


def test_invalid_scope_is_rejected_before_creating_database(tmp_path):
    path = tmp_path / "db"
    with pytest.raises(PermanentError, match="INVALID_ROUTE"):
        SQLiteStore(path, tenant="demo", consumer="a.b")
    assert not path.exists()


def test_cwd_change_does_not_redirect_existing_store(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    store = SQLiteStore("relative.db", tenant="demo", consumer="workers", clock=lambda: NOW)
    complete(store)
    other = tmp_path / "other"
    other.mkdir()
    monkeypatch.chdir(other)
    assert store.get_task("task-1") is not None
    assert not (other / "relative.db").exists()
