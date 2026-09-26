"""Adapter boundaries plus the real-PostgreSQL parity gate (explicit marker)."""
import os
from concurrent.futures import ThreadPoolExecutor
from types import SimpleNamespace
from uuid import uuid4

import pytest

from a2a_mq.domain import command, PermanentError
from a2a_mq.postgres import _Connection, _Row, open_store, scope_lock, PostgresStore
from a2a_mq.reference import echo_events
from a2a_mq.storage import StoreScope


def test_row_supports_named_and_positional_reads():
    row = _Row((7, "x"), ("number", "name"))
    assert row[0] == row["number"] == 7
    assert list(row) == [7, "x"]


def test_parameter_values_are_never_sql_interpolated():
    calls = []
    class Raw:
        def execute(self, sql, params):
            calls.append((sql, params))
            return SimpleNamespace(description=None)
    _Connection(Raw()).execute("SELECT * FROM table1 WHERE name=?", ("x' OR 1=1--?",))
    assert calls == [("SELECT * FROM table1 WHERE name=%s", ("x' OR 1=1--?",))]


def test_locks_are_stable_and_scoped():
    a = StoreScope("a", "bc")
    assert scope_lock(a) == scope_lock(a)
    assert scope_lock(a) != scope_lock(StoreScope("ab", "c"))
    assert scope_lock(a) != scope_lock(StoreScope("a", "b"))


@pytest.mark.parametrize("path,dsn", [(None, None), ("state", "postgresql://localhost/db")])
def test_exactly_one_backend(path, dsn):
    with pytest.raises(PermanentError, match="EXACTLY_ONE"):
        open_store(tenant="demo", consumer="w", path=path, dsn=dsn)


def test_sqlite_factory(tmp_path):
    store = open_store(path=str(tmp_path / "db"), tenant="demo", consumer="w")
    assert store.list_tasks() == ([], None)


@pytest.mark.postgres
def test_real_postgres_parity():
    # Selecting this required gate without prerequisites fails, rather than skips.
    dsn = os.environ.get("A2A_TEST_POSTGRES_DSN")
    assert dsn, "A2A_TEST_POSTGRES_DSN is required for the real PostgreSQL gate"
    consumer = "pg-" + uuid4().hex
    store = PostgresStore(dsn, tenant="tests", consumer=consumer, allow_insecure=True)
    peer = PostgresStore(dsn, tenant="tests", consumer=consumer, allow_insecure=True)
    try:
        req = command("real postgres", tenant="tests")
        with ThreadPoolExecutor(4) as pool:
            claims = list(pool.map(lambda n: (store if n % 2 else peer).claim(req, str(n)), range(4)))
        assert sum(c.status == "acquired" for c in claims) == 1
        winner = next(c for c in claims if c.status == "acquired")
        event = echo_events(req, consumer)[-1]
        store.commit_claim(winner, effect_key="effect", effect={"text": "real postgres"}, result=event)
        assert peer.claim(req, "reconnect").status == "completed"
        assert peer.get_effect("effect") == {"text": "real postgres"}
        item = peer.pending_outbox()[0]
        assert peer.record_publish_attempt(item)
        peer.mark_published(item)
        assert store.pending_outbox() == []
        assert len(store.list_tasks(limit=1)[0]) == 1
        other = PostgresStore(dsn, tenant="other", consumer=consumer, allow_insecure=True)
        try:
            assert other.list_tasks() == ([], None)
        finally:
            other.close()
        req2 = command("rollback", tenant="tests")
        c2 = store.claim(req2, "w")
        with pytest.raises(PermanentError, match="EFFECT_CONFLICT"):
            store.commit_claim(c2, effect_key="effect", effect="different", result=echo_events(req2)[-1])
        assert peer.claim(req2, "p").status == "busy"
    finally:
        peer.close()
        store.close()
