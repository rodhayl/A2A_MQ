"""Real PostgreSQL gate. A2A_TEST_POSTGRES_DSN must point to a disposable database."""
import os
from concurrent.futures import ThreadPoolExecutor
from threading import Barrier
from uuid import uuid4

import pytest

from a2a_mq.domain import command
from a2a_mq.postgres import PostgresStore
from a2a_mq.reference import echo_events

pytestmark = pytest.mark.postgres


def dsn():
    value = os.getenv("A2A_TEST_POSTGRES_DSN")
    if not value:
        pytest.fail("A2A_TEST_POSTGRES_DSN is required for the postgres gate")
    return value


def scope():
    return "pg-" + uuid4().hex[:16]


def test_real_postgres_claim_commit_outbox_restart_and_consumer_isolation():
    consumer = scope()
    store = PostgresStore(dsn(), tenant="demo", consumer=consumer, allow_insecure=True)
    try:
        request = command("postgres parity", tenant="demo", skill="echo")
        claim = store.claim(request, "worker-a")
        assert claim.status == "acquired"
        result = echo_events(request)[-1]
        store.commit_claim(claim, effect_key="effect", effect={"ok": True}, result=result)
        duplicate = store.claim(request, "worker-b")
        assert duplicate.status == "completed" and duplicate.result == result
        assert store.get_effect("effect") == {"ok": True}
        pending = store.pending_outbox()
        assert len(pending) == 1 and pending[0].envelope == result
        assert store.record_publish_attempt(pending[0])
        store.mark_published(pending[0])
        assert store.pending_outbox() == []
    finally:
        store.close()

    reopened = PostgresStore(dsn(), tenant="demo", consumer=consumer, allow_insecure=True)
    other = PostgresStore(dsn(), tenant="demo", consumer=scope(), allow_insecure=True)
    try:
        assert reopened.get_task(result.as_dict()["taskId"]) == result.as_dict()["payload"]
        assert other.get_task(result.as_dict()["taskId"]) is None
    finally:
        reopened.close(); other.close()


def test_real_postgres_replicas_serialize_same_command_claim():
    consumer = scope()
    first = PostgresStore(dsn(), tenant="demo", consumer=consumer, allow_insecure=True)
    second = PostgresStore(dsn(), tenant="demo", consumer=consumer, allow_insecure=True)
    request = command("race", tenant="demo", skill="echo")
    barrier = Barrier(2)

    def claim(store, owner):
        barrier.wait(timeout=10)
        return store.claim(request, owner).status

    try:
        with ThreadPoolExecutor(max_workers=2) as pool:
            outcomes = list(pool.map(lambda pair: claim(*pair), [(first, "a"), (second, "b")]))
        assert sorted(outcomes) == ["acquired", "busy"]
    finally:
        first.close(); second.close()
