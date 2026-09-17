"""Recovery reads fail closed on inconsistent persisted outcomes and outbox rows."""
from contextlib import closing
import sqlite3

import pytest

from a2a_mq.domain import RetryableError, PermanentError, canonical
from test_storage import complete, request, result, store  # noqa: F401


@pytest.mark.parametrize("stored", [None, "{}", "broken", "OTHER_TENANT", "OTHER_COMMAND"])
def test_completed_claim_requires_consistent_saved_result(store, stored):
    complete(store)
    if stored == "OTHER_TENANT":
        stored = result(request(tenant="other")).wire
    elif stored == "OTHER_COMMAND":
        stored = result(request(2)).wire
    with closing(sqlite3.connect(store.path)) as db, db:
        db.execute("UPDATE a2amq_inbox SET result_wire=?", (stored,))
    with pytest.raises(RetryableError, match="^STORAGE_INTEGRITY_ERROR$"):
        store.claim(request(), "new-worker")


@pytest.mark.parametrize("column,value", [
    ("wire", "{}"), ("wire", "broken"), ("digest", "wrong"),
    ("event_id", "wrong"), ("wire", "OTHER_TENANT"), ("wire", "NONCANONICAL"),
])
def test_outbox_read_validates_identity_digest_and_scope(store, column, value):
    complete(store)
    if value == "OTHER_TENANT":
        value = result(request(tenant="other")).wire
    elif value == "NONCANONICAL":
        value = result().wire + "\n"
    with closing(sqlite3.connect(store.path)) as db, db:
        db.execute(f"UPDATE a2amq_outbox SET {column}=?", (value,))
    with pytest.raises(RetryableError, match="^STORAGE_INTEGRITY_ERROR$"):
        store.pending_outbox()


def test_completed_result_must_match_outbox(store):
    complete(store)
    d = result().as_dict()
    d["payload"]["artifacts"] = []
    with closing(sqlite3.connect(store.path)) as db, db:
        db.execute("UPDATE a2amq_inbox SET result_wire=?", (canonical(d),))
    with pytest.raises(RetryableError, match="^STORAGE_INTEGRITY_ERROR$"):
        store.claim(request(), "w")


def test_outbox_pages_can_advance_past_failed_publications(store):
    for n in range(5):
        complete(store, request(n), result(request(n), task_id=f"task-{n}"), key=f"effect-{n}")
    expected = sorted(item.envelope.as_dict()["messageId"] for item in store.pending_outbox())
    cursor = None
    seen = []
    while True:
        items = store.pending_outbox(limit=2, after=cursor)
        if not items:
            break
        seen.extend(item.envelope.as_dict()["messageId"] for item in items)
        cursor = seen[-1]
    assert seen == expected and len(set(seen)) == 5
    assert len(store.pending_outbox()) == 5  # reading advances no durable acknowledgment


@pytest.mark.parametrize("cursor", ["", 0, True, "\x00", "\ud800"])
def test_bad_outbox_cursor_rejected(store, cursor):
    with pytest.raises(PermanentError, match="INVALID_STORAGE_KEY"):
        store.pending_outbox(after=cursor)
