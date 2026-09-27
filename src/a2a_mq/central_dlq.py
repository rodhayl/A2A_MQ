"""Central tenant DLQ with origin-consumer provenance and two-phase replay."""
from __future__ import annotations

import base64
import hashlib
from typing import Any

from .domain import Envelope, PermanentError, RetryableError, load_json, require
from .identity import Principal
from .operations import Metrics
from .storage import _key, _page
from .tasks import stable_id

DDL = (
    "CREATE TABLE IF NOT EXISTS a2amq_central_dlq ("
    "tenant TEXT NOT NULL,consumer TEXT NOT NULL,entry_id TEXT NOT NULL,"
    "digest TEXT NOT NULL,raw_base64 TEXT NOT NULL,reason TEXT NOT NULL,"
    "origin_consumer TEXT NOT NULL,truncated BIGINT NOT NULL,"
    "captured_at DOUBLE PRECISION NOT NULL,attempts BIGINT NOT NULL DEFAULT 0,"
    "replayed_at DOUBLE PRECISION,PRIMARY KEY(tenant,consumer,entry_id))",
    "CREATE INDEX IF NOT EXISTS a2amq_central_dlq_pending ON "
    "a2amq_central_dlq(tenant,consumer,replayed_at,captured_at,entry_id)",
)


def _admin(principal: Principal, tenant: str) -> None:
    require(isinstance(principal, Principal) and principal.tenant == tenant
            and "a2a:admin" in principal.scopes, "FORBIDDEN")


def _reason(value: Any) -> str:
    require(isinstance(value, str) and 1 <= len(value) <= 128
            and all(32 <= ord(c) < 127 for c in value), "INVALID_DLQ_REASON")
    return value


def _replay_envelope(raw: bytes, tenant: str) -> Envelope:
    """Recover a replayable Envelope from the real Dapr delivery bytes or a legacy raw Envelope."""
    try:
        envelope = Envelope.parse(raw)
    except PermanentError:
        try:
            event = load_json(raw, 1024 * 1024)
            require(isinstance(event, dict) and event.get("specversion") == "1.0",
                    "DLQ_NOT_REPLAYABLE")
            mime = event.get("datacontenttype")
            require(isinstance(mime, str)
                    and mime.split(";", 1)[0].strip().lower() == "application/json",
                    "DLQ_NOT_REPLAYABLE")
            require(isinstance(event.get("pubsubname"), str) and bool(event["pubsubname"]),
                    "DLQ_NOT_REPLAYABLE")
            envelope = Envelope.from_dict(event.get("data"))
            value = envelope.as_dict()
            require(event.get("id") == value["messageId"]
                    and event.get("source") == "urn:a2a-mq:" + value["source"]
                    and event.get("type") == "a2a-mq." + value["operation"]
                    and event.get("topic") == envelope.route,
                    "DLQ_NOT_REPLAYABLE")
        except PermanentError:
            raise PermanentError("DLQ_NOT_REPLAYABLE") from None
    require(envelope.as_dict()["tenantId"] == tenant, "DLQ_TENANT_MISMATCH")
    envelope.check_expiry()
    return envelope


class CentralDLQ:
    """One operational consumer stores poison from all application consumers."""

    def __init__(self, store, *, metrics: Metrics | None = None) -> None:
        self.store, self.metrics = store, metrics or Metrics()
        with store._transaction() as db:
            for statement in DDL:
                db.execute(statement)

    @property
    def keys(self) -> tuple[str, str]:
        return self.store._keys

    def record_dlq(self, raw: bytes, reason: str, *, origin_consumer: str,
                   truncated: bool = False, captured_at: float | None = None) -> str:
        require(isinstance(raw, bytes) and 0 < len(raw) <= 1024 * 1024, "INVALID_DLQ_PAYLOAD")
        reason, origin = _reason(reason), _key(origin_consumer)
        require(type(truncated) is bool, "INVALID_DLQ_PAYLOAD")
        digest = hashlib.sha256(raw).hexdigest()
        entry_id = stable_id(*self.keys, "central-dlq", origin, digest, reason, str(truncated))
        encoded = base64.b64encode(raw).decode("ascii")
        now = self.store._now() if captured_at is None else captured_at
        require(type(now) in {int, float} and not isinstance(now, bool) and 0 <= now <= 253402297199,
                "INVALID_TIME")
        expected = (digest, encoded, reason, origin, int(truncated))
        with self.store._transaction() as db:
            row = db.execute("SELECT digest,raw_base64,reason,origin_consumer,truncated "
                             "FROM a2amq_central_dlq WHERE tenant=? AND consumer=? AND entry_id=?",
                             (*self.keys, entry_id)).fetchone()
            if row is None:
                db.execute("INSERT INTO a2amq_central_dlq "
                           "(tenant,consumer,entry_id,digest,raw_base64,reason,origin_consumer,truncated,captured_at) "
                           "VALUES (?,?,?,?,?,?,?,?,?)", (*self.keys, entry_id, *expected, now))
                self.metrics.inc("a2amq_dlq_captured_total")
            else:
                require(tuple(row) == expected, "DLQ_ID_CONFLICT")
        return entry_id

    def stats(self, principal: Principal) -> dict[str, int]:
        _admin(principal, self.store.scope.tenant)
        with self.store._connection() as db:
            count = db.execute("SELECT COUNT(*) FROM a2amq_central_dlq "
                               "WHERE tenant=? AND consumer=? AND replayed_at IS NULL", self.keys).fetchone()[0]
        return {"dlqPending": int(count)}

    def prometheus(self, principal: Principal) -> str:
        return self.metrics.prometheus({"a2amq_central_pending_dlq": self.stats(principal)["dlqPending"]})

    def list_dlq(self, principal: Principal, *, limit: int = 50, after: str | None = None,
                 include_replayed: bool = False) -> tuple[list[dict[str, Any]], str | None]:
        _admin(principal, self.store.scope.tenant)
        _page(limit)
        require(type(include_replayed) is bool, "INVALID_QUERY")
        clauses, params = ["tenant=?", "consumer=?"], list(self.keys)
        if not include_replayed:
            clauses.append("replayed_at IS NULL")
        if after is not None:
            clauses.append("entry_id>?")
            params.append(_key(after))
        with self.store._connection() as db:
            rows = db.execute(
                "SELECT entry_id,digest,reason,origin_consumer,truncated,captured_at,attempts,replayed_at "
                "FROM a2amq_central_dlq WHERE " + " AND ".join(clauses)
                + " ORDER BY entry_id LIMIT ?", (*params, limit + 1)).fetchall()
        selected = rows[:limit]
        items = [{"entryId": r["entry_id"], "digest": r["digest"], "reason": r["reason"],
                  "originConsumer": r["origin_consumer"], "truncated": bool(r["truncated"]),
                  "capturedAt": r["captured_at"], "attempts": int(r["attempts"]),
                  "replayedAt": r["replayed_at"]} for r in selected]
        return items, selected[-1]["entry_id"] if len(rows) > limit and selected else None

    def dlq_payload(self, principal: Principal, entry_id: str) -> bytes:
        _admin(principal, self.store.scope.tenant)
        entry_id = _key(entry_id)
        with self.store._connection() as db:
            row = db.execute("SELECT digest,raw_base64 FROM a2amq_central_dlq "
                             "WHERE tenant=? AND consumer=? AND entry_id=?",
                             (*self.keys, entry_id)).fetchone()
        require(row is not None, "DLQ_NOT_FOUND")
        try:
            raw = base64.b64decode(row["raw_base64"], validate=True)
        except (ValueError, TypeError):
            raise RetryableError("STORAGE_INTEGRITY_ERROR") from None
        if base64.b64encode(raw).decode("ascii") != row["raw_base64"] \
                or hashlib.sha256(raw).hexdigest() != row["digest"]:
            raise RetryableError("STORAGE_INTEGRITY_ERROR")
        return raw

    def replay_candidate(self, principal: Principal, entry_id: str) -> Envelope:
        raw = self.dlq_payload(principal, entry_id)
        entry_id = _key(entry_id)
        with self.store._connection() as db:
            row = db.execute("SELECT truncated FROM a2amq_central_dlq "
                             "WHERE tenant=? AND consumer=? AND entry_id=?",
                             (*self.keys, entry_id)).fetchone()
        require(row is not None, "DLQ_NOT_FOUND")
        require(not bool(row["truncated"]), "DLQ_NOT_REPLAYABLE")
        return _replay_envelope(raw, self.store.scope.tenant)

    def prepare_replay(self, principal: Principal, entry_id: str, *, force: bool = False) -> None:
        _admin(principal, self.store.scope.tenant)
        require(type(force) is bool, "INVALID_QUERY")
        entry_id = _key(entry_id)
        with self.store._transaction() as db:
            row = db.execute("SELECT replayed_at FROM a2amq_central_dlq "
                             "WHERE tenant=? AND consumer=? AND entry_id=?",
                             (*self.keys, entry_id)).fetchone()
            require(row is not None, "DLQ_NOT_FOUND")
            require(force or row["replayed_at"] is None, "DLQ_ALREADY_REPLAYED")
            db.execute("UPDATE a2amq_central_dlq SET attempts=attempts+1 "
                       "WHERE tenant=? AND consumer=? AND entry_id=?", (*self.keys, entry_id))
        self.metrics.inc("a2amq_dlq_replay_attempts_total")

    def mark_replayed(self, principal: Principal, entry_id: str) -> None:
        _admin(principal, self.store.scope.tenant)
        entry_id = _key(entry_id)
        with self.store._transaction() as db:
            row = db.execute("SELECT attempts FROM a2amq_central_dlq "
                             "WHERE tenant=? AND consumer=? AND entry_id=?",
                             (*self.keys, entry_id)).fetchone()
            require(row is not None, "DLQ_NOT_FOUND")
            require(row["attempts"] > 0, "DLQ_NOT_ATTEMPTED")
            db.execute("UPDATE a2amq_central_dlq SET replayed_at=COALESCE(replayed_at,?) "
                       "WHERE tenant=? AND consumer=? AND entry_id=?",
                       (self.store._now(), *self.keys, entry_id))
        self.metrics.inc("a2amq_dlq_replayed_total")

    async def replay_dlq(self, principal: Principal, entry_id: str, publisher, *, force: bool = False) -> Envelope:
        envelope = self.replay_candidate(principal, entry_id)
        self.prepare_replay(principal, entry_id, force=force)
        await publisher.publish(envelope)
        self.mark_replayed(principal, entry_id)
        return envelope
