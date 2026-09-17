"""Durable DLQ archive and authorized, identity-preserving replay through Dapr.

Invalid/truncated/expired payloads stay inspectable but cannot be replayed. New
business intent is submitted through A2A, never by silently changing a DLQ ID.
"""
from __future__ import annotations

import asyncio
import base64
import hashlib
import re
from typing import Callable

from .schema_lifecycle import schema_transaction

from .domain import Envelope, RetryableError, canonical, load_json, require, token
from .storage import _key, _page
from .tasks import stable_id

MAX_DLQ_BYTES = 1048576
CE_FIELDS = {"specversion", "id", "source", "type", "time", "datacontenttype", "data", "traceparent", "tracestate"}
DDL = (
    "CREATE TABLE IF NOT EXISTS a2amq_delivery_dlq (tenant TEXT NOT NULL,consumer TEXT NOT NULL,"
    "record_id TEXT NOT NULL,digest TEXT NOT NULL,wire TEXT NOT NULL,reason TEXT NOT NULL,"
    "created_at DOUBLE PRECISION NOT NULL,PRIMARY KEY(tenant,consumer,record_id))",
    "CREATE TABLE IF NOT EXISTS a2amq_delivery_replays (tenant TEXT NOT NULL,consumer TEXT NOT NULL,"
    "replay_id TEXT NOT NULL,record_id TEXT NOT NULL,subject TEXT NOT NULL,route TEXT NOT NULL,"
    "wire TEXT NOT NULL,created_at DOUBLE PRECISION NOT NULL,published_at DOUBLE PRECISION,"
    "PRIMARY KEY(tenant,consumer,replay_id))",
)


def _hash(value: str) -> str:
    return hashlib.sha256(value.encode()).hexdigest()


class DeadLetters:
    def __init__(self, store, *, component: str = "bus", max_records: int = 10000):
        require(type(max_records) is int and 1 <= max_records <= 1000000, "INVALID_QUOTA")
        self.store, self.component, self.max_records = store, token(component), max_records
        self.topic = f"a2a.v1.{store.scope.tenant}.dlq"
        with schema_transaction(store) as db:
            for statement in DDL:
                db.execute(statement)

    def capture(self, event: dict) -> str:
        event = load_json(canonical(event), MAX_DLQ_BYTES)
        require(isinstance(event, dict) and event.get("specversion") == "1.0"
                and event.get("topic") == self.topic and event.get("pubsubname") == self.component,
                "INVALID_DLQ_DELIVERY")
        _key(event.get("id"))
        require(isinstance(event.get("source"), str) and isinstance(event.get("type"), str), "INVALID_CLOUDEVENT")
        # Delivery metadata varies on broker redelivery; archive stable CE fields.
        stable = {k: v for k, v in event.items() if k in CE_FIELDS}
        wire = canonical(stable)
        rid = stable_id(event["source"], event["id"])
        reason = "BROKER_DEADLETTER"
        if event["type"] == "a2a-mq.deadletter":
            data = event.get("data")
            require(isinstance(data, dict) and isinstance(data.get("reason"), str)
                    and re.fullmatch(r"[A-Z][A-Z0-9_]{0,63}", data["reason"]) is not None,
                    "INVALID_DLQ_REASON")
            reason = data["reason"]
        with self.store._transaction() as db:
            row = db.execute("SELECT digest FROM a2amq_delivery_dlq WHERE tenant=? AND consumer=? AND record_id=?",
                             (*self.store._keys, rid)).fetchone()
            if row is not None:
                require(row[0] == _hash(wire), "DLQ_ID_CONFLICT")
                return rid
            count = db.execute("SELECT COUNT(*) FROM a2amq_delivery_dlq WHERE tenant=? AND consumer=?",
                               self.store._keys).fetchone()[0]
            if count >= self.max_records:
                raise RetryableError("DLQ_QUOTA_EXCEEDED")
            db.execute("INSERT INTO a2amq_delivery_dlq VALUES (?,?,?,?,?,?,?)",
                       (*self.store._keys, rid, _hash(wire), wire, reason, self.store._now()))
        return rid

    def list(self, principal, *, after=None, limit=50):
        principal.authorize("operate", self.store.scope.tenant)
        _page(limit)
        extra, params = (" AND record_id>?", (*self.store._keys, _key(after))) if after else ("", self.store._keys)
        with self.store._connection() as db:
            rows = db.execute("SELECT record_id,reason,created_at FROM a2amq_delivery_dlq "
                              "WHERE tenant=? AND consumer=?"+extra+" ORDER BY record_id LIMIT ?",
                              (*params,limit+1)).fetchall()
        return {"records": [{"id": r[0], "reason": r[1], "createdAt": r[2]} for r in rows[:limit]],
                "nextCursor": rows[limit-1][0] if len(rows)>limit else None}

    def read(self, principal, record_id):
        principal.authorize("replay", self.store.scope.tenant)
        with self.store._connection() as db:
            row = db.execute("SELECT * FROM a2amq_delivery_dlq WHERE tenant=? AND consumer=? AND record_id=?",
                             (*self.store._keys, _key(record_id))).fetchone()
        require(row is not None, "DLQ_NOT_FOUND")
        if _hash(row["wire"]) != row["digest"]:
            raise RetryableError("STORAGE_INTEGRITY_ERROR")
        return load_json(row["wire"], MAX_DLQ_BYTES)

    def enqueue(self, principal, record_id, request_id, route, wire):
        principal.authorize("replay", self.store.scope.tenant)
        _key(request_id)
        rid = stable_id(*self.store._keys, principal.subject, record_id, request_id)
        with self.store._transaction() as db:
            prior = db.execute("SELECT wire,route FROM a2amq_delivery_replays WHERE tenant=? AND consumer=? AND replay_id=?",
                               (*self.store._keys, rid)).fetchone()
            if prior:
                require((prior[0],prior[1]) == (wire,route), "REPLAY_ID_CONFLICT")
            else:
                n = db.execute("SELECT COUNT(*) FROM a2amq_delivery_replays WHERE tenant=? AND consumer=? AND record_id=?",
                               (*self.store._keys,record_id)).fetchone()[0]
                require(n < 32, "REPLAY_LIMIT_EXCEEDED")
                db.execute("INSERT INTO a2amq_delivery_replays VALUES (?,?,?,?,?,?,?,?,NULL)",
                           (*self.store._keys,rid,record_id,principal.subject,route,wire,self.store._now()))
        return rid

    def pending(self, after=None, limit=50):
        _page(limit)
        extra, params = (" AND replay_id>?", (*self.store._keys,_key(after))) if after else ("",self.store._keys)
        with self.store._connection() as db:
            rows = db.execute("SELECT replay_id,route,wire FROM a2amq_delivery_replays "
                "WHERE tenant=? AND consumer=? AND published_at IS NULL"+extra+" ORDER BY replay_id LIMIT ?",
                (*params,limit)).fetchall()
            for row in rows:
                archive = db.execute("SELECT d.wire,d.digest FROM a2amq_delivery_dlq d JOIN a2amq_delivery_replays r ON "
                    "d.tenant=r.tenant AND d.consumer=r.consumer AND d.record_id=r.record_id "
                    "WHERE r.tenant=? AND r.consumer=? AND r.replay_id=?", (*self.store._keys,row[0])).fetchone()
                if archive is None or _hash(archive[0]) != archive[1]:
                    raise RetryableError("STORAGE_INTEGRITY_ERROR")
                expected = original(load_json(archive[0], MAX_DLQ_BYTES))
                expected = canonical({k:v for k,v in expected.items() if k in CE_FIELDS})
                if row[2] != expected:
                    raise RetryableError("STORAGE_INTEGRITY_ERROR")
        return [tuple(row) for row in rows]

    def mark(self, replay_id, wire):
        with self.store._transaction() as db:
            cur = db.execute("UPDATE a2amq_delivery_replays SET published_at=COALESCE(published_at,?) "
                             "WHERE tenant=? AND consumer=? AND replay_id=? AND wire=?",
                             (self.store._now(),*self.store._keys,replay_id,wire))
            require(cur.rowcount == 1, "REPLAY_ID_CONFLICT")


def original(event):
    if event.get("type") != "a2a-mq.deadletter":
        return event
    data = event["data"]
    require(data.get("truncated") is False, "TRUNCATED_DLQ_NOT_REPLAYABLE")
    encoded = data.get("originalBase64")
    require(isinstance(encoded, str) and len(encoded) <= MAX_DLQ_BYTES, "INVALID_DLQ_PAYLOAD")
    try:
        raw = base64.b64decode(encoded, validate=True)
    except ValueError:
        from .domain import PermanentError
        raise PermanentError("INVALID_DLQ_PAYLOAD") from None
    require(hashlib.sha256(raw).hexdigest() == data.get("capturedBytesSha256"), "DLQ_DIGEST_MISMATCH")
    return load_json(raw, MAX_DLQ_BYTES)


class ReplayService:
    def __init__(self, archive, database, bus, *, allowed_routes: frozenset[str], decoder: Callable | None = None):
        require(isinstance(allowed_routes, frozenset) and bool(allowed_routes), "REPLAY_ROUTES_REQUIRED")
        require(all(isinstance(r,str) and r.startswith(f"a2a.v1.{archive.store.scope.tenant}.")
                    and not r.endswith(".dlq") for r in allowed_routes), "INVALID_REPLAY_ROUTE")
        self.archive, self.database, self.bus = archive, database, bus
        self.allowed_routes, self.decoder, self.cursor = allowed_routes, decoder, None
        self.lock = asyncio.Lock()

    async def _validate(self, ce, route=None):
        require(isinstance(ce,dict) and ce.get("specversion") == "1.0", "INVALID_CLOUDEVENT")
        if ce.get("datacontenttype") == "application/json":
            require(self.decoder is None, "PLAINTEXT_REPLAY_DISABLED")
            envelope = Envelope.from_dict(ce.get("data"))
        else:
            require(ce.get("datacontenttype") == "application/vnd.a2amq.sealed+json"
                    and self.decoder is not None, "ENCRYPTED_REPLAY_DECODER_REQUIRED")
            envelope = await self.decoder(ce)
        e = envelope.as_dict()
        require(envelope.route in self.allowed_routes and (route is None or route == envelope.route), "FORBIDDEN")
        require(ce.get("id") == e["messageId"] and ce.get("source") == "urn:a2a-mq:"+e["source"]
                and ce.get("type") == "a2a-mq."+e["operation"], "IDENTIFIER_MISMATCH")
        envelope.check_expiry()
        return envelope.route

    async def enqueue(self, principal, record_id, request_id):
        ce = original(await self.database.call(self.archive.read, principal, record_id))
        route = await self._validate(ce)
        # Original ciphertext is preserved too. Dapr delivery-only fields are not republished.
        wire = canonical({k:v for k,v in ce.items() if k in CE_FIELDS})
        return await self.database.call(self.archive.enqueue, principal, record_id, request_id, route, wire)

    async def flush_once(self):
        async with self.lock:
            rows = await self.database.call(self.archive.pending, self.cursor)
            sent = 0
            for rid,route,wire in rows:
                try:
                    async with asyncio.timeout(30):
                        ce = load_json(wire, MAX_DLQ_BYTES)
                        await self._validate(ce, route)
                        await self.bus._post(route, ce)
                        await self.database.call(self.archive.mark, rid, wire)
                    sent += 1
                except Exception:
                    pass  # Durable pending record remains; no payload/credential logging.
            self.cursor = rows[-1][0] if len(rows)==50 else None
            return sent

    async def run(self, stop):
        while not stop.is_set():
            try:
                await self.flush_once()
            except Exception:
                pass
            try:
                await asyncio.wait_for(stop.wait(),1)
            except TimeoutError:
                pass
