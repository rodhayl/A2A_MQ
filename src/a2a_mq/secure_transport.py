"""Persist encrypted publication bytes before sending them through Dapr."""
from __future__ import annotations

import hashlib

from .domain import Envelope, RetryableError, load_json, require
from .protection import EnvelopeProtector, SEALED_LIMIT
from .runtime import AsyncSQLite
from .transport import DaprPublisher, cloud_event

SEALED_MIME = "application/vnd.a2amq.sealed+json"


class SealedCache:
    def __init__(self, store):
        self.store = store
        with store._transaction() as db:
            db.execute("CREATE TABLE IF NOT EXISTS a2amq_sealed ("
                       "tenant TEXT NOT NULL,consumer TEXT NOT NULL,event_id TEXT NOT NULL,"
                       "receiver TEXT NOT NULL,digest TEXT NOT NULL,sealed TEXT NOT NULL,"
                       "sealed_digest TEXT NOT NULL,PRIMARY KEY(tenant,consumer,event_id,receiver))")

    def _check(self, row, envelope):
        if row is None:
            return None
        require(row["digest"] == envelope.digest, "MESSAGE_ID_CONFLICT")
        raw = row["sealed"].encode()
        if len(raw) > SEALED_LIMIT or hashlib.sha256(raw).hexdigest() != row["sealed_digest"]:
            raise RetryableError("STORAGE_INTEGRITY_ERROR")
        return raw

    def get(self, envelope, receiver):
        require(envelope.as_dict()["tenantId"] == self.store.scope.tenant, "SEALED_SCOPE_MISMATCH")
        with self.store._connection() as db:
            row = db.execute("SELECT * FROM a2amq_sealed WHERE tenant=? AND consumer=? AND event_id=? AND receiver=?",
                             (*self.store._keys, envelope.as_dict()["messageId"], receiver)).fetchone()
            return self._check(row, envelope)

    def remember(self, envelope, receiver, raw):
        require(len(raw) <= SEALED_LIMIT, "MESSAGE_TOO_LARGE")
        with self.store._transaction() as db:
            db.execute("INSERT INTO a2amq_sealed VALUES (?,?,?,?,?,?,?) "
                       "ON CONFLICT(tenant,consumer,event_id,receiver) DO NOTHING",
                       (*self.store._keys, envelope.as_dict()["messageId"], receiver, envelope.digest,
                        raw.decode(), hashlib.sha256(raw).hexdigest()))
            row = db.execute("SELECT * FROM a2amq_sealed WHERE tenant=? AND consumer=? AND event_id=? AND receiver=?",
                             (*self.store._keys, envelope.as_dict()["messageId"], receiver)).fetchone()
            return self._check(row, envelope)


class ProtectedPublisher:
    def __init__(self, bus: DaprPublisher, database: AsyncSQLite,
                 routes: dict[str, EnvelopeProtector]):
        require(bool(routes), "ENCRYPTION_ROUTES_REQUIRED")
        self.bus, self.database, self.routes = bus, database, dict(routes)
        self.cache = SealedCache(database.store)

    async def publish(self, envelope: Envelope) -> None:
        protector = self.routes.get(envelope.route)
        require(protector is not None, "ENCRYPTION_ROUTE_NOT_AUTHORIZED")
        raw = await self.database.call(self.cache.get, envelope, protector.receiver)
        if raw is None:
            candidate = await protector.seal(envelope)
            raw = await self.database.call(self.cache.remember, envelope, protector.receiver, candidate)
        event = cloud_event(envelope)
        event.update(data=load_json(raw, SEALED_LIMIT), datacontenttype=SEALED_MIME)
        await self.bus._post(envelope.route, event)
