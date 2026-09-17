"""Single-host SQLite reference persistence, scoped to a tenant and consumer.

Internal trusted boundary, not a public authorization layer. Only the reference
SQL effect is transactional here; external tool effects need provider idempotency.
"""
from __future__ import annotations

import sqlite3
import time
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .domain import (
    MAX_BYTES, STATES, TERMINAL, Envelope, PermanentError, RetryableError,
    canonical, load_json, reachable, require, state_of, token,
)

SCHEMA_VERSION = 1
MAX_PAGE = 200


@dataclass(frozen=True)
class StoreScope:
    tenant: str
    consumer: str

    def __post_init__(self) -> None:
        token(self.tenant)
        token(self.consumer)


@dataclass(frozen=True)
class Claim:
    scope: StoreScope
    message_id: str
    digest: str
    owner: str
    fence: int
    status: str  # acquired, busy, completed
    result: Envelope | None = None


@dataclass(frozen=True)
class OutboxItem:
    scope: StoreScope
    envelope: Envelope
    attempts: int


DDL = (
    "CREATE TABLE IF NOT EXISTS a2amq_inbox ("
    "tenant TEXT NOT NULL,consumer TEXT NOT NULL,message_id TEXT NOT NULL,"
    "digest TEXT NOT NULL,wire TEXT NOT NULL,status TEXT NOT NULL "
    "CHECK(status IN ('processing','done')),lease_owner TEXT,lease_until REAL,"
    "fence INTEGER NOT NULL CHECK(fence>0),result_wire TEXT,"
    "PRIMARY KEY(tenant,consumer,message_id))",
    "CREATE TABLE IF NOT EXISTS a2amq_effects ("
    "tenant TEXT NOT NULL,consumer TEXT NOT NULL,effect_key TEXT NOT NULL,"
    "result_json TEXT NOT NULL,PRIMARY KEY(tenant,consumer,effect_key))",
    "CREATE TABLE IF NOT EXISTS a2amq_tasks ("
    "tenant TEXT NOT NULL,consumer TEXT NOT NULL,task_id TEXT NOT NULL,"
    "context_id TEXT NOT NULL,source TEXT NOT NULL,state TEXT NOT NULL,"
    "sequence INTEGER NOT NULL CHECK(sequence>0),payload_json TEXT NOT NULL,"
    "PRIMARY KEY(tenant,consumer,task_id))",
    "CREATE INDEX IF NOT EXISTS a2amq_tasks_context "
    "ON a2amq_tasks(tenant,consumer,context_id,task_id)",
    "CREATE INDEX IF NOT EXISTS a2amq_tasks_state "
    "ON a2amq_tasks(tenant,consumer,state,task_id)",
    "CREATE TABLE IF NOT EXISTS a2amq_outbox ("
    "tenant TEXT NOT NULL,consumer TEXT NOT NULL,event_id TEXT NOT NULL,"
    "digest TEXT NOT NULL,wire TEXT NOT NULL,created_at REAL NOT NULL,"
    "attempts INTEGER NOT NULL DEFAULT 0 CHECK(attempts>=0),published_at REAL,"
    "PRIMARY KEY(tenant,consumer,event_id))",
    "CREATE INDEX IF NOT EXISTS a2amq_outbox_pending "
    "ON a2amq_outbox(tenant,consumer,published_at,created_at,event_id)",
)


def _key(value: Any) -> str:
    require(isinstance(value, str) and 0 < len(value) <= 256 and "\x00" not in value,
            "INVALID_STORAGE_KEY")
    try:
        value.encode("utf-8", errors="strict")
    except UnicodeError:
        raise PermanentError("INVALID_STORAGE_KEY") from None
    return value


def _page(limit: int) -> int:
    require(type(limit) is int and 1 <= limit <= MAX_PAGE, "INVALID_PAGE_SIZE")
    return limit


def _lease(value: float) -> float:
    require(type(value) in {int, float} and 0 < value <= 3600, "INVALID_LEASE")
    return float(value)


class SQLiteStore:
    """One logical consumer, shared by its replicas; distinct observers use new scopes.

    Transactions use BEGIN IMMEDIATE. Connections are short-lived, file-backed,
    WAL/FULL, and never shared across threads. Leases use a trusted UTC wall clock.
    """

    def __init__(self, path: str | Path, *, tenant: str, consumer: str,
                 clock: Callable[[], float] = time.time, busy_timeout_ms: int = 5000):
        require(isinstance(path, (str, Path)) and str(path) not in {"", ":memory:"}
                and not str(path).startswith("file:"), "INVALID_STORAGE_PATH")
        require(type(busy_timeout_ms) is int and 1 <= busy_timeout_ms <= 60000,
                "INVALID_STORAGE_TIMEOUT")
        self.path = str(Path(path).absolute())
        self._scope = StoreScope(tenant, consumer)
        self.clock, self.busy_timeout_ms = clock, busy_timeout_ms
        self._initialize()

    @property
    def scope(self) -> StoreScope:
        return self._scope

    @property
    def _keys(self) -> tuple[str, str]:
        return self.scope.tenant, self.scope.consumer

    def _now(self) -> float:
        value = self.clock()
        require(type(value) in {int, float} and 0 <= value <= 253402297199, "INVALID_TIME")
        return float(value)

    @contextmanager
    def _connection(self) -> Iterator[sqlite3.Connection]:
        try:
            db = sqlite3.connect(self.path, timeout=self.busy_timeout_ms / 1000, isolation_level=None)
        except sqlite3.OperationalError:
            raise RetryableError("STORAGE_UNAVAILABLE") from None
        db.row_factory = sqlite3.Row
        try:
            db.execute(f"PRAGMA busy_timeout={self.busy_timeout_ms}")
            db.execute("PRAGMA foreign_keys=ON")
            db.execute("PRAGMA synchronous=FULL")
            yield db
        except sqlite3.OperationalError as exc:
            code = getattr(exc, "sqlite_errorcode", 0) & 255
            if code in {sqlite3.SQLITE_BUSY, sqlite3.SQLITE_LOCKED}:
                raise RetryableError("STORAGE_BUSY") from None
            raise RetryableError("STORAGE_UNAVAILABLE") from None
        finally:
            db.close()

    @contextmanager
    def _transaction(self) -> Iterator[sqlite3.Connection]:
        with self._connection() as db:
            try:
                db.execute("BEGIN IMMEDIATE")
                yield db
                db.commit()
            except BaseException:
                db.rollback()
                raise

    def _initialize(self) -> None:
        with self._connection() as db:
            mode = db.execute("PRAGMA journal_mode=WAL").fetchone()[0]
            require(mode.lower() == "wal", "UNSUPPORTED_STORAGE_MODE")
        with self._transaction() as db:
            tables = {r[0] for r in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
            require(not (tables & {"inbox", "effects", "tasks", "outbox"}),
                    "LEGACY_UNSCOPED_STORAGE")
            db.execute("CREATE TABLE IF NOT EXISTS a2amq_schema ("
                       "singleton INTEGER PRIMARY KEY CHECK(singleton=1),version INTEGER NOT NULL)")
            version = db.execute("SELECT version FROM a2amq_schema WHERE singleton=1").fetchone()
            if version is None:
                require(not any(name.startswith("a2amq_") for name in tables),
                        "MISSING_SCHEMA_VERSION")
                db.execute("INSERT INTO a2amq_schema VALUES (1,?)", (SCHEMA_VERSION,))
            else:
                require(version[0] == SCHEMA_VERSION, "UNSUPPORTED_SCHEMA_VERSION")
                require({"a2amq_inbox", "a2amq_effects", "a2amq_tasks", "a2amq_outbox"} <= tables,
                        "INCOMPLETE_STORAGE_SCHEMA")
            for statement in DDL:
                db.execute(statement)

    def _envelope(self, envelope: Envelope, operation: str) -> dict[str, Any]:
        require(isinstance(envelope, Envelope), "INVALID_ENVELOPE")
        e = envelope.as_dict()
        require(e["tenantId"] == self.scope.tenant, "STORAGE_TENANT_MISMATCH")
        require(e["operation"] == operation, "UNSUPPORTED_STORAGE_OPERATION")
        return e

    def claim(self, envelope: Envelope, owner: str, *, lease_seconds: float = 30) -> Claim:
        e = self._envelope(envelope, "message.send")
        owner, lease = _key(owner), _lease(lease_seconds)
        mid, digest = e["messageId"], envelope.digest
        with self._transaction() as db:
            # Sample after acquiring the write lock; queue wait must consume a lease.
            now = self._now()
            row = db.execute("SELECT * FROM a2amq_inbox WHERE tenant=? AND consumer=? "
                             "AND message_id=?", (*self._keys, mid)).fetchone()
            if row is not None:
                require(row["digest"] == digest and row["wire"] == envelope.wire,
                        "MESSAGE_ID_CONFLICT")
                if row["status"] == "done":
                    result = Envelope(row["result_wire"]) if row["result_wire"] is not None else None
                    return Claim(self.scope, mid, digest, owner, row["fence"], "completed", result)
                if row["lease_until"] is not None and row["lease_until"] > now:
                    return Claim(self.scope, mid, digest, owner, row["fence"], "busy")
            envelope.check_expiry(datetime.fromtimestamp(now, timezone.utc))
            fence = row["fence"] + 1 if row is not None else 1
            require(fence <= 9223372036854775807, "FENCE_EXHAUSTED")
            if row is None:
                db.execute("INSERT INTO a2amq_inbox "
                           "(tenant,consumer,message_id,digest,wire,status,lease_owner,lease_until,fence) "
                           "VALUES (?,?,?,?,?,'processing',?,?,?)",
                           (*self._keys, mid, digest, envelope.wire, owner, now + lease, fence))
            else:
                db.execute("UPDATE a2amq_inbox SET lease_owner=?,lease_until=?,fence=? "
                           "WHERE tenant=? AND consumer=? AND message_id=?",
                           (owner, now + lease, fence, *self._keys, mid))
            return Claim(self.scope, mid, digest, owner, fence, "acquired")

    def _owned(self, db: sqlite3.Connection, claim: Claim, now: float) -> sqlite3.Row:
        require(isinstance(claim, Claim) and claim.scope == self.scope
                and claim.status == "acquired", "CLAIM_NOT_OWNED")
        row = db.execute("SELECT * FROM a2amq_inbox WHERE tenant=? AND consumer=? "
                         "AND message_id=?", (*self._keys, claim.message_id)).fetchone()
        require(row is not None and row["digest"] == claim.digest
                and row["status"] == "processing", "CLAIM_NOT_OWNED")
        if row["fence"] != claim.fence:
            raise RetryableError("STALE_FENCE")
        require(row["lease_owner"] == claim.owner, "CLAIM_NOT_OWNED")
        if row["lease_until"] is None or now >= row["lease_until"]:
            raise RetryableError("LEASE_EXPIRED")
        return row

    def renew(self, claim: Claim, *, lease_seconds: float = 30) -> None:
        lease = _lease(lease_seconds)
        with self._transaction() as db:
            now = self._now()
            row = self._owned(db, claim, now)
            db.execute("UPDATE a2amq_inbox SET lease_until=? "
                       "WHERE tenant=? AND consumer=? AND message_id=?",
                       (max(row["lease_until"], now + lease), *self._keys, claim.message_id))

    def _project(self, db: sqlite3.Connection, event: Envelope) -> bool:
        e = self._envelope(event, "task.snapshot")
        task, sequence = e["payload"], e["sequence"]
        _key(e["taskId"])
        _key(e["contextId"])
        # JSON Schema considers integral floats integers; SQL versions are strict integers.
        require(type(sequence) is int, "INVALID_SEQUENCE")
        payload, state = canonical(task), state_of(task)
        row = db.execute("SELECT * FROM a2amq_tasks WHERE tenant=? AND consumer=? AND task_id=?",
                         (*self._keys, e["taskId"])).fetchone()
        if row is not None:
            require(row["source"] == e["source"], "TASK_OWNER_CONFLICT")
            require(row["context_id"] == e["contextId"], "TASK_CONTEXT_CONFLICT")
            if sequence < row["sequence"]:
                return False
            if sequence == row["sequence"]:
                require(row["payload_json"] == payload, "TASK_VERSION_CONFLICT")
                return False
            require(row["state"] not in TERMINAL, "TERMINAL_TASK_REWRITE")
            require(reachable(row["state"], state), "INVALID_TASK_TRANSITION")
        db.execute("INSERT INTO a2amq_tasks VALUES (?,?,?,?,?,?,?,?) "
                   "ON CONFLICT(tenant,consumer,task_id) DO UPDATE SET "
                   "state=excluded.state,sequence=excluded.sequence,payload_json=excluded.payload_json",
                   (*self._keys, e["taskId"], e["contextId"], e["source"], state, sequence, payload))
        return True

    def commit_claim(self, claim: Claim, *, effect_key: str, effect: Any,
                     result: Envelope) -> None:
        e = self._envelope(result, "task.snapshot")
        key = _key(effect_key)
        effect_json = canonical(effect)
        load_json(effect_json, MAX_BYTES)  # shared byte/depth/finite-number boundary
        with self._transaction() as db:
            now = self._now()
            row = self._owned(db, claim, now)
            request = Envelope(row["wire"]).as_dict()
            require(e["causationId"] == claim.message_id
                    and e["correlationId"] == request["correlationId"], "RESULT_COMMAND_MISMATCH")
            for name in ("taskId", "contextId"):
                supplied = request.get(name, request["payload"]["message"].get(name))
                require(supplied is None or e[name] == supplied, "RESULT_COMMAND_MISMATCH")
            prior = db.execute("SELECT result_json FROM a2amq_effects WHERE tenant=? AND consumer=? "
                               "AND effect_key=?", (*self._keys, key)).fetchone()
            if prior is not None:
                require(prior[0] == effect_json, "EFFECT_CONFLICT")
            else:
                db.execute("INSERT INTO a2amq_effects VALUES (?,?,?,?)",
                           (*self._keys, key, effect_json))
            require(self._project(db, result), "STALE_TASK_RESULT")
            existing = db.execute("SELECT digest,wire FROM a2amq_outbox WHERE tenant=? AND consumer=? "
                                  "AND event_id=?", (*self._keys, e["messageId"])).fetchone()
            if existing is None:
                db.execute("INSERT INTO a2amq_outbox "
                           "(tenant,consumer,event_id,digest,wire,created_at) VALUES (?,?,?,?,?,?)",
                           (*self._keys, e["messageId"], result.digest, result.wire, now))
            else:
                require(existing["digest"] == result.digest and existing["wire"] == result.wire,
                        "OUTBOX_ID_CONFLICT")
            db.execute("UPDATE a2amq_inbox SET status='done',lease_owner=NULL,lease_until=NULL,result_wire=? "
                       "WHERE tenant=? AND consumer=? AND message_id=?",
                       (result.wire, *self._keys, claim.message_id))

    def apply_snapshot(self, event: Envelope) -> bool:
        e = self._envelope(event, "task.snapshot")
        with self._transaction() as db:
            row = db.execute("SELECT digest,wire,status FROM a2amq_inbox "
                             "WHERE tenant=? AND consumer=? AND message_id=?",
                             (*self._keys, e["messageId"])).fetchone()
            if row is not None:
                require(row["digest"] == event.digest and row["wire"] == event.wire,
                        "MESSAGE_ID_CONFLICT")
                require(row["status"] == "done", "INBOX_KIND_CONFLICT")
                return False
            event.check_expiry(datetime.fromtimestamp(self._now(), timezone.utc))
            changed = self._project(db, event)
            db.execute("INSERT INTO a2amq_inbox "
                       "(tenant,consumer,message_id,digest,wire,status,fence) VALUES (?,?,?,?,?,'done',1)",
                       (*self._keys, e["messageId"], event.digest, event.wire))
            return changed

    def pending_outbox(self, *, limit: int = 100) -> list[OutboxItem]:
        _page(limit)
        with self._connection() as db:
            rows = db.execute("SELECT wire,attempts FROM a2amq_outbox WHERE tenant=? AND consumer=? "
                              "AND published_at IS NULL ORDER BY created_at,event_id LIMIT ?",
                              (*self._keys, limit)).fetchall()
        return [OutboxItem(self.scope, Envelope(r["wire"]), r["attempts"]) for r in rows]

    def _outbox_row(self, db: sqlite3.Connection, item: OutboxItem) -> sqlite3.Row:
        require(isinstance(item, OutboxItem) and item.scope == self.scope, "OUTBOX_SCOPE_MISMATCH")
        row = db.execute("SELECT * FROM a2amq_outbox WHERE tenant=? AND consumer=? AND event_id=?",
                         (*self._keys, item.envelope.as_dict()["messageId"])).fetchone()
        require(row is not None and row["wire"] == item.envelope.wire
                and row["digest"] == item.envelope.digest, "OUTBOX_ID_CONFLICT")
        return row

    def record_publish_attempt(self, item: OutboxItem) -> bool:
        with self._transaction() as db:
            row = self._outbox_row(db, item)
            if row["published_at"] is not None:
                return False
            db.execute("UPDATE a2amq_outbox SET attempts=attempts+1 "
                       "WHERE tenant=? AND consumer=? AND event_id=?",
                       (*self._keys, row["event_id"]))
            return True

    def mark_published(self, item: OutboxItem) -> None:
        with self._transaction() as db:
            row = self._outbox_row(db, item)
            require(row["attempts"] > 0, "OUTBOX_NOT_ATTEMPTED")
            db.execute("UPDATE a2amq_outbox SET published_at=COALESCE(published_at,?) "
                       "WHERE tenant=? AND consumer=? AND event_id=?",
                       (self._now(), *self._keys, row["event_id"]))

    def get_task(self, task_id: str) -> dict[str, Any] | None:
        task_id = _key(task_id)
        with self._connection() as db:
            row = db.execute("SELECT payload_json FROM a2amq_tasks "
                             "WHERE tenant=? AND consumer=? AND task_id=?",
                             (*self._keys, task_id)).fetchone()
        return load_json(row[0]) if row is not None else None

    def get_effect(self, effect_key: str) -> Any:
        key = _key(effect_key)
        with self._connection() as db:
            row = db.execute("SELECT result_json FROM a2amq_effects "
                             "WHERE tenant=? AND consumer=? AND effect_key=?",
                             (*self._keys, key)).fetchone()
        return load_json(row[0]) if row is not None else None

    def list_tasks(self, *, context_id: str | None = None, state: str | None = None,
                   after: str | None = None, limit: int = 50) -> tuple[list[dict[str, Any]], str | None]:
        _page(limit)
        clauses, params = ["tenant=?", "consumer=?"], list(self._keys)
        for column, value in (("context_id", context_id), ("state", state), ("task_id", after)):
            if value is not None:
                _key(value)
                if column == "state":
                    require(value in STATES, "INVALID_STATE")
                clauses.append(column + (">?" if column == "task_id" else "=?"))
                params.append(value)
        with self._connection() as db:
            rows = db.execute("SELECT task_id,payload_json FROM a2amq_tasks WHERE "
                              + " AND ".join(clauses) + " ORDER BY task_id LIMIT ?",
                              (*params, limit + 1)).fetchall()
        page = rows[:limit]
        cursor = page[-1]["task_id"] if len(rows) > limit else None
        return [load_json(r["payload_json"]) for r in page], cursor
