"""Scoped operational primitives: retention, DLQ replay, metrics and backups."""
from __future__ import annotations

import base64
import hashlib
import math
from pathlib import Path
import sqlite3
import threading
from typing import Any

from .domain import Envelope, PermanentError, RetryableError, require
from .identity import Principal
from .storage import SCHEMA_VERSION, _key, _page
from .tasks import TaskLedger, stable_id

TERMINAL_SQL = "('COMPLETED','FAILED','CANCELED','REJECTED')"
DLQ_DDL = (
    "CREATE TABLE IF NOT EXISTS a2amq_dlq (tenant TEXT NOT NULL,consumer TEXT NOT NULL,"
    "entry_id TEXT NOT NULL,digest TEXT NOT NULL,raw_base64 TEXT NOT NULL,reason TEXT NOT NULL,"
    "captured_at DOUBLE PRECISION NOT NULL,attempts BIGINT NOT NULL DEFAULT 0,"
    "replayed_at DOUBLE PRECISION,PRIMARY KEY(tenant,consumer,entry_id))",
    "CREATE INDEX IF NOT EXISTS a2amq_dlq_pending ON "
    "a2amq_dlq(tenant,consumer,replayed_at,captured_at,entry_id)",
)


def _admin(principal: Principal, tenant: str) -> None:
    require(isinstance(principal, Principal) and principal.tenant == tenant
            and "a2a:admin" in principal.scopes, "FORBIDDEN")


def _number(value: Any, code: str, *, maximum: float) -> float:
    require(type(value) in {int, float} and math.isfinite(value) and 0 <= value <= maximum, code)
    return float(value)


def _reason(value: Any) -> str:
    require(isinstance(value, str) and 1 <= len(value) <= 128
            and all(32 <= ord(c) < 127 for c in value), "INVALID_DLQ_REASON")
    return value


class Metrics:
    """Dependency-free counters plus Prometheus text exposition."""

    NAMES = frozenset({
        "a2amq_dlq_captured_total", "a2amq_dlq_replay_attempts_total",
        "a2amq_dlq_replayed_total", "a2amq_tasks_retired_total",
        "a2amq_backup_success_total", "a2amq_backup_failure_total",
    })

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._values = {name: 0.0 for name in self.NAMES}

    def inc(self, name: str, amount: int | float = 1) -> None:
        require(name in self.NAMES and type(amount) in {int, float}
                and math.isfinite(amount) and amount >= 0, "INVALID_METRIC")
        with self._lock:
            self._values[name] += float(amount)

    def snapshot(self) -> dict[str, float]:
        with self._lock:
            return dict(self._values)

    def prometheus(self, gauges: dict[str, int | float] | None = None) -> str:
        with self._lock:
            values = dict(self._values)
        for name, value in (gauges or {}).items():
            require(name.startswith("a2amq_") and type(value) in {int, float}
                    and math.isfinite(value), "INVALID_METRIC")
            values[name] = float(value)
        lines = []
        for name, value in sorted(values.items()):
            kind = "counter" if name.endswith("_total") else "gauge"
            lines.append(f"# TYPE {name} {kind}\n{name} {value:g}\n")
        return "".join(lines)


class OperationsManager:
    """Administrative operations bound to one tenant and logical consumer."""

    def __init__(self, ledger: TaskLedger, *, metrics: Metrics | None = None) -> None:
        self.ledger, self.store = ledger, ledger.store
        self.metrics = metrics or Metrics()
        with self.store._transaction() as db:
            for statement in DLQ_DDL:
                db.execute(statement)

    @property
    def keys(self) -> tuple[str, str]:
        return self.store._keys

    def stats(self, principal: Principal) -> dict[str, int]:
        _admin(principal, self.store.scope.tenant)
        queries = {
            "jobs": "SELECT COUNT(*) FROM a2amq_jobs WHERE tenant=? AND consumer=?",
            "active": "SELECT COUNT(*) FROM a2amq_tasks WHERE tenant=? AND consumer=? AND state NOT IN " + TERMINAL_SQL,
            "pendingActions": "SELECT COUNT(*) FROM a2amq_actions WHERE tenant=? AND consumer=? AND delivered_at IS NULL",
            "pendingOutbox": "SELECT COUNT(*) FROM a2amq_outbox WHERE tenant=? AND consumer=? AND published_at IS NULL",
            "dlqPending": "SELECT COUNT(*) FROM a2amq_dlq WHERE tenant=? AND consumer=? AND replayed_at IS NULL",
            "retired": "SELECT COUNT(*) FROM a2amq_retired WHERE tenant=? AND consumer=?",
        }
        with self.store._connection() as db:
            return {name: int(db.execute(sql, self.keys).fetchone()[0]) for name, sql in queries.items()}

    def prometheus(self, principal: Principal) -> str:
        names = {
            "jobs": "a2amq_jobs", "active": "a2amq_active_tasks",
            "pendingActions": "a2amq_pending_actions", "pendingOutbox": "a2amq_pending_outbox",
            "dlqPending": "a2amq_pending_dlq", "retired": "a2amq_retired_tasks",
        }
        return self.metrics.prometheus({names[k]: v for k, v in self.stats(principal).items()})

    def record_dlq(self, raw: bytes, reason: str, *, captured_at: float | None = None) -> str:
        require(isinstance(raw, bytes) and 0 < len(raw) <= 1024 * 1024, "INVALID_DLQ_PAYLOAD")
        reason = _reason(reason)
        digest = hashlib.sha256(raw).hexdigest()
        entry_id = stable_id(*self.keys, "dlq", digest, reason)
        encoded = base64.b64encode(raw).decode("ascii")
        now = self.store._now() if captured_at is None else _number(
            captured_at, "INVALID_TIME", maximum=253402297199)
        with self.store._transaction() as db:
            row = db.execute("SELECT digest,raw_base64,reason FROM a2amq_dlq "
                             "WHERE tenant=? AND consumer=? AND entry_id=?",
                             (*self.keys, entry_id)).fetchone()
            if row is None:
                db.execute("INSERT INTO a2amq_dlq "
                           "(tenant,consumer,entry_id,digest,raw_base64,reason,captured_at) "
                           "VALUES (?,?,?,?,?,?,?)",
                           (*self.keys, entry_id, digest, encoded, reason, now))
                self.metrics.inc("a2amq_dlq_captured_total")
            else:
                require((row["digest"], row["raw_base64"], row["reason"])
                        == (digest, encoded, reason), "DLQ_ID_CONFLICT")
        return entry_id

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
                "SELECT entry_id,digest,reason,captured_at,attempts,replayed_at FROM a2amq_dlq WHERE "
                + " AND ".join(clauses) + " ORDER BY entry_id LIMIT ?", (*params, limit + 1)).fetchall()
        selected = rows[:limit]
        items = [{"entryId": r["entry_id"], "digest": r["digest"], "reason": r["reason"],
                  "capturedAt": r["captured_at"], "attempts": int(r["attempts"]),
                  "replayedAt": r["replayed_at"]} for r in selected]
        return items, selected[-1]["entry_id"] if len(rows) > limit and selected else None

    def dlq_payload(self, principal: Principal, entry_id: str) -> bytes:
        _admin(principal, self.store.scope.tenant)
        entry_id = _key(entry_id)
        with self.store._connection() as db:
            row = db.execute("SELECT digest,raw_base64 FROM a2amq_dlq "
                             "WHERE tenant=? AND consumer=? AND entry_id=?",
                             (*self.keys, entry_id)).fetchone()
        require(row is not None, "DLQ_NOT_FOUND")
        try:
            raw = base64.b64decode(row["raw_base64"], validate=True)
        except (ValueError, TypeError):
            raise RetryableError("STORAGE_INTEGRITY_ERROR") from None
        if hashlib.sha256(raw).hexdigest() != row["digest"]:
            raise RetryableError("STORAGE_INTEGRITY_ERROR")
        return raw

    def replay_candidate(self, principal: Principal, entry_id: str) -> Envelope:
        _admin(principal, self.store.scope.tenant)
        entry_id = _key(entry_id)
        try:
            envelope = Envelope.parse(self.dlq_payload(principal, entry_id))
        except PermanentError:
            raise PermanentError("DLQ_NOT_REPLAYABLE") from None
        require(envelope.as_dict()["tenantId"] == self.store.scope.tenant, "DLQ_TENANT_MISMATCH")
        envelope.check_expiry()
        return envelope

    def prepare_replay(self, principal: Principal, entry_id: str, *, force: bool = False) -> None:
        _admin(principal, self.store.scope.tenant)
        require(type(force) is bool, "INVALID_QUERY")
        entry_id = _key(entry_id)
        with self.store._transaction() as db:
            row = db.execute("SELECT replayed_at FROM a2amq_dlq WHERE tenant=? AND consumer=? AND entry_id=?",
                             (*self.keys, entry_id)).fetchone()
            require(row is not None, "DLQ_NOT_FOUND")
            require(force or row["replayed_at"] is None, "DLQ_ALREADY_REPLAYED")
            db.execute("UPDATE a2amq_dlq SET attempts=attempts+1 WHERE tenant=? AND consumer=? AND entry_id=?",
                       (*self.keys, entry_id))
        self.metrics.inc("a2amq_dlq_replay_attempts_total")

    def mark_replayed(self, principal: Principal, entry_id: str) -> None:
        _admin(principal, self.store.scope.tenant)
        entry_id = _key(entry_id)
        with self.store._transaction() as db:
            row = db.execute("SELECT attempts FROM a2amq_dlq "
                             "WHERE tenant=? AND consumer=? AND entry_id=?",
                             (*self.keys, entry_id)).fetchone()
            require(row is not None, "DLQ_NOT_FOUND")
            require(row["attempts"] > 0, "DLQ_NOT_ATTEMPTED")
            db.execute("UPDATE a2amq_dlq SET replayed_at=COALESCE(replayed_at,?) "
                       "WHERE tenant=? AND consumer=? AND entry_id=?",
                       (self.store._now(), *self.keys, entry_id))
        self.metrics.inc("a2amq_dlq_replayed_total")

    async def replay_dlq(self, principal: Principal, entry_id: str, publisher,
                         *, force: bool = False) -> Envelope:
        envelope = self.replay_candidate(principal, entry_id)
        self.prepare_replay(principal, entry_id, force=force)
        await publisher.publish(envelope)
        self.mark_replayed(principal, entry_id)
        return envelope

    def retire(self, principal: Principal, *, retention_seconds: float,
               limit: int = 100) -> list[str]:
        """Retire terminal jobs only after runtime completion and publication drain."""
        _admin(principal, self.store.scope.tenant)
        _page(limit)
        retention = _number(retention_seconds, "INVALID_RETENTION",
                            maximum=10 * 365 * 24 * 3600)
        cutoff = self.store._now() - retention
        with self.store._transaction() as db:
            rows = db.execute(
                "SELECT j.task_id,j.subject,j.request_id,j.digest,j.wire FROM a2amq_jobs j "
                "JOIN a2amq_tasks t ON j.tenant=t.tenant AND j.consumer=t.consumer AND j.task_id=t.task_id "
                "JOIN a2amq_runtime_end e ON e.tenant=j.tenant AND e.consumer=j.consumer AND e.task_id=j.task_id "
                "WHERE j.tenant=? AND j.consumer=? AND t.state IN " + TERMINAL_SQL + " AND e.finished_at<=? "
                "AND NOT EXISTS (SELECT 1 FROM a2amq_actions a WHERE a.tenant=j.tenant "
                "AND a.consumer=j.consumer AND a.task_id=j.task_id AND a.delivered_at IS NULL) "
                "ORDER BY e.finished_at,j.task_id LIMIT ?", (*self.keys, cutoff, limit)).fetchall()
            retired: list[str] = []
            for row in rows:
                task_id = row["task_id"]
                steps = db.execute("SELECT event_wire FROM a2amq_steps WHERE tenant=? AND consumer=? AND task_id=?",
                                   (*self.keys, task_id)).fetchall()
                event_ids = [Envelope(r["event_wire"]).as_dict()["messageId"] for r in steps]
                if event_ids:
                    marks = ",".join("?" for _ in event_ids)
                    pending = db.execute("SELECT COUNT(*) FROM a2amq_outbox WHERE tenant=? AND consumer=? "
                                         "AND published_at IS NULL AND event_id IN (" + marks + ")",
                                         (*self.keys, *event_ids)).fetchone()[0]
                    if pending:
                        continue
                command_digest = Envelope(row["wire"]).digest
                expected = (row["subject"], row["request_id"], row["digest"], command_digest)
                prior = db.execute("SELECT subject,request_id,digest,command_digest FROM a2amq_retired "
                                   "WHERE tenant=? AND consumer=? AND task_id=?",
                                   (*self.keys, task_id)).fetchone()
                if prior is None:
                    db.execute("INSERT INTO a2amq_retired "
                               "(tenant,consumer,task_id,subject,request_id,digest,command_digest,retired_at) "
                               "VALUES (?,?,?,?,?,?,?,?)",
                               (*self.keys, task_id, *expected, self.store._now()))
                else:
                    require(tuple(prior) == expected, "RETIREMENT_CONFLICT")
                if event_ids:
                    marks = ",".join("?" for _ in event_ids)
                    db.execute("DELETE FROM a2amq_outbox WHERE tenant=? AND consumer=? AND event_id IN ("
                               + marks + ")", (*self.keys, *event_ids))
                for table in ("a2amq_steps", "a2amq_actions", "a2amq_runtime_end",
                              "a2amq_tasks", "a2amq_jobs"):
                    db.execute(f"DELETE FROM {table} WHERE tenant=? AND consumer=? AND task_id=?",
                               (*self.keys, task_id))
                retired.append(task_id)
        self.metrics.inc("a2amq_tasks_retired_total", len(retired))
        return retired

    def backup_sqlite(self, principal: Principal, destination: str | Path) -> dict[str, Any]:
        _admin(principal, self.store.scope.tenant)
        source_path = getattr(self.store, "path", None)
        require(isinstance(source_path, str), "SQLITE_BACKUP_ONLY")
        assert isinstance(source_path, str)
        destination = Path(destination).absolute()
        require(destination != Path(source_path).absolute(), "INVALID_BACKUP_PATH")
        try:
            # Reuse the no-overwrite online backup primitive used by the canonical
            # admin path. It snapshots WAL state into a private temp file and installs
            # with os.link(), avoiding destination symlink/check-then-create races.
            from .sqlite_archive import backup_sqlite as create_backup
            created = create_backup(source_path, destination)
            verified = self.verify_sqlite_backup(destination)
            require(created["sha256"] == verified["sha256"]
                    and created["bytes"] == verified["bytes"], "BACKUP_INTEGRITY_ERROR")
            self.metrics.inc("a2amq_backup_success_total")
            return {"path": str(destination), **verified}
        except PermanentError:
            self.metrics.inc("a2amq_backup_failure_total")
            raise
        except (OSError, sqlite3.Error):
            self.metrics.inc("a2amq_backup_failure_total")
            raise RetryableError("BACKUP_UNAVAILABLE") from None

    @staticmethod
    def verify_sqlite_backup(path: str | Path) -> dict[str, Any]:
        path = Path(path)
        require(path.is_file() and not path.is_symlink(), "INVALID_BACKUP_PATH")
        try:
            db = sqlite3.connect(f"file:{path.absolute().as_posix()}?mode=ro", uri=True)
            try:
                require(db.execute("PRAGMA integrity_check").fetchone()[0] == "ok", "BACKUP_INTEGRITY_ERROR")
                version = db.execute("SELECT version FROM a2amq_schema WHERE singleton=1").fetchone()
                require(version is not None and int(version[0]) == SCHEMA_VERSION, "BACKUP_INTEGRITY_ERROR")
            finally:
                db.close()
            return {"sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
                    "bytes": path.stat().st_size, "schemaVersion": SCHEMA_VERSION}
        except PermanentError:
            raise
        except (OSError, sqlite3.Error):
            raise PermanentError("BACKUP_INTEGRITY_ERROR") from None
