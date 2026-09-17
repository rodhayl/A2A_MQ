"""Pooled PostgreSQL adapter for the existing transactional storage contract.

The shared SQL methods live in SQLiteStore; only the unit-of-work and schema
boundary are backend-specific. Scope-level transaction advisory locks serialize
replicas of a logical consumer, not tenants globally. This deliberately favors
correctness over maximum within-pool write throughput.
"""
from __future__ import annotations

from contextlib import contextmanager
import hashlib
import time
from typing import Any, Iterator

from .domain import PermanentError, RetryableError, require
from .storage import DDL, SCHEMA_VERSION, SQLiteStore, StoreScope


class _Row:
    def __init__(self, values: tuple, names: tuple[str, ...]):
        self.values, self.names = values, names

    def __getitem__(self, key: int | str) -> Any:
        return self.values[self.names.index(key) if isinstance(key, str) else key]

    def __iter__(self):
        return iter(self.values)


class _Cursor:
    def __init__(self, cursor):
        self.cursor = cursor
        self.names = tuple(c.name for c in cursor.description) if cursor.description else ()

    @property
    def rowcount(self) -> int:
        return self.cursor.rowcount

    def fetchone(self):
        row = self.cursor.fetchone()
        return None if row is None else _Row(row, self.names)

    def fetchall(self):
        return [_Row(row, self.names) for row in self.cursor.fetchall()]

    def __iter__(self):
        return (_Row(row, self.names) for row in self.cursor)


class _Connection:
    def __init__(self, connection):
        self.raw = connection

    def execute(self, statement: str, parameters=()):
        # Statements are exclusively fixed application SQL, never client SQL.
        return _Cursor(self.raw.execute(statement.replace("?", "%s"), parameters))


def scope_lock(scope: StoreScope) -> tuple[int, int]:
    raw = hashlib.sha256((scope.tenant + "\x00" + scope.consumer).encode()).digest()
    return tuple(int.from_bytes(raw[i:i + 4], "big", signed=True) for i in (0, 4))


class PostgresStore(SQLiteStore):
    """Same scoped inbox/effect/projection/outbox API, with PostgreSQL transactions."""

    def __init__(self, dsn: str, *, tenant: str, consumer: str, clock=time.time,
                 pool_size: int = 8, timeout: float = 5, allow_insecure: bool = False):
        self._scope = StoreScope(tenant, consumer)
        require(isinstance(dsn, str) and 0 < len(dsn) <= 8192, "INVALID_POSTGRES_DSN")
        require(type(pool_size) is int and 1 <= pool_size <= 64, "INVALID_STORAGE_CAPACITY")
        require(type(timeout) in {int, float} and 0 < timeout <= 60, "INVALID_STORAGE_TIMEOUT")
        require(type(allow_insecure) is bool, "INVALID_TLS_CONFIG")
        try:
            import psycopg
            from psycopg.conninfo import conninfo_to_dict
            from psycopg_pool import ConnectionPool, PoolTimeout
        except ImportError:
            raise PermanentError("POSTGRES_EXTRA_REQUIRED") from None
        try:
            parameters = conninfo_to_dict(dsn)
        except psycopg.Error:
            raise PermanentError("INVALID_POSTGRES_DSN") from None
        require(allow_insecure or (parameters.get("sslmode") == "verify-full"
                                  and bool(parameters.get("sslrootcert"))), "POSTGRES_TLS_REQUIRED")
        self.clock, self.timeout = clock, float(timeout)
        self._errors = (psycopg.Error, PoolTimeout)
        self._pool = ConnectionPool(
            conninfo=dsn, min_size=0, max_size=pool_size, timeout=timeout, open=True,
            kwargs={"autocommit": True, "connect_timeout": max(1, int(timeout)),
                    "application_name": "a2amq", "prepare_threshold": None},
            check=ConnectionPool.check_connection,
        )
        try:
            self._initialize()
        except BaseException:
            self._pool.close()
            raise

    def close(self) -> None:
        self._pool.close()

    @contextmanager
    def _connection(self) -> Iterator[_Connection]:
        try:
            with self._pool.connection(timeout=self.timeout) as raw:
                # Read operations also get bounded transactions and a fixed schema.
                with raw.transaction():
                    raw.execute("SET LOCAL search_path TO a2amq, pg_catalog")
                    raw.execute("SELECT set_config('statement_timeout', %s, true)",
                                (str(int(self.timeout * 1000)),))
                    raw.execute("SELECT set_config('lock_timeout', %s, true)",
                                (str(int(self.timeout * 1000)),))
                    raw.execute("SET LOCAL synchronous_commit TO on")
                    yield _Connection(raw)
        except self._errors as exc:
            code = getattr(exc, "sqlstate", "")
            if code in {"40001", "40P01", "55P03", "57014"}:
                raise RetryableError("STORAGE_BUSY") from None
            raise RetryableError("STORAGE_UNAVAILABLE") from None

    @contextmanager
    def _transaction(self) -> Iterator[_Connection]:
        with self._connection() as db:
            db.execute("SELECT pg_advisory_xact_lock(?, ?)", scope_lock(self.scope))
            yield db

    def _initialize(self) -> None:
        with self._connection() as db:
            db.execute("SELECT pg_advisory_xact_lock(190247, 1)")
            db.execute("CREATE SCHEMA IF NOT EXISTS a2amq")
            names = {r[0] for r in db.execute(
                "SELECT table_name FROM information_schema.tables WHERE table_schema='a2amq'")}
            require(not names & {"inbox", "effects", "tasks", "outbox"}, "LEGACY_UNSCOPED_STORAGE")
            db.execute("CREATE TABLE IF NOT EXISTS a2amq_schema ("
                       "singleton INTEGER PRIMARY KEY CHECK(singleton=1),version INTEGER NOT NULL)")
            version = db.execute("SELECT version FROM a2amq_schema WHERE singleton=1").fetchone()
            if version is None:
                require(not any(n.startswith("a2amq_") for n in names), "MISSING_SCHEMA_VERSION")
                db.execute("INSERT INTO a2amq_schema VALUES (1,?)", (SCHEMA_VERSION,))
            else:
                require(version[0] == SCHEMA_VERSION, "UNSUPPORTED_SCHEMA_VERSION")
                require({"a2amq_inbox", "a2amq_tasks", "a2amq_effects", "a2amq_outbox"} <= names,
                        "INCOMPLETE_STORAGE_SCHEMA")
            for statement in DDL:
                db.execute(statement.replace(" REAL", " DOUBLE PRECISION").replace(" INTEGER", " BIGINT"))


def open_store(*, tenant: str, consumer: str, path: str | None = None,
               dsn: str | None = None, allow_insecure: bool = False):
    require(bool(path) != bool(dsn), "EXACTLY_ONE_STORAGE_BACKEND_REQUIRED")
    if dsn:
        return PostgresStore(dsn, tenant=tenant, consumer=consumer, allow_insecure=allow_insecure)
    return SQLiteStore(path, tenant=tenant, consumer=consumer)
