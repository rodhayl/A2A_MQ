"""Serialize optional-module schema creation across different PostgreSQL scopes."""
from contextlib import contextmanager


@contextmanager
def schema_transaction(store):
    transaction = getattr(store, "schema_transaction", store._transaction)
    with transaction() as db:
        yield db
