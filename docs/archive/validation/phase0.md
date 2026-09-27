# Phase 0 validation

Executed in the local Python 3.13.5 environment:

- `python tools/check.py`: passed syntax, JSON, schema-copy and local-link checks.
- `python -m pytest -q`: 15 passed.
- `pip install --no-deps --no-build-isolation -e .`: passed; imported version 0.1.0.

This reused preinstalled dependencies. Fresh network dependency resolution, Ruff,
mypy, upstream SDK, PostgreSQL and live Dapr/JetStream were not executed here.
Docker/Dapr/NATS/PostgreSQL binaries were absent; direct clone/download attempts failed.
