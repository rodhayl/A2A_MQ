# Integrated implementation evidence - 2026-09-17

## Source and integration provenance

Work started from remote commit `617ecbabfe8db5f4854f345c6e6d8a17ecc34621`.
The remote branch later advanced by 25 commits to
`79b4de69f2460479e691be2eaa67a41dfdee2e4f` (tree
`c142b64424dfbc082d765be4aa11ac48205c9b1c`). These concurrent changes are preserved.
Only the new profile and explicitly verified shared-file edits enter the supplied
integration patches; they do not replace the new remote production/operations/CI
modules. The delivered verdict records local HEAD and source hashes. These refer to a
selected materialization created before publication; validate the repository HEAD
containing this file for the published integration commit.

The execution workspace contains the previously verified package files, the new
modules/tests and selected metadata. It is not a full clone of the latest remote
repository. Original/concurrent repository tests were not present in this execution
selection. Integration preimages are verified Git blobs; application checks are
performed against a reconstructed affected-file base. This does not prove all
concurrently added modules were tested together.

The new helpers are `maintenance.py` and `sqlite_archive.py`. The delivery DLQ
schema is separately named `a2amq_delivery_dlq`/`a2amq_delivery_replays`; the remote
`operations.py`, `backup.py`, their different DLQ schema and production profiles
remain intact. No schema ownership is inferred and no archive migration is claimed.

## Executed results

- **170 selected new local tests passed; 0 failures, 0 errors, 0 skipped.**
  Six native/live cases were explicitly deselected, not counted as passing.
- Actual SQLite transactions, permissions/retirement, cryptography/JWT, OTel SDK,
  application lifecycle/ASGI requests and deterministic deployment contracts ran.
- Native SDK/service boundaries and PostgreSQL command clients in local tests are
  declared fixtures where applicable, not evidence of live SDK/server behavior.
- Python syntax parsing passed. Layout-only edits were AST-equivalence checked.
  This is not a Ruff or mypy execution.
- During development an offline wheel build/target installation and isolated smoke
  verified 41 package files, imports, SQL APIs/isolation, reordered projection,
  AEAD and SQLite backup/restore. That experiment preceded the final concurrent-
  integration renames; its wheel is not delivered as the full updated repository.

Tests ran on Python 3.13.5 with pytest 9.0.2 and pytest-asyncio 1.3.0, with warnings
as errors and only pytest-asyncio explicitly enabled. The reproducible local command:

```sh
PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 PYTHONDONTWRITEBYTECODE=1 \
python -m pytest -p pytest_asyncio.plugin -q -W error \
  -m 'not live and not postgres and not upstream and not mcp and not vault' \
  --junitxml=/outside/source/local.junit.xml
```

Counts from earlier stages overlap. Do not add them to 170 or combine them with
old phase-3 totals. On a complete checkout the selected repository test count will
be larger; this record is not that larger suite's result.

## Explicit unavailable gates

The runner was invoked for native SDK, PostgreSQL and Vault scopes. They returned
INCOMPLETE/UNAVAILABLE with exit 2, not PASS: a2a/Dapr/MCP/httpx2 and psycopg packages
were absent, as were configured disposable PostgreSQL and Vault test connections.
Docker/brokers/Dapr runtime were unavailable. The real dual-broker harness and
native tests are implemented but not executed here. Python 3.12, the full current
repository suite, actual encrypted broker behavior, real pg_dump/restore, native
MCP/A2A/Dapr interoperability, full lint/types, image builds, transitive lock/SBOM/
vulnerability scans and production HA/DR acceptance remain unestablished.

The runner requires every named full gate exactly once and rejects empty or
skipped JUnit. Explicit HTTP/protocol denial is needed for negative authorization
checks; a connection failure cannot satisfy a security gate. Neither local nor
full implementation verdicts constitute signed upstream/production certification.

## Publication

This evidence was generated before the final repository publication step. The
published integration is the repository HEAD that contains this file and is based
on the exact remote base above by fast-forward, without force-pushing. Re-run the
acceptance gates from that checkout rather than treating this historical evidence
as proof of the final published tree.
