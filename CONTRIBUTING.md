# Contributing

The project is experimental and its licensing decision is pending. Do not assume
an open-source grant from repository visibility. Confirm contribution terms with
the owner before submitting material owned by somebody else.

Use Python 3.12+ and an isolated virtual environment:

```bash
python -m pip install -c requirements/portfolio-constraints.txt -e '.[dev,upstream,postgres,local,observability]'
python -m pip check
python tools/check.py
ruff check .
mypy
PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 python -m pytest -p pytest_asyncio.plugin \
  -p no:cacheprovider -q -W error -m 'not live and not postgres and not vault'
```

This command selects broker-independent tests; it is not a full infrastructure run.
Run the relevant live/PostgreSQL/Vault/deployment gates separately on disposable
infrastructure. Keep missing services as BLOCKED/NOT_RUN, never PASS.

Keep the schema and binding copies byte-identical. Test externally visible method
names against the pinned SDK service descriptor. Preserve tenant/owner checks,
immutable IDs, fencing, transactional outbox and terminal guards. Do not replace
Dapr Workflow with another scheduler in Local-lite.

Before a change, read `AGENTS.md`, `docs/STATUS.md` and the relevant ADR. Add a narrow
regression first; run the broader suite after the candidate is stable. Include exact
commands, source identity, results, exclusions and package hashes in a review.
Never commit local profiles, secrets, raw user payloads or generated build output.
