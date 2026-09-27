# Disposable Compose deployment fixture

The `deploy/` tree is a **development/live-gate fixture**, not the recommended
v0.1.0-rc2 local runtime and not production infrastructure.

For a normal first run use:

- [../docs/GETTING_STARTED.md](../docs/GETTING_STARTED.md)
- `a2amq-local`
- the repository-pinned Dapr 1.18.3 / NATS Server 2.15.0 native bootstrap.

This Compose fixture remains useful for multi-role broker behavior and historical
acceptance scenarios.

## Security boundary

All published host ports bind loopback. Use only synthetic data.

The fixture does not establish production:

- identity;
- TLS;
- secret management;
- KMS;
- HA;
- supply-chain pinning.

Generated Compose/manifests are ignored by Git; `deploy/generate.py` is the reviewed
source.

## Current fixture versions

The current generator intentionally renders:

- Dapr container image `daprio/daprd:1.18.2`;
- NATS image `nats:2.14.6-alpine`;
- NATS-box `natsio/nats-box:0.19.0`;
- Python application image built from the repository Dockerfile.

These are the versions encoded by this separate disposable fixture. They are **not**
the RC2 native Local-lite provenance versions.

Do not update these values merely to match Local-lite documentation without rerunning
the fixture's relevant live acceptance.

## Start

```bash
python -m pip install -e '.[dev]'
make up

python -m a2a_mq --text 'hello agents' --save /tmp/a2a-command.json
```

On an ambiguous timeout, retry the same immutable command rather than generating a
new identity:

```bash
python -m a2a_mq --retry-file /tmp/a2a-command.json
```

The Compose project name is `a2a-mq-local`.

## Topology

The fixture creates:

- one file-backed JetStream server;
- a producer role;
- two worker containers sharing the logical worker identity/state;
- two independent observer roles;
- a Dapr sidecar per application role.

The stream is created before sidecars and uses bounded limits retention. An existing
stream is not silently rewritten; configuration changes require an intentional
migration or disposable-volume reset.

Worker replicas share one logical consumer. Observer A/B use distinct durable
consumer identities so they each receive task events independently.

## Inspect

Useful loopback endpoints include:

- producer Dapr: `http://127.0.0.1:3500`;
- application health: `:18000/health/ready`;
- observer metadata: `:18001/internal/observations` and
  `:18002/internal/observations`;
- worker health: `:18011` and `:18012`;
- NATS monitoring: `:8222`.

Avoid logging payloads/secrets to inspect the fixture.

## Live gate

```bash
A2A_LIVE=1 python -m pytest -m live -v
```

The live test is destructive to this fixture's named volumes. Use a disposable
machine/project.

It validates retained delivery and the intended worker/observer consumer shape. A
transport mock cannot establish those broker properties.

Dapr callback retry and JetStream delivery limits remain explicit in the generated
resources. Permanent application errors use the quarantine path; exhausted/poison
delivery requires operator inspection rather than assuming invisible successful
processing.

## Stop

```bash
make down
```

This preserves volumes.

To destroy the fixture and its named volumes:

```bash
docker compose -f deploy/compose.yaml down -v
```

## Production

Do not deploy this fixture as production.

Production application resources are rendered by `deploy/production/generate.py`
and require administrator-owned secrets/infrastructure plus an immutable image
digest. See [../docs/IMPLEMENTED_STACK.md](../docs/IMPLEMENTED_STACK.md).
