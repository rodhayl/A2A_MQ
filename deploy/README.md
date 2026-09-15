# Local deployment and live gates

Development only. No production identity, TLS, KMS or HA claim. All published host
ports bind loopback. Container listeners bind internally for sidecar access. Use
only synthetic tasks. The Compose project is named `a2a-mq-local`.

## Start

```sh
python -m pip install -e '.[dev]'
make up
python -m a2a_mq --text 'hello agents' --save /tmp/a2a-command.json
# On an ambiguous timeout, retry the same file; do not generate another ID:
python -m a2a_mq --retry-file /tmp/a2a-command.json
```

`make up` runs the stdlib generator, then Docker Compose. Generated manifests are
ignored by Git; their reviewed source is `generate.py`. Images: Dapr 1.18.2,
NATS 2.14.6-alpine and NATS-box 0.19.0. Python uses the 3.13-slim image family;
image digests/transitive dependencies are not yet locked for a production supply chain.

The initializer creates a file-backed limits-retention stream **before sidecars**.
It does not rewrite an existing stream; intentional retention/config changes need
an operator migration or disposable-volume reset. Retention is one day / 256 MiB /
100,000 messages; full storage rejects new publishes instead of silently evicting
old accepted work. Expiry still limits guarantees.

Worker A/B share one logical app ID, consumer pair and a single-host SQLite volume.
Observer A/B have distinct consumer pairs and volumes. Components include local
publish/subscribe scopes. These are not a substitute for broker credentials or
production authorization. The phase-2 checkpoint is deliberately a volatile demo;
phase 3 installs transactional processing behind the same endpoints.

## Inspect

Producer Dapr: `http://127.0.0.1:3500`; app health: `:18000/health/ready`.
Observer metadata: `:18001/internal/observations` and `:18002/internal/observations`.
Worker health: ports 18011/18012. NATS monitor: port 8222. No payload logs are needed.

## Real infrastructure gate

```sh
A2A_LIVE=1 python -m pytest -m live -v
```

This test **deletes this fixture's named volumes**, publishes before workers exist,
starts two worker replicas and checks both observers receive the expected events.
Use a disposable machine/project only. Phase 3 extends it with durable effect counts.
A transport mock cannot establish these broker properties. This environment had no
Docker/runtime binaries, so this live gate was not executed here.

Dapr retries callbacks 20 times at 1-second intervals before its configured dead
letter route. Permanent application errors are explicitly quarantined first.
Broker ack wait is 60 seconds and maxDeliver 20; exhausted broker deliveries require
operator inspection/advisories, not an assumption of automatic DLQ movement.
The protected reference transaction is short; phase-3 leases must be shorter than
the normal callback retry window. Long inference belongs in phase-4 workflows.

`make down` preserves volumes. `docker compose -f deploy/compose.yaml down -v`
destroys them. Database server and upstream SDK gates are separate from this fixture.
