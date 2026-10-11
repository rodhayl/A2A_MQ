# A2A-MQ

**Durable task transport for A2A agents, with failures you can reproduce.**

A2A-MQ connects the official A2A Python SDK to Dapr Pub/Sub and NATS JetStream.
It explores a practical question: what happens to a task when a process dies after
admission, a command is delivered twice, or an operator replays a dead letter?

This is an experimental engineering portfolio project, version **0.1.0**. It is an
A2A implementation with a custom queue envelope, not ACP or a new messaging standard.
It does not claim customers, production certification or arbitrary exactly-once
external effects. No license grant is supplied. Public-exposure review is tracked
separately in the [Publication checklist](docs/PUBLICATION.md).

## Start with the failure demo

The default Local-lite runtime uses exactly three product processes on Linux:

```text
Producer / official A2A client
        │ JSON-RPC, REST or SSE
        ▼
Python A2A-MQ app ── SQLite task ledger, inbox/outbox, Queue Lab
        │ Dapr Pub/Sub
        ▼
      daprd ── NATS JetStream ── delivery back to the app's reference worker
```

The deterministic worker supports Echo and Uppercase without a model, API key or
paid call. It is a reference consumer, not a demonstrated customer workflow.
The local app and worker share a process. Independently scalable workers belong to
the separate distributed profile.

```bash
git clone https://github.com/rodhayl/A2A_MQ.git
cd A2A_MQ
python3 -m venv .venv
. .venv/bin/activate
python -m pip install -c requirements/portfolio-constraints.txt -e '.[local]'
python -m pip check
python scripts/bootstrap_local_tools.py --output .local-validation/tools
python scripts/check_local_native_versions.py \
  --daprd .local-validation/tools/bin/daprd \
  --nats-server .local-validation/tools/bin/nats-server \
  --bootstrap-manifest .local-validation/tools/manifest.json \
  --json .local-validation/native-versions.json
python scripts/portfolio_demo.py \
  --tools-bin .local-validation/tools/bin \
  --work-dir .local-validation/portfolio-demo \
  --output .local-validation/portfolio-demo.json
```

The demo creates a fresh private profile, kills its own app at a named admission/ACK
boundary, restarts it, checks actual broker redelivery, verifies duplicate and
conflicting IDs, cancels paused work, and exercises real DLQ archival and authorized replay publication. It also
runs a separate official SDK client and records a small, scoped latency sample.
Existing profile/report paths are never overwritten. Read the
[demo guide](docs/PORTFOLIO_DEMO.md) for requirements, safety and exact limits.

For the browser console, use the [Getting Started](docs/GETTING_STARTED.md) guide.
Open `http://127.0.0.1:18000/lab/` only while your local runtime is running. Profile
files include signing keys and tokens: keep them private and outside commits.

## Engineering decisions worth inspecting

- Immutable message identity and content-conflict detection, scoped by tenant/consumer
- SQL ledger, inbox/outbox, terminal guards and fenced distributed execution
- Durable admission before asynchronous execution; explicit cancellation semantics
- Verified-principal authorization; payload fields never authorize a caller
- Archived dead letters and explicit, identity-preserving replay
- Public A2A bindings implemented by the official SDK; internal envelope kept separate
- Low-footprint local profile with Workflow-only features rejected explicitly

A queue gives at-least-once delivery. SQL protections cover the declared local
transactional boundary. Email, payment and other external effects still need a
stable destination idempotency key or reconciliation.

## Pick the right profile

| Profile | Purpose | Infrastructure |
| --- | --- | --- |
| Local-lite | Immediate reference tasks, failures and Queue Lab | Python, native Dapr/NATS, SQLite |
| Local Workflow | Durable waits, continuation, approval and timers | Local-lite plus Dapr Placement/Scheduler |
| Distributed | Separate API/worker/operations roles | PostgreSQL, Dapr Workflow, JetStream or RabbitMQ; optional Vault/telemetry |

For a simple synchronous request, ordinary HTTP may be easier to operate. This
project is useful when retained work, retries, recovery and audit boundaries matter.

## Evidence and limits

Start with [Current status](docs/STATUS.md), the
[compatibility matrix](docs/COMPATIBILITY.md) and the
[validation evidence index](docs/VALIDATION.md).
Each result identifies its source/profile. Older RC2, Kubernetes and resource
records remain historical; they do not certify the current candidate.

`productionCertified=false` and `upstreamBindingCertified=false`. Repository
verdict names are test labels, not external certifications. No SLA, general
throughput claim, Windows-native support claim or exactly-once external guarantee
is implied.

## Documentation and development

- [Documentation map](docs/README.md)
- [Architecture](docs/ARCHITECTURE.md) and [protocol](docs/PROTOCOL.md)
- [API examples](docs/API_EXAMPLES.md) and [configuration](docs/CONFIGURATION.md)
- [Operations](docs/OPERATIONS.md), [deployment](docs/DEPLOYMENT.md), [security model](docs/SECURITY.md)
- [Contributing](CONTRIBUTING.md), [security reporting](SECURITY.md), [changelog](CHANGELOG.md)
