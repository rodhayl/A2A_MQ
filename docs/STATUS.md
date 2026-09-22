# Source and acceptance status - 2026-09-22

## Current branch

Branch: `implementation/production-hardening-20260916`.

Source head before this status-only wrap-up:
`a5a1ccb25e2bad7f7023249b61bad2ce0ae37e7e`.

`main` remains unchanged at
`66c17153d7ec75b9ad85808ef141b9dba43a9d91`.

Package version remains **0.1.0**. No merge to main, release, production
certification or upstream binding certification is implied by this branch.

## Current implementation

The source now contains substantially more than the 2026-09-17 reference service.

### Lightweight local product

The default local product is intentionally the small three-process topology:

- one Python A2A-MQ process;
- one native `daprd`;
- one native `nats-server`;
- SQLite application state.

PostgreSQL, Redis, Docker, Kubernetes, Vault, OTLP, Placement and Scheduler are
not default local-lite requirements.

Implemented local support includes:

- `a2a_mq.local_lite:from_env`;
- `a2amq-local` prepare/run launcher;
- atomic private profile generation;
- bounded/fail-closed profile and secret reads;
- explicit loopback-port preflight;
- explicit Dapr internal gRPC port;
- least-privilege child environments;
- core, Python-product and native-stack resource probes;
- peak/steady RSS, CPU and per-role native measurements;
- private pinned native bootstrap for Dapr 1.18.3 all-components and
  NATS Server 2.15.0;
- bootstrap provenance, SHA-256 and exact archive/binary-size evidence.

The optional local-workflow tier adds Placement + Scheduler only when native Dapr
Workflow semantics are required. It remains separate from local-lite economics.

### Distributed / production-like source

Source is present for the role-separated integrated stack and related production
capabilities, including:

- SQLite and PostgreSQL stores;
- Dapr Pub/Sub with JetStream and RabbitMQ profiles;
- Dapr Workflow;
- standard A2A JSON-RPC / HTTP+JSON bridge and official SDK contracts;
- canonical authenticated MCP server;
- owner/tenant authorization and cancellation/continuation/approval flows;
- independent observers and operations/DLQ/replay paths;
- Vault Transit payload protection;
- observability/operations modules;
- deployment generators and production environment factories;
- backup/restore and retention/maintenance support.

Presence of source is not equivalent to a passed external acceptance gate.

## Important hardening added in this iteration

The current branch also includes focused fixes around:

- local resource-measurement correctness and warmed Python-product economics;
- Dapr/NATS bootstrap provenance and native-tooling cost evidence;
- local secret isolation and deterministic port ownership;
- production PostgreSQL DSN wiring for non-root Kubernetes pods;
- public health endpoints compatible with Kubernetes probes while callbacks stay
  app-token protected;
- Dapr Kubernetes secret-store behavior with ServiceAccount token automount disabled;
- canonical MCP runtime use in production composition;
- PostgreSQL acceptance DSN alignment and explicit disposable-CI insecure override;
- MCP marker classification and MCP 2.x `httpx2` prerequisite;
- official SDK cancellation and MCP authentication-denial coverage;
- integrated Dapr runtime/control-plane alignment on **1.18.3**;
- required Vault startup validation;
- independent send/read authorization capabilities;
- capacity reservation before durable stream admission;
- SQLite backup destination symlink-race protection.

## Evidence that exists

Historical evidence must stay attached to the SHA it actually tested.

The independent local audit in
`docs/validation/local-audit-20260922.md` established selected launcher/probe
regressions and a lightweight core observation. It does **not** certify the current
branch head after subsequent fixes.

The owner's earlier isolated validation also established that the core Python +
SQLite design was comfortably inside the configured local resource budget, but
that run could not execute native Dapr/NATS because those binaries were absent at
the time. The repository now contains a private pinned bootstrap specifically so
the next target-machine run can measure the intended stack without installing
host packages or using Docker.

## Gates still requiring real execution

The following remain execution/evidence gates rather than missing source modules:

1. Run the current SHA's core and warmed Python-product economic probes on the
   target low-power machine.
2. Run the current SHA's selected local regression files in the private venv.
3. Bootstrap the pinned private Dapr/NATS binaries and measure the real native
   three-process local-lite stack on that machine.
4. Execute official SDK, PostgreSQL verified-TLS, Vault and both disposable
   broker gates when their explicit prerequisites are available.
5. Execute Ruff, mypy, dependency checks, packaging/install smoke and the full
   repository regression suite on the final SHA.
6. Run production/deployment acceptance separately from local-lite economics.

Until those gates are executed on the current SHA, full acceptance remains
**INCOMPLETE** and `productionCertified=false`.

## Hosted CI status

Recent GitHub Actions push runs complete as failures in roughly five seconds with
no job steps, logs or uploaded artifacts exposed by the available GitHub interface.
That is consistent with failure before runner execution and is not evidence that
repository tests ran and failed. The exact platform-side annotation is not
available through the connector, so no more specific cause is claimed.

Hosted CI therefore remains **not passed** and must be rerun/diagnosed separately.

## Original snapshot recovery

The historical staged handoff at
`e80096da84190a8af9aa044a4bd97b0123fca4ad` expected bundle SHA-256
`47a2c5418ec35d0d1c5fbcbb0fa4cd502b8479760b935a3b94ed01efe93965e9`
and final tree `79521dec0000e246baaab5bf4129841aa4799914`.

Accessible recovery fragments contain literal ellipsis corruption/incompleteness;
that exact tree has not been checksum-verified or reconstructed. Do not fabricate
or silently substitute recovery bytes. Current implementation work proceeds from
the accessible tested branch instead.

## Next machine handoff

Use `PROMPT_AGENT_LOCAL_FOLLOWUP.md` for the next bounded owner-machine run.
It is intentionally local-first, venv-based and non-containerized until the
lightweight gates pass.
