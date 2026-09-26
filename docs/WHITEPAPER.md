# A2A-MQ: a narrow durable task transport for heterogeneous agents

## Abstract

A2A-MQ investigates whether existing durable messaging can reduce custom plumbing
for asynchronous agent collaboration. Multiple orchestrators publish to capability
or agent routes; worker replicas compete for commands and independent observers
receive results. A2A supplies the payload vocabulary, Dapr supplies the Pub/Sub API,
and JetStream supplies retained delivery. The proposed contribution is a small
interoperability profile with explicit identity, transaction, ordering and security
boundaries, not a new broker or a claim of protocol standardization.

This is an engineering proposal with an executable reference slice. It does not
report enterprise adoption, measured production reliability or a comprehensive
market absence proof. Actual validation belongs in `VALIDATION.md`.

## 1. Problem and research questions

An agent task can outlive its caller or receiving process. Direct RPC still can be
asynchronous and durable when backed by a server-side runtime; it is not inherently
incapable of long tasks. A queue is useful when offline workers, independent scaling,
back-pressure and fan-out are deployment requirements. The question is whether a
thin queue profile improves those operational properties without imposing a new
agent framework on every participant.

RQ1: can independent worker/observer groups share the bus without accidental task
loss or duplicate reference effects? RQ2: can a projection remain correct under
reorder, replay and restart? RQ3: how much interoperability code remains after
reusing Dapr? RQ4: what data stays visible even with future payload encryption?

## 2. Prior components and scope

The original primary-source comparison is archived at
`archive/planning/FOUNDATION_RESEARCH.md`. Dapr/JetStream is a project-specific
foundation choice, not a universal winner. Existing A2A runtimes,
Dapr Agents, workflow engines and gateways overlap substantially with this idea.
Claims about what no other project implements must be rechecked before marketing.

A2A 1.0 data objects are reused; the custom `task.snapshot` transport event carries
a complete canonical Task. The standard A2A bridge, cancellation, durable
input/auth continuation, discovery and optional Vault-backed payload protection are
implemented in the current distributed profile. Local-lite intentionally exposes a
narrower feature set.

## 3. Consistency model

Transport delivery is at least once. Let C be a command identity and E(C) the local
reference SQL effect. The unique effect key, fenced claim and atomic completion
transaction enforce at most one committed E(C). This says nothing about an arbitrary
external email, payment or tool call: those require destination idempotency or an
explicit reconciliation workflow. The outbox converts a lost publication window
into possible duplicate events. Observer deduplication handles those duplicates.

Task snapshots are ordered by one authority's integer version. Keeping a full
snapshot costs O(task size) per update but avoids a gap-buffering/event-sourcing
system in the first release. The projection accepts version jumps with reachable
states and rejects terminal rewrites. Delta/chunk streaming is a separate extension.

## 4. Security model

Topic scopes and broker identities enforce who receives traffic. Symmetric content
encryption is an optional additional boundary, not a replacement for authorization.
The optional protection profile uses per-message content encryption with Vault-backed
key wrapping for the receiving group. Shared-key
recipients can still forge group-authenticated content, so AEAD alone does not
prove the individual publisher. Workload signatures or authenticated ingress may
be needed across trust domains. Metadata, DB state and endpoint compromise remain
visible risks; see `SECURITY.md`.

## 5. Evaluation design

Use a deterministic, model-free echo effect before adding inference. Test all state
pairs, malformed input, duplicate identities, tenant collisions, lease takeover,
stale fencing tokens, transaction crash points, outbox publish/mark crashes,
snapshot permutations and bounded queries. The current validation suite separately runs real JetStream/RabbitMQ live probes,
verified-TLS PostgreSQL and Vault Transit gates. A mock passing still does not
establish broker or external-infrastructure behavior.

Measure publish-to-delivery and result lag separately from inference. Report
hardware, dependency versions, concurrency, payload size and retained backlog.
Earlier 50/200 ms numbers are hypotheses, not achieved SLOs. No throughput, SLA or production-certification claim is made without the
corresponding experiment and explicit evidence boundary.

## 6. Limitations and conclusion

The deterministic SQL fixture remains stronger and narrower than arbitrary external
agent side effects. SQLite is the local durability profile; verified PostgreSQL and
live broker tests cover the distributed acceptance boundary. Full snapshots trade
bandwidth for simpler recovery. The current implementation now includes the standard
A2A bridge, Workflow tier and optional Vault-backed protection, while explicit
certification booleans remain false. The highest-value work remains contracts,
evidence and deployment discipline rather than rebuilding brokers, workflow runtimes
or cryptography.
