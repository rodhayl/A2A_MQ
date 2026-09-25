# ADR 0010 - Native SDK bindings and integrated operational roles

Status: implementation decision, 2026-09-17. Not an upstream conformance approval.

Use the official pinned A2A Python SDK's JSON-RPC, HTTP+JSON, Agent Card and streaming
routes. The adapter implements authorized durable task operations; the SDK owns
wire codecs, versions and HTTP framing. Do not describe the private Dapr/MQ
CloudEvent profile as a registered or certified upstream A2A transport binding.
Independent client tests and a formal upstream review remain external exit gates.

Use the official MCP SDK over the same TaskAPI, with a resource-specific audience
and per-call token verification. No session can retain another caller's authority.
Use Dapr's Workflow runtime for waits/timers/replay/fan-out. The application only
dispatches and reconciles durable records; it does not implement orchestration.

Separate roles: API admission; Workflow execution; independent projections; DLQ
operations. SQL namespaces are tenant plus logical consumer. APIs and their worker
pool share task scope; observers use different consumers. Real broker delivery
semantics remain backend-specific and require the same live tests on both brokers.

Preserve task/admission/event identities after payload retirement. Bounded quotas
are preferable to silently forgetting old deduplication state. Automated retention
requires terminal outcomes, runtime completion and confirmed publications. Archive
replay preserves bytes and identity and checks authoritative source hashes again.
Production encryption uses authorized KMS wrapping; plaintext fallback is not an
availability strategy. Restores consume private, hash-verified archive snapshots.

Local Compose provisions only disposable single-node infrastructure with generated
credentials. Kubernetes resources integrate with independently operated secure/HA
services and require their credentials, trust and policies. Static manifests or
local mocks do not prove production isolation, HA or disaster-recovery readiness.

The implementation gate keeps LOCAL_PASS, native SDK/service passes and FULL_PASS
separate. A missing mandatory gate is INCOMPLETE; a failed test is FAILED. Neither
FULL_PASS nor hashes constitute signed independent evidence or production approval.
