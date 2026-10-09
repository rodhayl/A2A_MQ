# Compatibility and guarantee matrix

## Protocol and implementation versions

| Layer | Candidate contract | Verification boundary |
| --- | --- | --- |
| Public protocol | A2A 1.0 | Official SDK models/routes and repository tests; no independent certification |
| Python A2A SDK | `a2a-sdk[http-server]==1.1.5` | See `pyproject.toml`, descriptor regression and native adapter tests |
| Queue envelope | A2A-MQ envelope 1.0 | Custom transport contract, separate from A2A; schema copies must match |
| Dapr runtime/Python SDK | 1.18.3 | Native binary provenance and separately identified local tests |
| Default broker | NATS Server 2.15.0 / JetStream | Exact pinned archive; real local demo |
| Local persistence | Python SQLite | Single-host application state |
| Optional profiles | Workflow, PostgreSQL, RabbitMQ, Vault, MCP | Separate dependencies/infrastructure/tests; never inferred from Local-lite |

The maintained upstream references are the [A2A 1.0 specification](https://a2a-protocol.org/v1.0.0/specification/)
and [SDK 1.1.5 source](https://github.com/a2aproject/a2a-python/tree/v1.1.5).
This candidate makes no blanket claim about A2A 1.0.1, future SDK releases or other
language clients. See [candidate evidence](validation/portfolio-20261009.md).

## Public operations

| JSON-RPC name | REST binding | Behavior |
| --- | --- | --- |
| SendMessage | POST `/message:send` | Durable admission; `returnImmediately` selects admission response |
| SendStreamingMessage | POST `/message:stream` | SSE snapshots/events |
| GetTask | GET `/tasks/{id}` | Owner/tenant-scoped state, history/artifacts |
| ListTasks | GET `/tasks` | Scoped filters and bounded keyset pages |
| CancelTask | POST `/tasks/{id}:cancel` | Confirms committed cancellation; terminal tasks reject cancellation |
| SubscribeToTask | POST `/tasks/{id}:subscribe` | Existing non-terminal task; current Task first, then changes |
| GetExtendedAgentCard | GET `/extendedAgentCard` | Authenticated extended card |
| Create/Get/List/DeleteTaskPushNotificationConfig(s) | SDK push-config routes | Explicitly unsupported; `pushNotifications=false` |

Discovery is `/.well-known/agent-card.json`; JSON-RPC is `/rpc`. SSE is a streaming
response mechanism, not an additional queue protocol. No gRPC public binding is
advertised. Legacy v0.3 compatibility is disabled in both SDK route factories.

The old binding inventory used slash-style labels such as `message/send`. The
candidate aligns its inventory with actual A2A 1.0 method names. It does not change
the wire dispatcher: the SDK already rejected those legacy method names.

[A2A §3.1.6](https://a2a-protocol.org/v1.0.0/specification/#316-subscribe-to-task)
and §9.4.6 require terminal subscription to return `UnsupportedOperationError`.
The baseline bridge and focused bridge test already matched this rule. An older
native-adapter assertion expecting a terminal stream was corrected; runtime behavior
was preserved.

## Authorization, errors and limits

Verified RS256 JWT principal establishes tenant, owner and scopes. Tenant/owner text
inside a payload does not authorize access. Foreign tasks are hidden or denied;
invalid/missing credentials are unauthorized. SDK error mappings include task-not-found,
not-cancelable, unsupported operation and invalid parameters. The operations API is a
separate administrative surface; see [its profile contracts](OPERATIONS.md).

A stream reconnect obtains the latest durable full task, not a guarantee to replay
all historical intermediate events. Authorization is checked at request start;
immediate revocation of already-open streams is not claimed.

Local-lite permits immediate Echo/Uppercase only. Workflow continuation, durable waits,
approval and timers require the Workflow tier and fail closed in Local-lite.
At-least-once transport plus local SQL idempotency is not exactly-once for external
payments, email or tools. DLQ replay preserves task identity rather than creating new
business intent.
