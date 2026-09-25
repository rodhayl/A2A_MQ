# ADR 0009 - Durable operations and request-scoped MCP

Date: 2026-09-17. Implementation extension to the existing TaskLedger/TaskAPI.

Retirement is transactional, opt-in and scoped. It requires SQL terminal status,
confirmed Dapr termination, no undelivered controls and all task events published.
It retains identity tombstones so an old command cannot execute again. Tombstones
remain within the admission-record quota. This is not unlimited retention or a
promise to delete every external copy, backup, broker message or audit record.

DLQ capture preserves stable CloudEvent fields and hashes. Replay requires an
operator with replay permission, an allowlisted route, complete captured bytes,
matching metadata and unexpired validated plaintext. Encrypted replay requires a
configured decoder and sends the original ciphertext. Replay never invents a new
message ID; corrected/new business requests use the public submission API.

SQLite backup uses its online backup API, a checked temporary file and an atomic
no-overwrite install. Restore is an offline whole-database administrator operation,
not a tenant HTTP endpoint; the separately retained SHA-256 must match.

MCP uses official mcp==2.2.0, not a new protocol server. Authentication is the SDK
resource-server integration; resource-specific JWT audience is mandatory. Tools
reverify the SDK request token and delegate to TaskAPI. Caller/tenant identity is
never a tool argument, and no principal survives on an MCP session. Legacy HTTP
sessions are disabled. Client tool annotations are hints, not authorization.

The MCP host must enter the SDK session-manager lifespan. The adapter exposes
submit/get/list/continue/cancel/approve, not arbitrary code or model calls. A SQL
facade test does not establish official MCP wire interoperability.

Primary API references checked:
- https://py.sdk.modelcontextprotocol.io/run/authorization/
- https://py.sdk.modelcontextprotocol.io/run/asgi/
- https://py.sdk.modelcontextprotocol.io/run/deploy/
- https://py.sdk.modelcontextprotocol.io/servers/tools/
