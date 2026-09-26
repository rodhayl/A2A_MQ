# ADR 0008 - Official A2A SDK bridge and bounded task facade

Accepted for implementation, 2026-09-17. This is not a conformance certificate.

Use a2a-sdk 1.1.2 JSON-RPC, REST and SSE routes, protobuf models and error mapping.
The adapter implements send/get/list/cancel/subscribe and Agent Card discovery.
Default sends wait for terminal/input/auth states; explicit returnImmediately
returns durable admission. Cancellation returns CANCELED only after owner state
confirms it. Timeouts preserve admitted work. Streaming polls durable snapshots,
not process-local queues; reconnect obtains the latest durable full Task.

List queries are tenant/owner scoped, ordered by updated time and ID descending,
with bounded opaque keyset pages, context/state/timestamp filters, history bounds
and explicit artifact inclusion. Cursors are not authorization capabilities.

Pin issuer/audience/RSA public keys. Payload tenant/owner fields never authorize.
Workload policies govern read/send/cancel/approve. Duplicate credentials fail.
The external approval endpoint is application-specific and advertised in the
AUTH_REQUIRED status message. Push notifications are explicitly unsupported.

Bound JSON before SDK parsing and discard upstream diagnostic exception text.
Keep SQL tombstones to prevent retired request IDs from re-executing; quota includes
those tombstones. Require runtime completion and expired retention before retirement.

Executed: 62 SQLite/facade/workflow/HTTP-boundary tests, zero failures/skips;
one SDK workflow registration test excluded. Official SDK route tests are supplied
but require installed dependencies and independent interoperability execution.
