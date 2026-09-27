# A2A API examples

The files under `examples/a2a/` are small JSON-RPC request examples for the official
A2A surface exposed by A2A-MQ.

They are examples, not a new client SDK.

## Setup

For Local-lite:

```bash
TOKEN=$(cat .local-validation/local-lite/client.jwt)
URL=http://127.0.0.1:18000
```

Common headers:

```text
Authorization: Bearer <token>
A2A-Version: 1.0
Content-Type: application/json
```

## SendMessage

```bash
curl -sS -X POST "$URL/rpc" \
  -H "Authorization: Bearer $TOKEN" \
  -H "A2A-Version: 1.0" \
  -H "Content-Type: application/json" \
  --data-binary @examples/a2a/send-message.json
```

Use a new `messageId` for new work. On an ambiguous retry of the same logical
request, keep the same immutable identity/content.

## GetTask

Replace `replace-with-task-id` in:

`examples/a2a/get-task.json`

then:

```bash
curl -sS -X POST "$URL/rpc" \
  -H "Authorization: Bearer $TOKEN" \
  -H "A2A-Version: 1.0" \
  -H "Content-Type: application/json" \
  --data-binary @examples/a2a/get-task.json
```

## ListTasks

```bash
curl -sS -X POST "$URL/rpc" \
  -H "Authorization: Bearer $TOKEN" \
  -H "A2A-Version: 1.0" \
  -H "Content-Type: application/json" \
  --data-binary @examples/a2a/list-tasks.json
```

## CancelTask

Replace the task ID in `examples/a2a/cancel-task.json` before sending it.

Cancellation remains subject to owner/scope and terminal-state rules.

## SendStreamingMessage

`examples/a2a/send-streaming-message.json` exercises the official A2A streaming
method. The HTTP response is SSE, so use:

```bash
curl -N -sS -X POST "$URL/rpc" \
  -H "Authorization: Bearer $TOKEN" \
  -H "A2A-Version: 1.0" \
  -H "Content-Type: application/json" \
  --data-binary @examples/a2a/send-streaming-message.json
```

## SubscribeToTask

Subscription requires a **non-terminal** existing task.

Replace the ID in `examples/a2a/subscribe-task.json`, then:

```bash
curl -N -sS -X POST "$URL/rpc" \
  -H "Authorization: Bearer $TOKEN" \
  -H "A2A-Version: 1.0" \
  -H "Content-Type: application/json" \
  --data-binary @examples/a2a/subscribe-task.json
```

Terminal tasks are not converted into fake subscriptions.

## Browser alternative

Queue Lab exposes the same methods plus raw JSON-RPC editing at:

```text
http://127.0.0.1:18000/lab/
```

See [QUEUE_LAB.md](QUEUE_LAB.md).
