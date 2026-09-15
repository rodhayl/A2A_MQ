# Workflows

## 1. Basic asynchronous task

```mermaid
sequenceDiagram
    participant O as Orchestrator
    participant M as A2A-MQ Adapter
    participant B as Broker
    participant W as Worker
    participant P as Task Projection

    O->>M: SendMessage
    M->>B: publish message.send
    M-->>O: accepted / correlation
    B->>W: deliver command
    W->>P: persist SUBMITTED/WORKING
    W->>B: publish task.status WORKING
    W->>W: execute task
    W->>P: persist COMPLETED + artifacts
    W->>B: publish artifacts/status
    W-->>B: ack command
    B-->>M: events
    M-->>O: correlated task updates
```

## 2. Worker offline at submission

```mermaid
sequenceDiagram
    participant O as Orchestrator
    participant B as JetStream
    participant W as Worker
    O->>B: durable command
    Note over B: worker unavailable; command retained
    W->>B: reconnect durable consumer
    B->>W: deliver pending command
    W->>B: ack after durable acceptance/outcome
```

Expected property: no task loss merely because a worker was temporarily offline.

## 3. Competing workers

```mermaid
sequenceDiagram
    participant B as Broker
    participant W1 as Worker replica 1
    participant W2 as Worker replica 2
    B->>W1: command X
    Note over W1,W2: same durable consumer/group
    W1->>B: ack X
    Note over B,W2: W2 does not execute X
```

The application still implements deduplication because redelivery can happen after crashes/timeouts.

## 4. Crash and redelivery

```mermaid
sequenceDiagram
    participant B as Broker
    participant W1 as Worker A
    participant D as Dedup/Workflow Store
    participant W2 as Worker B

    B->>W1: command X
    W1->>D: claim X / durable workflow acceptance
    W1--xW1: crash before broker ack
    B->>W2: redeliver X
    W2->>D: check X
    D-->>W2: already accepted / resume outcome
    W2->>B: ack safely
```

## 5. Input required

```mermaid
sequenceDiagram
    participant O as Orchestrator/User
    participant B as Broker
    participant W as Durable Worker Workflow

    W->>B: task.status INPUT_REQUIRED
    B-->>O: event
    Note over W: workflow suspended durably
    O->>B: continuation message with same task/context
    B->>W: continuation
    W->>B: task.status WORKING
    W->>B: task.status COMPLETED
```

`AUTH_REQUIRED` follows the same pattern, with authorization material handled by secure references rather than plaintext secrets in task content.

## 6. Cancellation

```mermaid
sequenceDiagram
    participant O as Orchestrator
    participant B as Broker
    participant W as Worker/Workflow
    participant P as Projection

    O->>B: task.cancel(taskId)
    B->>W: cancellation request
    W->>W: stop at safe cancellation point
    W->>P: persist CANCELED
    W->>B: task.status CANCELED
    B-->>O: cancellation observed
```

Race rule: if completion becomes durable before cancellation is accepted, terminal `COMPLETED` wins. Terminal states are not rewritten by a later stale cancellation event.

## 7. Multi-orchestrator fan-out

```mermaid
sequenceDiagram
    participant W as Worker
    participant B as Broker
    participant O1 as Orchestrator A
    participant O2 as Orchestrator B
    participant A as Audit

    W->>B: task.status event
    B-->>O1: own subscription
    B-->>O2: own subscription
    B-->>A: restricted audit subscription
```

Each observer has an independent consumer identity, so one observer receiving an event does not consume it on behalf of the others.

## 8. Orchestrator fan-out/fan-in

```mermaid
sequenceDiagram
    participant O as Parent Orchestrator
    participant B as Broker
    participant A as Agent A
    participant C as Agent C

    O->>B: child task A
    O->>B: child task C
    B->>A: execute A
    B->>C: execute C
    A->>B: completed A
    C->>B: completed C
    B-->>O: event A
    B-->>O: event C
    O->>O: durable join/fan-in
    O->>B: parent completion/result
```

The parent workflow stores child task IDs/reference relationships using A2A-compatible task/message metadata.

## 9. Encrypted command

```mermaid
sequenceDiagram
    participant O as Producer
    participant K as KMS/Key Provider
    participant B as Broker
    participant W as Authorized Worker

    O->>K: request/wrap content key
    O->>O: encrypt A2A payload using AES-GCM
    O->>B: publish routing metadata + ciphertext
    Note over B: broker cannot read payload
    B->>W: encrypted command
    W->>K: unwrap content key
    W->>W: authenticate/decrypt payload
    W->>W: process normally
```

## 10. Poison message and DLQ

```mermaid
sequenceDiagram
    participant B as Broker
    participant W as Worker
    participant D as DLQ
    participant Op as Operator

    B->>W: command
    W-->>B: retryable failure
    B->>W: redelivery (bounded)
    W-->>B: deterministic permanent failure
    W->>D: dead-letter envelope + failure metadata
    W->>B: ack original
    Op->>D: inspect
    Op->>B: controlled replay with explicit message identity policy
```

## 11. Standard A2A bridge

```mermaid
sequenceDiagram
    participant C as Standard A2A Client
    participant H as A2A HTTP+JSON Bridge
    participant B as MQ
    participant W as MQ-backed Agent

    C->>H: POST message:send
    H->>B: message.send command
    B->>W: command
    W->>B: task events
    B-->>H: correlated events/projection
    H-->>C: canonical A2A response
```

For streaming, the bridge maintains the client stream while consuming correlated task events. If the bridge restarts, the client follows normal A2A reconnection/subscription semantics; task durability does not depend on the stream connection.
