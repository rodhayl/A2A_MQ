# Reproduce a task failure

This demo uses the existing Echo/Uppercase consumer and real Dapr Pub/Sub + NATS
JetStream. A synthetic invoice sentence is only sample text; there is no document
extraction product, approval workflow, LLM call or customer adoption claim.

## Before running

Use Linux amd64/arm64, Python 3.12+, the repository's dependency constraints and
verified native Dapr 1.18.3 / NATS 2.15.0 archives. Follow the commands in
[README](../README.md). The tested environment is recorded in
[validation](validation/portfolio-20261009.md); another OS is not automatically validated.

Stop your own existing Local-lite instance first. Required loopback ports are
18000, 3500, 50001, 50002 and 4222; the actual launcher checks its declared list.
The demo refuses conflicts rather than stopping the process that owns a port.
The harness rechecks exact retained archive/binary provenance and executable permissions.
Work/profile and output paths must not already exist. Use a new name for another run.

## What happens

1. Create an owner-private disposable Local-lite profile. Credentials stay there.
2. Start the three-process runtime with its existing `unacked_delivery` fault hook.
3. Send one Uppercase task over A2A JSON-RPC. The hook kills the app after durable
   broker admission and before its acknowledgement to Dapr.
4. Stop only the demo's remaining sidecars, restart the same profile without the
   killing hook and observe at least two real broker deliveries and one terminal record.
5. Resend identical message/content: recover the same task. Reuse its message ID
   with changed content: require an invalid-parameters error.
6. Pause reference-worker delivery, submit and cancel a task, then resume.
7. Use Queue Lab's explicit Demo DLQ control, inspect the archive and replay through
   the authorized operations API and real transport. Confirm publication and an unchanged terminal snapshot at that instant. Consumer
   receipt/execution of that replay is not verified: broker deduplication may retain
   the already-published message identity. This is intentional quarantine, not an
   exhausted-retry or end-to-end replay-consumption test.
8. Use the official SDK client factory in the producer process for send/get against
   the running app. SDK route/isolation/stream tests are a separate suite.
9. Measure 20 warm, sequential task round trips by default; stop only owned processes.

The harness briefly shortens `ackWait` from 60s to 4s in its freshly generated demo
profile. It does not edit an existing deployment. Failure hooks are never enabled
in normal runtime launches. A fresh report records a failed run as failed.

## Reading the result

`status=PASS` applies only to those steps on the source content hash and environment
in that report. The source hash covers runtime Python/JSON, package metadata and
this demo script; it is not a Git commit or a whole-repository history digest.

The small latency sample includes HTTP and 50ms completion polling. Concurrency is
one, payloads are synthetic, caches are warm and earlier demo actions share the same
profile. It is not a capacity, saturation, comparative or production benchmark.
No unmeasured RSS/CPU figures are supplied by this harness.

The demo proves no arbitrary crash-recovery guarantee and no exactly-once external
side effect. It does not exercise Workflow, PostgreSQL, Vault, RabbitMQ, Kubernetes,
GUI accessibility or complete official A2A conformance.

## Safety and cleanup

The harness creates a new process group and signals only that owned group. Its
`finally` path stops it after success, failure, Ctrl-C or parent SIGTERM. It does not use global `pkill`,
change firewall settings or kill a pre-existing port owner. The launcher keeps the
app, sidecar and broker on loopback.

The profile includes JWT signing material, bearer tokens, broker tokens, databases
and private logs. Never attach or publish the profile, `.local-validation`, raw
runtime logs or an unreviewed terminal recording. Review only the sanitized JSON
report for sharing. Keep the profile for local diagnosis, then remove only that
specific disposable directory when you no longer need it. See [Security](SECURITY.md).
