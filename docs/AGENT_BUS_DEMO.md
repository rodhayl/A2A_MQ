# Protected messages across topics

This bounded native demonstration follows a command from a producer process to a
separate deterministic worker, then fans the worker's task events out to two
independent observer processes. Dapr and NATS JetStream are real processes. Existing
A2A-MQ publisher, subscriber and encryption primitives carry the messages.

## Run

Use the Python 3.12+ `[local]` environment and verified native Dapr/NATS tools from
the [README setup](../README.md). No additional package, Docker, Vault, model or
paid API is required for this demonstration.

```bash
python scripts/agent_bus_demo.py \
  --tools-bin .local-validation/tools/bin \
  --work-dir .local-validation/agent-bus-demo \
  --output .local-validation/agent-bus-demo.json
```

The work directory and output must be new. Use another name for another run.
`--base-port` defaults to 24222. The fixture checks 17 loopback ports before starting:
the base broker port and four per-role ranges at base+10..13, +20..23, +30..33 and
+40..43. It refuses occupied ports and never stops their owners.

The harness starts one broker, four sidecars, three long-lived Python consumers and
short-lived producer processes. This is a separate component fixture; Local-lite's
three-process architecture is unchanged. Check available memory before starting.
It cleans up only its owned process groups on success, failure or interruption.
A cleanup failure fails the report even if message assertions already passed.

## What happens

1. Producer publishes an encrypted internal command on
   `a2a.v1.busdemo.command.skill.echo`.
2. The worker's protected subscriber decrypts and processes that command using the
   maintained deterministic Echo fixture. It publishes three encrypted task
   snapshots on `a2a.v1.busdemo.event.task`.
3. Observer A and observer B each receive those events through a separately named
   durable subscription. The report checks exact payload digests, message identity
   and distinct process identity.
4. Read-only JetStream inspection requires the sealed payload type and ciphertext
   structure for the real stored command/events. Unique synthetic plaintext canaries
   must be absent from these broker messages.
5. A fresh producer process resends the same immutable command using its existing
   SQLite sealed-byte cache. Accepted Dapr publications from two producer PIDs must
   have the original broker ciphertext digest. This does not claim a second physical
   delivery inside the broker deduplication window.
6. Observer B and its sidecar are stopped. Another command completes and observer A
   receives its results. Observer B is restarted and must receive its retained events.
7. Four deliberately invalid command messages traverse the real broker: plaintext
   downgrade, altered ciphertext, wrong receiver and wrong key. Each must reach
   quarantine with the expected reason, without entering the worker handler. The
   harness records handler entry before any worker effect so an effect failure cannot
   disguise an unauthorized invocation.

## Evidence

The reviewed final run on 2026-10-11 at 19:34:46 UTC passed all four scenario groups
and confirmed owned-process cleanup. All 17 fixture ports were independently
bindable afterward. It inspected eight positive protected broker messages (two
commands and six events), verified two independent event consumers and observed
recovery of the offline observer.

- [Sanitized real-run report](validation/evidence/20261011-agent-bus/report.json)
- [Candidate and artifact manifest](validation/evidence/20261011-agent-bus/manifest.json)
- [367 focused passing checks](validation/evidence/20261011-agent-bus/focused.junit.xml)

Harness SHA-256:
`784ebe4535b0e9b3a45683f6378d323f063064c0c4e0497590923911fc9c495c`.
Source-content SHA-256:
`7b249f711a72995eb5477a800c397dac76c72cdad1557bd3bff1c7daa535cf73`.
The manifest defines the source-digest framing and identifies the starting Git
commit plus uncommitted repair files; this digest is not a Git commit or a whole
repository digest. Every runtime/helper file hash is recorded in the report.
The tests, live demonstration and documentation edits are separate evidence scopes.
No full-suite or deployment acceptance is inferred from the focused count.

The first development run was followed by independent review. Two harness evidence
issues were corrected before the final run: cleanup could preserve a stale PASS,
and a completion receipt did not prove whether a failed handler had been entered.
Regression tests and a fresh real run cover those corrections; the earlier report
is not the final acceptance record.

## Exact protection boundary

The fixture uses `LocalKeyRing`, an explicit development-only symmetric key provider.
Command and event receiver groups have different keys; both authorized observers
share the event receiver-group key. Processes run under one local OS account. This
is not hostile-process isolation, recipient-specific public-key exchange or sender
signatures. The application endpoints can read plaintext.

The positive wire inspection concerns the selected valid encrypted command/events.
The intentionally injected plaintext negative is invalid traffic; its dead-letter
capture can retain that supplied plaintext. Payload encryption does not encrypt
all quarantine data, routing metadata, SQL state, Workflow history or backups.
See [payload protection](PAYLOAD_PROTECTION.md) for the larger trust model.

The producer directly uses the internal Envelope contract. This run does **not**
exercise the public A2A API, full `application:from_env`/ServiceApplication, Vault,
Workflow, PostgreSQL, RabbitMQ, arbitrary dynamic agents or external effects.
The [existing portfolio demo](PORTFOLIO_DEMO.md) separately exercises the official
A2A SDK and bounded recovery; those results are not merged into one encrypted
end-to-end product-acceptance claim.

The reference handlers remain a deterministic component fixture. This run does not
establish exactly-once business effects, production readiness, HA or capacity.

## Keep the fixture private

The work directory contains development keys, bearer tokens, private logs and local
state. Never commit it or publish raw logs. Only the reviewed sanitized report,
manifest and test summaries are suitable for sharing. After diagnosis, remove only
that specific disposable work directory when it is no longer needed.
