# Operations Guide

## 1. Operational principles

- broker persistence is not a substitute for application idempotency;
- task queries use the projection store, not broker scans;
- consumer identities must remain stable across restarts;
- retention, dedup retention and key-retention windows must be designed together;
- DLQ is an operational queue, not a trash can;
- production deployments must have explicit resource bounds.

## 2. Core SLO indicators

Track at minimum:

- publish success/error rate;
- publish-to-consume latency;
- consumer lag/backlog;
- in-flight command count;
- processing duration;
- retry/redelivery count;
- duplicate suppression count;
- DLQ depth/age;
- task completion/failure/cancel rates;
- projection conflict/stale-event count;
- broker/storage availability;
- decryption/auth failures when encrypted mode is enabled.

## 3. Alert candidates

- oldest command age above service threshold;
- sustained DLQ growth;
- zero active consumers for a critical route;
- high redelivery ratio;
- task failure spike;
- projection write failures;
- broker storage near capacity;
- repeated authorization/decryption failures;
- key version nearing retirement while retained messages still reference it.

## 4. Retention sizing

For each route define:

- expected publish rate;
- mean/max payload size;
- maximum acceptable worker outage;
- replay/debug window;
- maximum command expiry;
- dedup record retention;
- encryption key decrypt-retention.

Invariant: do not retain replayable commands longer than the system retains enough idempotency/key material to process them safely.

## 5. DLQ process

Every DLQ item should include, outside sensitive payload content:

- original message ID;
- route;
- operation;
- task/context IDs when allowed;
- failure class;
- attempt count;
- first/last failure timestamp;
- trace ID;
- original envelope or an encrypted/secured reference to it.

Replay must be explicit:

- **same logical command replay** reuses original identity when testing redelivery semantics;
- **new logical attempt** gets a new message ID and references the old message through `causationId`/operator metadata.

The replay CLI must force the operator to choose which behavior is intended.

## 6. Graceful shutdown

Consumers should:

1. stop accepting new deliveries;
2. finish or safely checkpoint in-flight work up to a bounded timeout;
3. leave uncompleted commands unacked for redelivery;
4. flush telemetry;
5. terminate.

Never ack all in-flight messages during shutdown just to empty the client buffer.

## 7. Deployment changes

For rolling upgrades:

- envelope changes must remain backward-compatible within major version;
- consumers should be upgraded before producers begin using new optional fields that require support;
- schema major-version changes require separate routes or an explicitly staged compatibility window;
- task projection migrations must be backward/forward compatible during rolling deployment where possible.

## 8. Broker migration

Dapr keeps the application boundary portable, but broker semantics differ. Before switching backend:

- run the complete integration/conformance suite;
- verify competing-consumer behavior;
- verify redelivery timing;
- verify retention and DLQ behavior;
- verify topic/ACL mapping;
- benchmark payload limits and latency;
- perform a staged cutover rather than assuming configuration equivalence.

## 9. Backup and recovery

Back up persistent task projection/workflow state according to backend guidance. Broker streams with business-critical pending commands must have an HA/backup strategy matching required RPO/RTO.

Recovery drill must test:

- state store restore;
- broker restart/cluster failover;
- workers resuming pending commands;
- no unintended duplicate side effects;
- encrypted historical messages still decrypt when required.

## 10. Debug workflow

Given a `taskId`:

1. query task projection;
2. obtain latest state version/event ID;
3. search structured logs by task/message/correlation IDs;
4. inspect traces;
5. inspect relevant consumer backlog/redelivery statistics;
6. inspect DLQ only if failure classification indicates it;
7. avoid manually republishing payloads until idempotency implications are known.

## 11. Capacity controls

Configure:

- max broker message size;
- max inline A2A artifact size;
- worker concurrency;
- max in-flight messages;
- workflow fan-out limits;
- retry attempts/backoff;
- task history limit;
- task/list page size;
- broker storage quotas;
- log sampling/retention.

Defaults should favor safety over maximum throughput.
