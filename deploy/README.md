# Deployment Notes

This directory contains reference deployment configuration. It is not production-ready until the implementation phases add broker authentication/TLS, Dapr scopes, secret-store integration, HA settings and environment-specific state stores.

## Local JetStream

Current Dapr documentation defines the JetStream component type as `pubsub.jetstream`. The component supports durable consumer configuration including `durableName`, `queueGroupName`, `ackWait`, `maxDeliver`, `backOff`, `maxAckPending`, replicas and storage options.

Start a local NATS server with JetStream enabled, then create a stream covering the A2A subjects/topics used by the implementation. Exact subject names will be locked when Phase 2 transport code lands.

The checked-in `pubsub-jetstream.local.yaml` intentionally does **not** hard-code a global `durableName`/`queueGroupName`. Competing worker pools and fan-out observers require different stable consumer identities. Those settings should be generated/scoped per application role once the Dapr subscription layout is implemented and integration-tested.

## Production requirements

Before using these manifests in production:

- enable broker TLS/authentication;
- use a secret store for credentials;
- configure Dapr publish/subscribe scopes;
- configure broker account/subject permissions where applicable;
- use HA JetStream and persistent volumes appropriate to RPO/RTO;
- configure production workflow/task state store;
- configure OpenTelemetry export;
- define retention/storage quotas;
- define DLQ routing and operator permissions;
- test failure/recovery scenarios from `docs/TEST_PLAN.md`.

Reference: https://docs.dapr.io/reference/components-reference/supported-pubsub/setup-jetstream/
