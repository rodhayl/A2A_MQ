# Distributed deployment guide

Operations endpoints differ by runtime: consult the [profile-to-API mapping](OPERATIONS.md#choose-the-operations-surface-by-entry-point) before using the admin CLI or replay API.

This guide describes the boundary between what A2A-MQ renders and what an operator
must provide. It is intentionally not a Kubernetes/PostgreSQL/NATS/RabbitMQ
administration tutorial.

For a first evaluation, use Local-lite. Use this guide only when you need the
role-separated distributed profile.

## Responsibility split

| Resource | A2A-MQ renderer | Operator |
| --- | ---: | ---: |
| API/worker/DLQ Deployments | yes | applies/operates |
| API Service | yes | exposes/ingresses |
| Dapr Pub/Sub components | yes | broker infrastructure + secrets |
| Dapr Workflow state component | yes | PostgreSQL state service + secret |
| PodDisruptionBudgets | yes | cluster capacity |
| JWT public-key ConfigMap | yes | issuer/private signing infrastructure |
| application PostgreSQL | no | yes |
| broker cluster | no | yes |
| Dapr control plane | no | yes |
| TLS/CA/identity provider | no | yes |
| production secrets | no | yes |
| image registry/build pipeline | no | yes |
| ingress/network policy | no | yes |

## 1. Build and publish an immutable image

Use `deploy/production/Dockerfile` or an equivalent controlled build pipeline.

The renderer requires an image reference containing a digest:

```text
registry.example/a2amq@sha256:<64-hex-digest>
```

A mutable tag alone is rejected.

A new image is a new deployment artifact and needs its own image-bound validation.

## 2. Provide PostgreSQL

The application roles use shared PostgreSQL in the production topology.

Create Kubernetes Secret:

```text
a2amq-postgres
  key: dsn
```

The Dapr Workflow state component separately references:

```text
a2amq-workflow-state
  key: connection-string
```

The renderer does not create a database server or credentials.

Use verified TLS in real deployments.

## 3. Provide the broker

Choose one renderer profile:

```text
jetstream
rabbitmq
```

### JetStream secret

The generated JetStream components expect Secret `nats` with:

```text
url
token
```

The production broker must provide the durability/TLS/replication properties required
by your deployment.

### RabbitMQ secret

The generated RabbitMQ components expect Secret `rabbitmq` with:

```text
host
ca-cert
client-cert
client-key
```

Earlier live RabbitMQ acceptance exists, but RabbitMQ Kubernetes deployment parity
has not been separately certified.

## 4. Provide Dapr

The cluster must already have an operator-managed Dapr control plane suitable for the
deployment, including Placement/Scheduler for Workflow use.

Application sidecars use:

- Dapr API token Secret `dapr-api-token`, key `token`;
- app callback token Secret `app-api-token`, key `token`.

The generated workloads expect colocated sidecars.

## 5. Provide JWT identity

Prepare:

- HTTPS issuer URL;
- key ID;
- PEM **public** key for that key ID;
- external/private signing infrastructure for client tokens.

The renderer copies only the public key into `a2amq-jwt-public-key`.

Never place the production private signing key in the generated API workload.

## 6. Render manifests

Example:

```bash
python deploy/production/generate.py \
  --namespace a2amq \
  --tenant demo \
  --broker jetstream \
  --image 'registry.example/a2amq@sha256:<digest>' \
  --public-url https://a2amq.example \
  --issuer https://identity.example \
  --jwt-kid production-2026 \
  --jwt-public-key /secure/a2amq-public.pem \
  --output /tmp/a2amq-production.json
```

The output is secret-free application/Dapr resource configuration. Review it before
applying.

## 7. Validate server-side before applying

Where supported:

```bash
kubectl apply --dry-run=server -f /tmp/a2amq-production.json
```

Then:

```bash
kubectl apply -f /tmp/a2amq-production.json
```

## 8. Check rollout

```bash
kubectl -n a2amq rollout status deployment/a2amq-api
kubectl -n a2amq rollout status deployment/a2amq-worker
kubectl -n a2amq rollout status deployment/a2amq-dlq
```

Confirm application and Dapr sidecar containers are ready.

## 9. Check health before sending work

A temporary operator port-forward is useful for a first check:

```bash
kubectl -n a2amq port-forward service/a2amq-api 18000:80
```

Then:

```bash
curl -fsS http://127.0.0.1:18000/health/live
curl -fsS http://127.0.0.1:18000/health/ready
```

The public production endpoint itself should be HTTPS; the port-forward is only an
operator diagnostic path.

## 10. Send the first authenticated A2A request

Use a JWT issued by your configured identity system with the intended tenant/audience
and scopes. Do not reuse Local-lite's generated signing material.

Request examples are in [API_EXAMPLES.md](API_EXAMPLES.md).

Verify:

- admission succeeds for the intended owner/tenant;
- task completes;
- a foreign owner/tenant is denied;
- operations/DLQ access requires the intended operator scopes.

## 11. Upgrade

For every application build:

1. build/push a new immutable digest;
2. render manifests with the new digest;
3. validate the rendered output;
4. execute the required deployment gate for the new artifact;
5. roll out normally.

Do not move an old validated image verdict onto a new digest.

## 12. Rollback

Keep the last known-good immutable image digest and rendered configuration available.

Rollback should restore the previous application image/configuration without
rewriting task identities or manually marking durable claims complete.

Database/Dapr Workflow/broker schema and retention changes require their own
operator-managed rollback/DR plan.

## Evidence boundary

The earlier JetStream Kubernetes deployment acceptance is recorded in
[VALIDATION.md](VALIDATION.md). It is evidence for its exact recorded image and
environment, not a generic production certification.
