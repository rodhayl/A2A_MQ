import importlib.util
from pathlib import Path

import pytest

from a2a_mq.domain import PermanentError

SCRIPT = Path(__file__).resolve().parents[1] / "deploy" / "production" / "generate.py"
spec = importlib.util.spec_from_file_location("production_deploy", SCRIPT)
assert spec is not None and spec.loader is not None
deploy = importlib.util.module_from_spec(spec)
spec.loader.exec_module(deploy)

IMAGE = "registry.example/a2amq@sha256:" + "a" * 64
PUBLIC_KEY = "-----BEGIN PUBLIC KEY-----\nfixture\n-----END PUBLIC KEY-----\n"


def manifests(broker="jetstream"):
    return deploy.generate(namespace="a2amq", tenant="demo", broker=broker, image=IMAGE,
                           public_url="https://agent.example", issuer="https://issuer.example",
                           jwt_kid="key-1", public_key=PUBLIC_KEY)["items"]


@pytest.mark.parametrize("broker", ["jetstream", "rabbitmq"])
def test_generation_contains_all_roles_scoped_components_and_workflow_state(broker):
    items = manifests(broker)
    deployments = {item["metadata"]["name"]: item for item in items if item["kind"] == "Deployment"}
    assert set(deployments) == {"a2amq-api", "a2amq-worker", "a2amq-dlq"}
    components = {item["metadata"]["name"]: item for item in items if item["kind"] == "Component"}
    assert {"bus-demo-api", "bus-demo-worker", "bus-demo-dlq", "workflow-state"} <= set(components)
    state = components["workflow-state"]
    assert state["spec"]["version"] == "v2" and state["scopes"] == ["a2amq-worker"]
    metadata = {entry["name"]: entry for entry in state["spec"]["metadata"]}
    assert metadata["actorStateStore"]["value"] == "true"
    assert "secretKeyRef" in metadata["connectionString"]
    assert not any(item["kind"] == "Secret" for item in items)


def test_worker_and_dlq_require_dapr_to_app_token_but_api_does_not():
    items = manifests()
    deployments = {item["metadata"]["name"]: item for item in items if item["kind"] == "Deployment"}
    for name in ("a2amq-worker", "a2amq-dlq"):
        annotations = deployments[name]["spec"]["template"]["metadata"]["annotations"]
        assert annotations["dapr.io/app-token-secret"] == "app-api-token"
        assert annotations["dapr.io/api-token-secret"] == "dapr-api-token"
    api = deployments["a2amq-api"]["spec"]["template"]["metadata"]["annotations"]
    assert "dapr.io/app-token-secret" not in api and api["dapr.io/api-token-secret"] == "dapr-api-token"


def test_image_must_be_immutable_digest():
    with pytest.raises(PermanentError, match="IMAGE_DIGEST_REQUIRED"):
        deploy.generate(namespace="a2amq", tenant="demo", broker="jetstream",
                        image="registry.example/a2amq:latest", public_url="https://agent.example",
                        issuer="https://issuer.example", jwt_kid="key-1", public_key=PUBLIC_KEY)


def test_postgres_dsn_uses_secret_env_not_unreadable_root_owned_volume():
    items = manifests()
    deployments = [item for item in items if item["kind"] == "Deployment"]
    for deployment in deployments:
        pod = deployment["spec"]["template"]["spec"]
        container = pod["containers"][0]
        env = {entry["name"]: entry for entry in container["env"]}
        assert "A2A_POSTGRES_DSN_FILE" not in env
        assert env["A2A_POSTGRES_DSN"]["valueFrom"]["secretKeyRef"] == {
            "name": "a2amq-postgres", "key": "dsn"
        }
        assert not any(volume.get("name") == "postgres" for volume in pod["volumes"])
        assert not any(mount.get("name") == "postgres" for mount in container["volumeMounts"])


def test_kubernetes_health_probes_target_public_health_contract():
    items = manifests()
    deployments = [item for item in items if item["kind"] == "Deployment"]
    for deployment in deployments:
        container = deployment["spec"]["template"]["spec"]["containers"][0]
        assert container["readinessProbe"]["httpGet"]["path"] == "/health/ready"
        assert container["livenessProbe"]["httpGet"]["path"] == "/health/live"


def test_dapr_sidecars_do_not_require_service_account_token_for_runtime_secret_api():
    items = manifests()
    deployments = [item for item in items if item["kind"] == "Deployment"]
    for deployment in deployments:
        template = deployment["spec"]["template"]
        assert template["spec"]["automountServiceAccountToken"] is False
        assert template["metadata"]["annotations"]["dapr.io/disable-builtin-k8s-secret-store"] == "true"
