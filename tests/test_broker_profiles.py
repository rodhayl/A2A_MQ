import pytest
from a2a_mq.broker_profiles import (
    assert_portability_contract,
    jetstream_component,
    platform_profiles,
    production_profiles,
    rabbitmq_component,
)
from a2a_mq.domain import PermanentError


def values(profile):
    return {item["name"]: item for item in profile.component["spec"]["metadata"]}


def test_rabbitmq_profile_is_persistent_confirmed_tls_and_manual_ack():
    profile = rabbitmq_component(component="bus-workers", consumer="demo-workers",
                                 app_ids=("worker-a", "worker-b"), prefetch=16)
    assert_portability_contract(profile)
    metadata = values(profile)
    assert metadata["prefetchCount"]["value"] == "16"
    for name in ("host", "caCert", "clientCert", "clientKey"):
        assert "secretKeyRef" in metadata[name] and "value" not in metadata[name]
    assert profile.component["scopes"] == ["worker-a", "worker-b"]




def test_jetstream_profile_does_not_treat_secret_pem_as_client_cert_path():
    profile = jetstream_component(component="bus-workers", consumer="demo-workers",
                                  app_ids=("worker-a", "worker-b"), replicas=3)
    metadata = values(profile)
    assert "secretKeyRef" in metadata["token"]
    assert "tls_client_cert" not in metadata
    assert "tls_client_key" not in metadata

def test_jetstream_profile_has_shared_durable_queue_identity():
    profile = jetstream_component(component="bus-workers", consumer="demo-workers",
                                  app_ids=("worker-a", "worker-b"), replicas=3)
    assert_portability_contract(profile)
    metadata = values(profile)
    assert metadata["durableName"]["value"] == metadata["queueGroupName"]["value"] == "demo-workers"
    assert metadata["replicas"]["value"] == "3" and "secretKeyRef" in metadata["natsURL"]


@pytest.mark.parametrize("broker", ["rabbitmq", "jetstream"])
def test_topology_competes_workers_and_fans_out_observers(broker):
    profiles = production_profiles(broker=broker, tenant="demo", workers=("worker-a", "worker-b"),
                                   observers=("observer-a", "observer-b"))
    assert len(profiles) == 3 and profiles[0].app_ids == ("worker-a", "worker-b")
    assert profiles[0].consumer == "demo-workers"
    assert {p.consumer for p in profiles[1:]} == {"demo-observer-a", "demo-observer-b"}
    assert all(len(p.app_ids) == 1 for p in profiles[1:])
    for profile in profiles:
        assert_portability_contract(profile)


@pytest.mark.parametrize("broker", ["rabbitmq", "jetstream"])
def test_deployable_platform_uses_role_local_components_over_shared_topics(broker):
    profiles = platform_profiles(broker=broker, tenant="demo")
    assert set(profiles) == {"api", "worker", "dlq"}
    assert profiles["api"].component_name == "bus-demo-api"
    assert profiles["worker"].component_name == "bus-demo-worker"
    assert profiles["worker"].consumer == "demo-workers"
    assert profiles["dlq"].component_name == "bus-demo-dlq"
    assert profiles["api"].app_ids == ("a2amq-api",)
    assert profiles["worker"].app_ids == ("a2amq-worker",)
    assert profiles["dlq"].app_ids == ("a2amq-dlq",)
    for profile in profiles.values():
        assert_portability_contract(profile)


def test_unsupported_broker_fails_closed():
    with pytest.raises(PermanentError, match="UNSUPPORTED_BROKER"):
        production_profiles(broker="redis", tenant="demo")
    with pytest.raises(PermanentError, match="UNSUPPORTED_BROKER"):
        platform_profiles(broker="redis", tenant="demo")
