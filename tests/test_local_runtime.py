from pathlib import Path
import tomllib

import pytest

from a2a_mq.domain import PermanentError
from a2a_mq.local_runtime import (
    APP_ID,
    _bus_component,
    _dlq_component,
    _loopback_public_url,
    _plan,
    _resiliency_resource,
    _workflow_state_component,
)


def test_local_plan_is_single_node_and_bounded():
    plan = _plan("demo", "local-worker")
    stream = plan["stream"]
    assert stream["name"] == "A2A_MQ"
    assert stream["subjects"] == ["a2a.v1.demo.>"]
    assert stream["num_replicas"] == 1
    assert stream["max_bytes"] == 268435456
    assert stream["discard"] == "new"


def test_local_bus_uses_one_app_and_loopback_nats():
    raw = _bus_component("demo", "local-worker", "secret-token")
    assert f"  - {APP_ID}\n" in raw
    assert "nats://127.0.0.1:4222" in raw
    assert "a2a.v1.demo.command.agent.local-worker" in raw
    assert "durableName" in raw and "queueGroupName" in raw
    assert "a2a-demo-local-command-v1" in raw
    assert f"publishingScopes\n      value: {APP_ID}=" in raw
    assert f"subscriptionScopes\n      value: {APP_ID}=a2a.v1.demo.command.agent.local-worker" in raw
    assert "secret-token" in raw


def test_dlq_has_a_separate_durable_consumer_and_no_publish_permission():
    raw = _dlq_component("demo", "secret-token")
    assert "name: bus-dlq" in raw
    assert "a2a-demo-local-dlq-v1" in raw
    assert f"value: {APP_ID}=" in raw
    assert f"value: {APP_ID}=a2a.v1.demo.dlq" in raw


def test_local_pubsub_retries_are_bounded_before_dlq():
    raw = _resiliency_resource()
    assert "kind: Resiliency" in raw
    assert f"  - {APP_ID}\n" in raw
    assert "localPubsubRetry" in raw
    assert "duration: 500ms" in raw
    assert "maxRetries: 5" in raw
    assert "bus:" in raw and "inbound:" in raw


def test_optional_workflow_state_is_sqlite_not_redis_and_quotes_path(tmp_path):
    path = Path(tmp_path / "path with spaces" / "workflow.db")
    raw = _workflow_state_component(path)
    assert "type: state.sqlite" in raw
    assert "actorStateStore" in raw
    assert f'value: "{path}"' in raw
    assert "redis" not in raw.lower()


def test_builtin_launcher_url_matches_its_fixed_loopback_listener():
    assert _loopback_public_url("http://127.0.0.1:18000") == "http://127.0.0.1:18000"
    assert _loopback_public_url("http://localhost:18000/") == "http://localhost:18000"
    for invalid in (
        "https://127.0.0.1:18000",
        "http://127.0.0.1:9999",
        "http://[::1]:18000",
        "http://0.0.0.0:18000",
    ):
        with pytest.raises(PermanentError) as exc:
            _loopback_public_url(invalid)
        assert exc.value.code == "LOCAL_LOOPBACK_URL_REQUIRED"


def test_local_extra_excludes_distributed_and_workflow_runtime_dependencies():
    data = tomllib.loads((Path(__file__).resolve().parents[1] / "pyproject.toml").read_text())
    local = data["project"]["optional-dependencies"]["local"]
    lowered = " ".join(local).lower()
    assert "a2a-sdk" in lowered
    assert "dapr" not in lowered
    assert "psycopg" not in lowered
    assert "postgres" not in lowered
    assert "redis" not in lowered
    assert "docker" not in lowered
