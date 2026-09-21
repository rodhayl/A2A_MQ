from pathlib import Path

from a2a_mq.local_runtime import APP_ID, _bus_component, _plan, _workflow_state_component


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
    assert "secret-token" in raw


def test_optional_workflow_state_is_sqlite_not_redis(tmp_path):
    raw = _workflow_state_component(Path(tmp_path / "workflow.db"))
    assert "type: state.sqlite" in raw
    assert "actorStateStore" in raw
    assert "redis" not in raw.lower()
