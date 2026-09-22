from pathlib import Path
import tomllib

import pytest

from a2a_mq.domain import PermanentError
from a2a_mq.local_runtime import (
    APP_ID,
    CLIENT_TOKEN_LIFETIME_SECONDS,
    CLIENT_TOKEN_REFRESH_SECONDS,
    SUPERVISOR_INTERVAL_SECONDS,
    _bus_component,
    _dlq_component,
    _load_profile,
    _loopback_public_url,
    _nats_config,
    _plan,
    _private_replace,
    _read,
    _resiliency_resource,
    _workflow_state_component,
)


def test_local_plan_is_single_node_and_bounded():
    plan = _plan("demo", "local-worker")
    stream = plan["stream"]
    assert stream["name"] == "A2A_MQ"
    assert stream["subjects"] == ["a2a.v1.demo.>"]
    assert stream["num_replicas"] == 1
    assert stream["max_bytes"] == 67108864
    assert stream["max_msgs"] == 50000
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


def test_local_profile_mode_is_fail_closed_before_runtime_start(tmp_path):
    root = tmp_path / "profile"
    root.mkdir(mode=0o700)
    profile = {
        "mode": "local-workflow",
        "tenant": "demo",
        "source": "local-worker",
        "publicUrl": "http://127.0.0.1:18000",
        "issuer": "https://local-a2amq.invalid",
        "mcp": False,
        "appId": APP_ID,
        "appPort": 18000,
        "daprHttpPort": 3500,
        "daprGrpcPort": 50001,
    }
    (root / "profile.json").write_text(__import__("json").dumps(profile))
    with pytest.raises(PermanentError, match="LOCAL_PROFILE_MODE_MISMATCH"):
        _load_profile(root, "local-lite")
    assert _load_profile(root, "local-workflow")["mode"] == "local-workflow"


def test_local_profile_rejects_group_access_and_atomic_secret_refresh(tmp_path):
    root = tmp_path / "profile"
    root.mkdir(mode=0o770)
    (root / "profile.json").write_text("{}")
    with pytest.raises(PermanentError, match="UNSAFE_LOCAL_PROFILE"):
        _load_profile(root, "local-lite")

    secret = tmp_path / "token"
    _private_replace(secret, "first")
    _private_replace(secret, "second")
    assert secret.read_text() == "second"
    assert secret.stat().st_mode & 0o077 == 0


def test_local_native_caps_match_low_resource_profile(tmp_path):
    raw = _nats_config(tmp_path / "nats", "fixture-token")
    assert "max_mem_store: 16MB" in raw
    assert "max_file_store: 96MB" in raw
    assert "host: 127.0.0.1" in raw


def test_local_supervision_and_token_rotation_are_low_frequency_and_bounded():
    assert CLIENT_TOKEN_REFRESH_SECONDS < CLIENT_TOKEN_LIFETIME_SECONDS
    assert CLIENT_TOKEN_REFRESH_SECONDS <= CLIENT_TOKEN_LIFETIME_SECONDS // 2
    assert SUPERVISOR_INTERVAL_SECONDS >= 1.0


def test_local_rotation_constants_match_documented_budget():
    assert CLIENT_TOKEN_LIFETIME_SECONDS == 3600
    assert CLIENT_TOKEN_REFRESH_SECONDS == 1800
    assert SUPERVISOR_INTERVAL_SECONDS == 1.0


def test_local_profile_rejects_symlink_and_oversized_profile(tmp_path):
    root = tmp_path / "profile"
    root.mkdir(mode=0o700)
    target = tmp_path / "outside-profile.json"
    target.write_text("{}")
    (root / "profile.json").symlink_to(target)
    with pytest.raises(PermanentError, match="CONFIG_FILE_UNAVAILABLE"):
        _load_profile(root, "local-lite")

    (root / "profile.json").unlink()
    (root / "profile.json").write_text("x" * 16385)
    with pytest.raises(PermanentError, match="INVALID_CONFIG_FILE"):
        _load_profile(root, "local-lite")


def test_local_secret_reader_rejects_symlink_and_oversize(tmp_path):
    target = tmp_path / "outside-token"
    target.write_text("secret")
    target.chmod(0o600)
    link = tmp_path / "token-link"
    link.symlink_to(target)
    with pytest.raises(PermanentError, match="CONFIG_FILE_UNAVAILABLE"):
        _read(link)

    oversized = tmp_path / "oversized-token"
    oversized.write_text("x" * 513)
    oversized.chmod(0o600)
    with pytest.raises(PermanentError, match="INVALID_CONFIG_FILE"):
        _read(oversized)
