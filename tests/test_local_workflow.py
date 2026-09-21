from pathlib import Path
import tomllib

from a2a_mq.local_runtime import APP_ID, _workflow_state_component
from a2a_mq.local_workflow import LocalWorkflowSettings
from a2a_mq.local_workflow_runtime import _placement_command, _scheduler_command


def test_local_workflow_settings_reuse_lite_defaults_and_add_grpc_port():
    settings = LocalWorkflowSettings.from_env({
        "A2A_SQLITE_PATH": "/tmp/a2amq.db",
        "APP_API_TOKEN_FILE": "/tmp/app-token",
        "DAPR_API_TOKEN_FILE": "/tmp/dapr-token",
        "A2A_JWT_KEYS_FILE": "/tmp/keys.json",
        "A2A_JWT_ISSUER": "https://local-a2amq.invalid",
        "A2A_PUBLIC_URL": "http://127.0.0.1:18000",
        "DAPR_GRPC_PORT": "50001",
    })
    assert settings.dapr_grpc_port == 50001
    assert settings.concurrency == 2
    assert settings.database_capacity == 4
    assert settings.max_active == 128


def test_local_workflow_state_is_sqlite_and_scoped_to_same_app(tmp_path):
    path = Path(tmp_path / "workflow.db")
    raw = _workflow_state_component(path, app_id=APP_ID)
    assert "type: state.sqlite" in raw
    assert "actorStateStore" in raw
    assert f"  - {APP_ID}" in raw
    assert "redis" not in raw.lower()
    assert "postgres" not in raw.lower()


def test_native_workflow_control_plane_is_loopback_and_single_node(tmp_path):
    placement = _placement_command("/opt/dapr/placement")
    scheduler = _scheduler_command("/opt/dapr/scheduler", tmp_path)
    joined_placement = " ".join(placement)
    joined_scheduler = " ".join(scheduler)
    assert "--listen-address=127.0.0.1" in joined_placement
    assert "--port=50005" in joined_placement
    assert "--tls-enabled=false" in joined_placement
    assert "--listen-address=127.0.0.1" in joined_scheduler
    assert "--port=50006" in joined_scheduler
    assert "--override-broadcast-host-port=127.0.0.1:50006" in joined_scheduler
    assert "docker" not in (joined_placement + joined_scheduler).lower()
    assert "redis" not in (joined_placement + joined_scheduler).lower()


def test_local_workflow_extra_only_adds_official_dapr_workflow_sdks():
    data = tomllib.loads((Path(__file__).resolve().parents[1] / "pyproject.toml").read_text())
    extra = data["project"]["optional-dependencies"]["local-workflow"]
    lowered = " ".join(extra).lower()
    assert "dapr==1.18.3" in lowered
    assert "dapr-ext-workflow==1.18.3" in lowered
    assert "psycopg" not in lowered
    assert "redis" not in lowered
    assert "docker" not in lowered
