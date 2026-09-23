import importlib.util
from pathlib import Path
import tomllib

import pytest

from a2a_mq.domain import PermanentError
from a2a_mq.local_runtime import APP_ID, DAPR_INTERNAL_GRPC_PORT, _workflow_state_component
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


def test_workflow_sidecar_internal_grpc_port_is_reserved():
    assert DAPR_INTERNAL_GRPC_PORT == 50002


def _workflow_gate_module():
    path = Path(__file__).resolve().parents[1] / "scripts/local_workflow_gate.py"
    spec = importlib.util.spec_from_file_location("local_workflow_gate", path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_workflow_evidence_paths_must_be_fresh_and_distinct(tmp_path):
    gate = _workflow_gate_module()
    json_path = tmp_path / "workflow.json"
    log_path = tmp_path / "workflow.log"
    gate._prepare_evidence_paths(json_path, log_path)

    existing = tmp_path / "existing.json"
    existing.write_text("{}")
    with pytest.raises(PermanentError, match="EVIDENCE_OUTPUT_EXISTS"):
        gate._prepare_evidence_paths(existing, log_path)

    target = tmp_path / "target.log"
    target.write_text("target")
    link = tmp_path / "link.log"
    link.symlink_to(target)
    with pytest.raises(PermanentError, match="EVIDENCE_OUTPUT_EXISTS"):
        gate._prepare_evidence_paths(json_path, link)

    with pytest.raises(PermanentError, match="EVIDENCE_PATH_CONFLICT"):
        gate._prepare_evidence_paths(tmp_path / "same", tmp_path / "same")
