import base64
import importlib.util
import json
import os
from pathlib import Path
import subprocess
from unittest.mock import patch

import pytest

ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture(autouse=True)
def clean_model_credentials(monkeypatch):
    gate = _gate_module()
    for key in list(os.environ.keys()):
        if gate.MODEL_CREDENTIAL.match(key):
            monkeypatch.delenv(key, raising=False)


def _gate_module():
    path = ROOT / "scripts" / "production_deployment_gate.py"
    spec = importlib.util.spec_from_file_location("production_deployment_gate", path)
    assert spec and spec.loader
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


VALID_PUBLIC_KEY = (
    "-----BEGIN PUBLIC KEY-----\n"
    "MFkwEwYHKoZIzj0CAQYIKoZIzj0DAQcDQgAE7p1bT6Z1gZ8J3Nl5X4L7P0kQ1vGk\n"
    "u4J7Z1wG7p1bT6Z1gZ8J3Nl5X4L7P0kQ1vGku4J7Z1wG7p1bT6Z1gZ8J3Nl5X4L7\n"
    "PQ==\n"
    "-----END PUBLIC KEY-----\n"
)


def _make_key_file(tmp_path: Path) -> Path:
    key_file = tmp_path / "jwt_pubkey.pem"
    key_file.write_text(VALID_PUBLIC_KEY, encoding="utf-8")
    return key_file


def _make_token_file(tmp_path: Path, name: str, value: str = "test-token") -> Path:
    token_file = tmp_path / name
    token_file.write_text(value, encoding="utf-8")
    token_file.chmod(0o600)
    return token_file


def _valid_args(tmp_path: Path, out_dir: Path, include_tokens: bool = True) -> list[str]:
    key_file = _make_key_file(tmp_path)
    args = [
        "--output", str(out_dir),
        "--image", "registry.example.org/a2amq@sha256:" + "a" * 64,
        "--public-url", "https://a2amq.example.com",
        "--issuer", "https://auth.example.com",
        "--jwt-kid", "prod-key-1",
        "--jwt-public-key-file", str(key_file),
        "--namespace", "a2amq-test-ns",
        "--tenant", "test-tenant",
    ]
    if include_tokens:
        args.extend([
            "--owner-token-file", str(_make_token_file(tmp_path, "owner_token.txt", "owner-token")),
            "--foreign-owner-token-file", str(_make_token_file(tmp_path, "foreign_token.txt", "foreign-token")),
            "--cross-tenant-token-file", str(_make_token_file(tmp_path, "cross_tenant.txt", "cross-tenant-token")),
            "--mcp-token-file", str(_make_token_file(tmp_path, "mcp_token.txt", "mcp-token")),
            "--operator-token-file", str(_make_token_file(tmp_path, "operator_token.txt", "operator-token")),
        ])
    return args


def test_manifest_rendering_and_tenant_propagation():
    gate = _gate_module()
    manifests = gate.render_production_manifests(
        namespace="test-ns",
        tenant="custom-tenant-xyz",
        broker="jetstream",
        image="registry.example/a2amq@sha256:" + "b" * 64,
        public_url="https://agent.example",
        issuer="https://issuer.example",
        jwt_kid="key-1",
        public_key=VALID_PUBLIC_KEY,
    )
    assert manifests["kind"] == "List"
    items = manifests["items"]
    deployments = {item["metadata"]["name"]: item for item in items if item["kind"] == "Deployment"}
    assert set(deployments.keys()) == {"a2amq-api", "a2amq-worker", "a2amq-dlq"}

    # Verify tenant propagation to environment
    for dep_name, dep in deployments.items():
        assert dep["metadata"]["labels"]["app.kubernetes.io/managed-by"] == "a2amq-production-gate"
        container = dep["spec"]["template"]["spec"]["containers"][0]
        env_vars = {e["name"]: e.get("value") for e in container["env"]}
        assert env_vars.get("A2A_TENANT") == "custom-tenant-xyz"


def test_kubectl_abstraction():
    gate = _gate_module()
    recorded = []

    def fake_runner(cmd, input_bytes=None, timeout=60.0):
        recorded.append(cmd)
        return gate.CommandResult(0, '{"test": "val"}', "")

    k = gate.Kubectl(kubeconfig="/etc/kubeconfig", context="my-ctx", namespace="my-ns", runner=fake_runner)
    res = k.run("get", "pods", "password=secret123")
    assert res.returncode == 0
    assert recorded[0] == ["kubectl", "--kubeconfig", "/etc/kubeconfig", "--context", "my-ctx", "-n", "my-ns", "get", "pods", "password=secret123"]
    # Verify sanitization in history
    assert k.history[0]["cmd"][-1] == "password=REDACTED"

    data = k.get_json("get", "pods")
    assert data == {"test": "val"}


def test_kubectl_timeout_and_error():
    gate = _gate_module()
    k = gate.Kubectl()

    with patch("subprocess.run", side_effect=subprocess.TimeoutExpired(["kubectl"], 5.0)):
        res = k._default_run(["kubectl", "get"])
        assert res.returncode == 124
        assert "timed out" in res.stderr

    with patch("subprocess.run", side_effect=OSError("binary missing")):
        res = k._default_run(["kubectl", "get"])
        assert res.returncode == 127
        assert "binary missing" in res.stderr


def test_prerequisites_missing_kubectl():
    gate = _gate_module()
    with patch("shutil.which", return_value=None):
        missing = gate.check_prerequisites()
        assert missing == ["kubectl"]


def test_prerequisites_kubectl_version_fails():
    gate = _gate_module()
    with patch("shutil.which", return_value="/usr/bin/kubectl"):
        with patch("subprocess.run", return_value=subprocess.CompletedProcess(["kubectl"], returncode=1, stdout=b"", stderr=b"error")):
            missing = gate.check_prerequisites()
            assert missing == ["kubectl"]


def test_prerequisites_cluster_unreachable():
    gate = _gate_module()
    with patch("shutil.which", return_value="/usr/bin/kubectl"):
        def fake_run(cmd, **kwargs):
            if "version" in cmd:
                return subprocess.CompletedProcess(cmd, returncode=0, stdout=b"{}", stderr=b"")
            return subprocess.CompletedProcess(cmd, returncode=1, stdout=b"", stderr=b"cluster down")

        with patch("subprocess.run", side_effect=fake_run):
            missing = gate.check_prerequisites()
            assert missing == ["kubernetes-cluster"]


def test_prerequisites_all_present():
    gate = _gate_module()
    with patch("shutil.which", return_value="/usr/bin/kubectl"):
        with patch("subprocess.run", return_value=subprocess.CompletedProcess(["kubectl"], returncode=0, stdout=b"{}", stderr=b"")):
            missing = gate.check_prerequisites()
            assert missing == []


def test_cli_rejects_in_repo_output(tmp_path):
    gate = _gate_module()
    repo_output = gate.ROOT / "tests" / "scratch_output"
    argv = _valid_args(tmp_path, repo_output)
    rc = gate.main(argv)
    assert rc == 1


def test_cli_rejects_existing_output(tmp_path):
    gate = _gate_module()
    existing = tmp_path / "already_exists"
    existing.mkdir()
    argv = _valid_args(tmp_path, existing)
    rc = gate.main(argv)
    assert rc == 1


def test_cli_rejects_non_digest_image(tmp_path):
    gate = _gate_module()
    out = tmp_path / "out_tag"
    argv = _valid_args(tmp_path, out)
    argv[argv.index("--image") + 1] = "registry.example.org/a2amq:latest"
    rc = gate.main(argv)
    assert rc == 1
    verdict = json.loads((out / "production-deployment-verdict.json").read_text())
    assert verdict["status"] == "FAILED"
    assert "Image must be an immutable reference" in verdict["reason"]


def test_cli_rejects_unsafe_namespace(tmp_path):
    gate = _gate_module()
    out = tmp_path / "out_ns"
    argv = _valid_args(tmp_path, out)
    argv[argv.index("--namespace") + 1] = "kube-system"
    rc = gate.main(argv)
    assert rc == 1
    verdict = json.loads((out / "production-deployment-verdict.json").read_text())
    assert verdict["status"] == "FAILED"
    assert "Unsafe or forbidden" in verdict["reason"]


def test_cli_rejects_invalid_jwt_key_file(tmp_path):
    gate = _gate_module()
    out = tmp_path / "out_key"
    argv = _valid_args(tmp_path, out)
    bad_key = tmp_path / "bad.pem"
    bad_key.write_text("-----BEGIN PRIVATE KEY-----\nsecret\n-----END PRIVATE KEY-----\n")
    argv[argv.index("--jwt-public-key-file") + 1] = str(bad_key)
    rc = gate.main(argv)
    assert rc == 1
    verdict = json.loads((out / "production-deployment-verdict.json").read_text())
    assert verdict["status"] == "FAILED"
    assert "Invalid JWT public key" in verdict["reason"]


def test_cli_rejects_present_credentials(tmp_path, monkeypatch):
    gate = _gate_module()
    monkeypatch.setenv("OPENAI_API_KEY", "sk-proj-secret")
    out = tmp_path / "out_cred"
    argv = _valid_args(tmp_path, out)
    rc = gate.main(argv)
    assert rc == 1
    verdict = json.loads((out / "production-deployment-verdict.json").read_text())
    assert verdict["status"] == "FAILED"
    assert verdict["credentialGuard"] == "FAILED"


def test_cli_missing_kubectl_clean_exit(tmp_path):
    gate = _gate_module()
    out = tmp_path / "gate_evidence"
    argv = _valid_args(tmp_path, out)
    with patch.object(gate, "check_prerequisites", return_value=["kubectl"]):
        rc = gate.main(argv)
        assert rc == 2
        verdict_file = out / "production-deployment-verdict.json"
        assert verdict_file.is_file()
        verdict = json.loads(verdict_file.read_text(encoding="utf-8"))
        assert verdict["status"] == "INCOMPLETE_PREREQUISITE"
        assert verdict["productionCertified"] is False
        assert "kubectl" in verdict["missingPrerequisites"]
        assert (out / "sha256.json").is_file()


def _make_simulated_runner(gate, *, cluster_ok=True, dapr_ok=True, dapr_ready=True,
                           mtls_ok=True, sched_ok=True, secrets_ok=True, dry_run_ok=True,
                           rollout_ok=True, pod_sec_ok=True, sidecar_ok=True,
                           pdb_ok=True, pdb_selector_ok=True, state_ok=True, state_v2_ok=True,
                           broker_target_owned=True, broker_disruption_label_ok=True,
                           broker_target_namespace="a2amq-test-ns",
                           broker_scale_down_to_zero=True, broker_restored_pods_ok=True,
                           image_digest_ok=True,
                           worker_has_uid_before=True, worker_has_uid_after=True,
                           worker_replacement_ready=True, worker_uid_changes=True,
                           api_has_uids_before=True, api_has_uids_after=True,
                           api_uid_changes=True, namespace_exists=True,
                           disposable_resources_map=None):
    """Factory creating a simulated Kubectl runner returning configurable responses."""
    worker_restart_count = [0]
    api_restart_count = [0]
    broker_replicas = [1]

    def fake_run(cmd, input_bytes=None, timeout=60.0):
        cmd_str = " ".join(cmd)

        if "version" in cmd:
            if not cluster_ok:
                return gate.CommandResult(1, "", "Cannot connect")
            return gate.CommandResult(0, json.dumps({"serverVersion": {"gitVersion": "v1.30.2"}}), "")

        if "--dry-run=server" in cmd:
            if not dry_run_ok:
                return gate.CommandResult(1, "", "Dry run schema rejected")
            return gate.CommandResult(0, "configured", "")

        if "get pods -n dapr-system" in cmd_str:
            if not dapr_ok:
                return gate.CommandResult(0, json.dumps({"items": []}), "")
            conditions = [{"type": "Ready", "status": "True"}] if dapr_ready else [{"type": "Ready", "status": "False"}]
            return gate.CommandResult(0, json.dumps({"items": [
                {
                    "metadata": {"name": "dapr-operator-xyz"},
                    "spec": {"containers": [{"image": "daprio/dapr:1.18.3"}]},
                    "status": {
                        "phase": "Running",
                        "containerStatuses": [{"ready": dapr_ready}],
                        "conditions": conditions,
                    },
                }
            ]}), "")

        if "dapr-config" in cmd_str:
            return gate.CommandResult(0, json.dumps({"spec": {"mtls": {"enabled": mtls_ok}}}), "")

        if "app=dapr-placement" in cmd_str:
            if not sched_ok:
                return gate.CommandResult(0, json.dumps({"items": []}), "")
            return gate.CommandResult(0, json.dumps({"items": [
                {
                    "metadata": {"name": "dapr-placement-0"},
                    "status": {
                        "phase": "Running",
                        "conditions": [{"type": "Ready", "status": "True"}],
                    },
                }
            ]}), "")

        if "app=dapr-scheduler" in cmd_str:
            if not sched_ok:
                return gate.CommandResult(0, json.dumps({"items": []}), "")
            return gate.CommandResult(0, json.dumps({"items": [
                {
                    "metadata": {"name": "dapr-scheduler-0"},
                    "status": {
                        "phase": "Running",
                        "conditions": [{"type": "Ready", "status": "True"}],
                    },
                }
            ]}), "")

        if "get namespace" in cmd_str:
            if not namespace_exists:
                return gate.CommandResult(1, "", "namespaces 'a2amq-test-ns' not found")
            return gate.CommandResult(0, json.dumps({"metadata": {"name": "a2amq-test-ns"}}), "")

        if "get secrets" in cmd_str:
            names = ["a2amq-postgres", "a2amq-workflow-state", "dapr-api-token", "app-api-token"] if secrets_ok else ["a2amq-postgres"]
            return gate.CommandResult(0, json.dumps({"items": [{"metadata": {"name": n}} for n in names]}), "")

        if "delete pod" in cmd_str and "a2amq-worker" in cmd_str:
            worker_restart_count[0] += 1
            return gate.CommandResult(0, "pod deleted", "")

        if "rollout status" in cmd_str:
            if not rollout_ok:
                return gate.CommandResult(1, "", "Deployment rollout timed out")
            return gate.CommandResult(0, "successfully rolled out", "")

        if "rollout restart deployment/a2amq-api" in cmd_str:
            api_restart_count[0] += 1
            return gate.CommandResult(0, "restarted", "")

        if "scale" in cmd_str:
            if "--replicas=0" in cmd_str:
                broker_replicas[0] = 0 if broker_scale_down_to_zero else 1
            elif "--replicas=1" in cmd_str:
                broker_replicas[0] = 1
            return gate.CommandResult(0, "scaled", "")

        if "get statefulset/nats" in cmd_str or "get deployment/nats" in cmd_str:
            labels = {}
            if broker_target_owned:
                labels["app.kubernetes.io/managed-by"] = "a2amq-production-gate"
            if broker_disruption_label_ok:
                labels[gate.ALLOW_DISRUPTION_LABEL] = gate.ALLOW_DISRUPTION_VALUE
            reps = broker_replicas[0]
            return gate.CommandResult(0, json.dumps({
                "metadata": {"name": "nats", "namespace": broker_target_namespace, "labels": labels},
                "spec": {"replicas": 1},
                "status": {"replicas": reps, "readyReplicas": reps},
            }), "")

        if ("app.kubernetes.io/name=nats" in cmd_str or "name=nats" in cmd_str) and "get pods" in cmd_str:
            if broker_replicas[0] > 0 and broker_restored_pods_ok:
                return gate.CommandResult(0, json.dumps({"items": [{
                    "metadata": {"name": "nats-0", "uid": "uid-nats-0", "labels": {"app.kubernetes.io/name": "nats"}},
                    "status": {"phase": "Running", "conditions": [{"type": "Ready", "status": "True"}]},
                }]}), "")
            return gate.CommandResult(0, json.dumps({"items": []}), "")

        if "app=a2amq-worker" in cmd_str and "get pods" in cmd_str:
            if worker_restart_count[0] > 0:
                pname = "a2amq-worker-pod-2"
                puid = "uid-worker-2" if (worker_uid_changes and worker_has_uid_after) else ("uid-worker-1" if worker_has_uid_after else "")
                ready_status = "True" if worker_replacement_ready else "False"
            else:
                pname = "a2amq-worker-pod-1"
                puid = "uid-worker-1" if worker_has_uid_before else ""
                ready_status = "True"
            return gate.CommandResult(0, json.dumps({"items": [{
                "metadata": {"name": pname, "uid": puid, "labels": {"app": "a2amq-worker"}},
                "status": {"phase": "Running", "conditions": [{"type": "Ready", "status": ready_status}]},
            }]}), "")

        if "app=a2amq-api" in cmd_str and "get pods" in cmd_str:
            if api_restart_count[0] > 0:
                if not api_has_uids_after:
                    return gate.CommandResult(0, json.dumps({"items": []}), "")
                pname = "a2amq-api-pod-2"
                puid = "uid-api-2" if api_uid_changes else "uid-api-1"
            else:
                if not api_has_uids_before:
                    return gate.CommandResult(0, json.dumps({"items": []}), "")
                pname = "a2amq-api-pod-1"
                puid = "uid-api-1"
            return gate.CommandResult(0, json.dumps({"items": [{
                "metadata": {"name": pname, "uid": puid, "labels": {"app": "a2amq-api"}},
                "status": {"phase": "Running", "conditions": [{"type": "Ready", "status": "True"}]},
            }]}), "")

        if "app.kubernetes.io/name=a2a-mq" in cmd_str and "get pods" in cmd_str:
            items = []
            for dep in ("a2amq-api", "a2amq-worker", "a2amq-dlq"):
                containers = []
                c_statuses = []
                img_sha = ("a" * 64) if image_digest_ok else ("e" * 64)
                if pod_sec_ok:
                    containers.append({
                        "name": "app",
                        "securityContext": {
                            "runAsNonRoot": True,
                            "runAsUser": 10001,
                            "allowPrivilegeEscalation": False,
                            "readOnlyRootFilesystem": True,
                            "capabilities": {"drop": ["ALL"]},
                        },
                    })
                    c_statuses.append({
                        "name": "app",
                        "ready": True,
                        "imageID": "registry.example.org/a2amq@sha256:" + img_sha,
                    })
                else:
                    containers.append({
                        "name": "app",
                        "securityContext": {
                            "runAsNonRoot": False,
                            "runAsUser": 0,
                            "allowPrivilegeEscalation": True,
                            "readOnlyRootFilesystem": False,
                        },
                    })
                    c_statuses.append({"name": "app", "ready": True})

                if sidecar_ok:
                    containers.append({"name": "daprd"})
                    c_statuses.append({"name": "daprd", "ready": True, "state": {"running": {}}})

                uid_val = f"uid-{dep}-1"
                if dep == "a2amq-worker" and worker_restart_count[0] > 0:
                    uid_val = "uid-worker-2" if worker_uid_changes else "uid-worker-1"
                elif dep == "a2amq-api" and api_restart_count[0] > 0:
                    uid_val = "uid-api-2" if api_uid_changes else "uid-api-1"

                items.append({
                    "metadata": {"name": f"{dep}-xyz", "uid": uid_val, "labels": {"app": dep}},
                    "spec": {
                        "automountServiceAccountToken": False if pod_sec_ok else True,
                        "securityContext": {"seccompProfile": {"type": "RuntimeDefault" if pod_sec_ok else "Unconfined"}},
                        "containers": containers,
                    },
                    "status": {
                        "phase": "Running",
                        "conditions": [{"type": "Ready", "status": "True"}],
                        "containerStatuses": c_statuses,
                    },
                })
            return gate.CommandResult(0, json.dumps({"items": items}), "")

        if "get pdb" in cmd_str:
            if not pdb_ok:
                return gate.CommandResult(0, json.dumps({"items": []}), "")
            items = []
            for n in ["a2amq-api", "a2amq-worker", "a2amq-dlq"]:
                sel_app = n if pdb_selector_ok else "wrong-app"
                items.append({
                    "metadata": {"name": n},
                    "spec": {"minAvailable": 1, "selector": {"matchLabels": {"app": sel_app}}},
                    "status": {"currentHealthy": 2, "desiredHealthy": 1, "disruptionsAllowed": 1},
                })
            return gate.CommandResult(0, json.dumps({"items": items}), "")

        if "get components" in cmd_str:
            if not state_ok:
                return gate.CommandResult(0, json.dumps({"items": []}), "")
            meta = [
                {"name": "actorStateStore", "value": "true"},
                {"name": "connectionString", "secretKeyRef": {"name": "a2amq-workflow-state", "key": "connection-string"}},
            ]
            if not state_v2_ok:
                meta = [{"name": "actorStateStore", "value": "true"}, {"name": "connectionString", "value": "plaintext-password"}]
            return gate.CommandResult(0, json.dumps({"items": [
                {
                    "metadata": {"name": "workflow-state"},
                    "spec": {
                        "type": "state.postgresql",
                        "version": "v2" if state_v2_ok else "v1",
                        "metadata": meta,
                    },
                    "scopes": ["a2amq-worker"],
                }
            ]}), "")

        if "all,pdb,configmap,components.dapr.io" in cmd_str:
            if disposable_resources_map is not None:
                for arg in cmd:
                    if arg.startswith(f"{gate.GATE_RUN_ID_LABEL}="):
                        rid = arg.split("=", 1)[1]
                        if "delete" in cmd:
                            disposable_resources_map.pop(rid, None)
                            return gate.CommandResult(0, "deleted", "")
                        elif "get" in cmd:
                            items = disposable_resources_map.get(rid, [])
                            return gate.CommandResult(0, json.dumps({"items": items}), "")
            if "delete" in cmd:
                return gate.CommandResult(0, "deleted", "")
            if "get" in cmd:
                return gate.CommandResult(0, json.dumps({"items": []}), "")

        return gate.CommandResult(0, "success", "")

    return fake_run


def _make_simulated_http_runner(gate, *, task_ok=True, auth_ok=True, mcp_ok=True,
                                wf_ok=True, dlq_ok=True, dlq_empty=False,
                                dlq_unrelated_only=False, dlq_replay_downstream_ok=True,
                                rolling_ok=True, broker_false_complete=False):
    """Factory creating a simulated HTTP client runner."""
    broker_down = [False]
    wf_query_count = [0]
    outage_query_count = [0]
    roll_query_count = [0]
    dlq_replayed_map = {}

    def fake_http(method: str, path: str, *, headers=None, json_data=None, timeout=30.0):
        headers = headers or {}
        auth = headers.get("Authorization") or headers.get("authorization", "")
        method_call = (json_data or {}).get("method")
        params = (json_data or {}).get("params", {})
        req_id = (json_data or {}).get("id", 1)

        # 0. Discovery Agent Card
        if path == "/.well-known/agent-card.json":
            return gate.HttpResponse(200, data={
                "name": "A2A-MQ test",
                "version": "0.1.0",
                "supportedInterfaces": [
                    {"url": "https://a2amq.example.com/rpc", "protocolBinding": "JSONRPC", "protocolVersion": "1.0"},
                ],
                "capabilities": {"streaming": False},
                "skills": [{"id": "text", "name": "text"}],
                "defaultInputModes": ["text/plain"],
                "defaultOutputModes": ["text/plain"],
            })

        # 1. MCP endpoint
        if path == "/mcp":
            if "invalid" in auth or not auth:
                return gate.HttpResponse(401, text="Unauthorized")
            if not mcp_ok:
                return gate.HttpResponse(500, text="MCP internal error")
            if method_call == "server/discover":
                return gate.HttpResponse(404, text="Not Found")
            if method_call == "initialize":
                return gate.HttpResponse(200, data={
                    "jsonrpc": "2.0", "id": req_id,
                    "result": {
                        "protocolVersion": "2025-11-25",
                        "capabilities": {"tools": {}},
                        "serverInfo": {"name": "A2A-MQ", "version": "0.1.0"},
                    }
                })
            if method_call == "notifications/initialized":
                return gate.HttpResponse(200, data={})
            if method_call == "tools/list":
                return gate.HttpResponse(200, data={
                    "jsonrpc": "2.0", "id": req_id,
                    "result": {
                        "tools": [
                            {"name": "get_task", "description": "Get task", "inputSchema": {"type": "object"}},
                            {"name": "list_tasks", "description": "List tasks", "inputSchema": {"type": "object"}},
                            {"name": "submit_task", "description": "Submit task", "inputSchema": {"type": "object"}},
                            {"name": "submit_task_lite", "description": "Submit task lite", "inputSchema": {"type": "object"}},
                            {"name": "continue_task", "description": "Continue task", "inputSchema": {"type": "object"}},
                            {"name": "approve_task", "description": "Approve task", "inputSchema": {"type": "object"}},
                        ]
                    }
                })
            if method_call == "tools/call":
                t_id = params.get("arguments", {}).get("task_id", params.get("arguments", {}).get("id", "task-mcp-queried"))
                return gate.HttpResponse(200, data={
                    "jsonrpc": "2.0", "id": req_id,
                    "result": {
                        "content": [{"type": "text", "text": json.dumps({"id": t_id, "status": {"state": "TASK_STATE_COMPLETED"}})}],
                        "isError": False,
                    }
                })
            return gate.HttpResponse(200, data={"jsonrpc": "2.0", "id": req_id, "result": "ok"})

        # 2. DLQ ops endpoint
        if path.startswith("/ops/dlq"):
            if "operator-token" not in auth:
                return gate.HttpResponse(403, text="Forbidden")
            if not dlq_ok:
                return gate.HttpResponse(500, text="DLQ error")
            req_run_id = headers.get("X-Gate-Run-Id") or "gate-sim-run"
            entry_id = f"dlq-{req_run_id}-1" if not dlq_unrelated_only else "dlq-unrelated-entry-1"
            run_id_val = req_run_id if not dlq_unrelated_only else "unrelated-run-999"

            if method == "GET":
                if dlq_empty:
                    return gate.HttpResponse(200, data={"items": []})
                parts = path.split("?")[0].rstrip("/").split("/")
                if len(parts) == 4 and parts[2] == "dlq":
                    requested_id = parts[3]
                    replayed_at = dlq_replayed_map.get(requested_id)
                    b64_payload = base64.b64encode(json.dumps({"runId": run_id_val}).encode()).decode()
                    return gate.HttpResponse(200, data={"entryId": requested_id, "base64": b64_payload, "replayedAt": replayed_at})
                replayed_at = dlq_replayed_map.get(entry_id)
                items = [{
                    "id": entry_id,
                    "entryId": entry_id,
                    "runId": run_id_val,
                    "reason": "POISON_DELIVERY",
                    "originConsumer": run_id_val,
                    "replayedAt": replayed_at,
                }]
                return gate.HttpResponse(200, data={"items": items})

            if method == "POST" and "replay" in path:
                parts = path.split("?")[0].rstrip("/").split("/")
                replayed_id = parts[3]
                if dlq_replay_downstream_ok:
                    dlq_replayed_map[replayed_id] = 1727198400.0
                return gate.HttpResponse(202, data={"replayed": True})

        # 3. Tasks JSON-RPC endpoint (/ and /rpc)
        if path in ("/", "/rpc"):
            # Foreign owner rejection test
            if "foreign-token" in auth or "cross-tenant" in auth:
                if not auth_ok:
                    return gate.HttpResponse(200, data={"jsonrpc": "2.0", "id": req_id, "result": {"id": params.get("id")}})
                return gate.HttpResponse(403, text="Forbidden")

            # Task submission
            if method_call in ("tasks/send", "SendMessage"):
                if not task_ok:
                    return gate.HttpResponse(500, text="Task admission failed")
                mid = params.get("message", {}).get("messageId", "sim-task-1")
                if "outage-" in mid:
                    broker_down[0] = True
                task_data = {"id": f"task-{mid}", "status": {"state": "TASK_STATE_SUBMITTED"}}
                return gate.HttpResponse(200, data={"jsonrpc": "2.0", "id": req_id, "result": {"task": task_data}})

            # Task query
            if method_call in ("tasks/get", "GetTask"):
                task_id = params.get("id", "")
                if "roll-" in task_id:
                    roll_query_count[0] += 1
                    if roll_query_count[0] == 1:
                        return gate.HttpResponse(200, data={"jsonrpc": "2.0", "id": req_id, "result": {"id": task_id, "status": {"state": "TASK_STATE_WORKING"}}})
                    if not rolling_ok:
                        return gate.HttpResponse(200, data={"jsonrpc": "2.0", "id": req_id, "result": {"id": task_id, "status": {"state": "TASK_STATE_FAILED"}}})
                    return gate.HttpResponse(200, data={"jsonrpc": "2.0", "id": req_id, "result": {"id": task_id, "status": {"state": "TASK_STATE_COMPLETED"}}})
                if "wf-" in task_id:
                    if not wf_ok:
                        return gate.HttpResponse(500, text="Workflow lost")
                    wf_query_count[0] += 1
                    if wf_query_count[0] == 1:
                        return gate.HttpResponse(200, data={"jsonrpc": "2.0", "id": req_id, "result": {"id": task_id, "status": {"state": "TASK_STATE_WORKING"}}})
                    return gate.HttpResponse(200, data={"jsonrpc": "2.0", "id": req_id, "result": {"id": task_id, "status": {"state": "TASK_STATE_COMPLETED"}}})
                if "outage-" in task_id:
                    outage_query_count[0] += 1
                    if outage_query_count[0] == 1:
                        return gate.HttpResponse(200, data={"jsonrpc": "2.0", "id": req_id, "result": {"id": task_id, "status": {"state": "TASK_STATE_WORKING"}}})
                    elif outage_query_count[0] == 2:
                        state = "TASK_STATE_COMPLETED" if broker_false_complete else "TASK_STATE_WORKING"
                        return gate.HttpResponse(200, data={"jsonrpc": "2.0", "id": req_id, "result": {"id": task_id, "status": {"state": state}}})
                    else:
                        return gate.HttpResponse(200, data={"jsonrpc": "2.0", "id": req_id, "result": {"id": task_id, "status": {"state": "TASK_STATE_COMPLETED"}}})
                return gate.HttpResponse(200, data={"jsonrpc": "2.0", "id": req_id, "result": {"id": task_id, "status": {"state": "TASK_STATE_COMPLETED"}}})

            # Task cancel
            if method_call in ("tasks/cancel", "CancelTask"):
                task_id = params.get("id", "")
                return gate.HttpResponse(200, data={"jsonrpc": "2.0", "id": req_id, "result": {"id": task_id, "status": {"state": "TASK_STATE_CANCELED"}}})

        return gate.HttpResponse(200, data={"status": "ok"})

    return fake_http


def test_simulation_mode_never_emits_production_pass(tmp_path):
    """Critical safety invariant: simulation can never emit PRODUCTION_DEPLOYMENT_PASS."""
    gate = _gate_module()
    out = tmp_path / "out_sim"
    argv = _valid_args(tmp_path, out) + [
        "--allow-broker-disruption",
        "--disrupt-broker-target", "statefulset/nats",
    ]
    fake_k = _make_simulated_runner(gate)
    fake_http = _make_simulated_http_runner(gate)

    rc = gate.main(argv, runner_override=fake_k, http_runner_override=fake_http)
    assert rc == 0
    verdict = json.loads((out / "production-deployment-verdict.json").read_text())

    # Invariant checks:
    assert verdict["executionMode"] == "simulated"
    assert verdict["status"] == "SIMULATED_VALIDATION_PASS"
    assert verdict["status"] != "PRODUCTION_DEPLOYMENT_PASS"
    assert verdict["productionCertified"] is False
    assert (out / "command-history.json").is_file()
    assert (out / "sha256.json").is_file()


def test_missing_required_check_forces_incomplete_or_failed(tmp_path):
    gate = _gate_module()
    out = tmp_path / "out_missing"
    k = gate.Kubectl()
    http = gate.HttpClient("https://a2amq.example.com")
    runner = gate.ProductionDeploymentRunner(
        k=k,
        http=http,
        output=out,
        namespace="test-ns",
        tenant="prod-tenant",
        broker="jetstream",
        image="registry.example/a2amq@sha256:" + "a" * 64,
        public_url="https://a2amq.example.com",
        issuer="https://auth.example.com",
        jwt_kid="key-1",
        public_key=VALID_PUBLIC_KEY,
        execution_mode="live",
    )
    # Only record 1 check
    runner.record("manifest-synthesis-and-validation", "PASSED")
    verdict = runner._build_verdict("PRODUCTION_DEPLOYMENT_PASS", "All passed")
    assert verdict["status"] != "PRODUCTION_DEPLOYMENT_PASS"
    assert verdict["status"] in {"FAILED", "INCOMPLETE_PREREQUISITE"}
    assert "Required checks missing from verdict" in verdict["reason"]


def test_task_lifecycle_failure(tmp_path):
    gate = _gate_module()
    out = tmp_path / "out_task_fail"
    argv = _valid_args(tmp_path, out)
    fake_k = _make_simulated_runner(gate)
    fake_http = _make_simulated_http_runner(gate, task_ok=False)

    rc = gate.main(argv, runner_override=fake_k, http_runner_override=fake_http)
    assert rc == 1
    verdict = json.loads((out / "production-deployment-verdict.json").read_text())
    assert verdict["status"] == "FAILED"
    assert "Task lifecycle failed" in verdict["reason"] or "Task submission returned" in verdict["reason"]


def test_owner_authorization_foreign_owner_not_rejected(tmp_path):
    gate = _gate_module()
    out = tmp_path / "out_auth_fail"
    argv = _valid_args(tmp_path, out)
    fake_k = _make_simulated_runner(gate)
    fake_http = _make_simulated_http_runner(gate, auth_ok=False)

    rc = gate.main(argv, runner_override=fake_k, http_runner_override=fake_http)
    assert rc == 1
    verdict = json.loads((out / "production-deployment-verdict.json").read_text())
    assert verdict["status"] == "FAILED"
    assert "Foreign owner GetTask was not canonically denied" in verdict["reason"]


def test_owner_authorization_5xx_cannot_satisfy_denial(tmp_path):
    gate = _gate_module()
    out = tmp_path / "out_auth_5xx"
    argv = _valid_args(tmp_path, out)
    fake_k = _make_simulated_runner(gate)

    base_http = _make_simulated_http_runner(gate)

    def http_with_5xx(method, path, **kwargs):
        headers = kwargs.get("headers", {})
        auth = headers.get("Authorization", "")
        if "foreign-token" in auth:
            # Server crash / unhandled exception returning 500
            return gate.HttpResponse(500, text="Internal Server Error Crash")
        return base_http(method, path, **kwargs)

    rc = gate.main(argv, runner_override=fake_k, http_runner_override=http_with_5xx)
    assert rc == 1
    verdict = json.loads((out / "production-deployment-verdict.json").read_text())
    assert verdict["status"] == "FAILED"
    assert "was not canonically denied" in verdict["reason"]


def test_authenticated_mcp_invalid_token_accepted_fails(tmp_path):
    gate = _gate_module()
    out = tmp_path / "out_mcp_fail"
    argv = _valid_args(tmp_path, out)
    fake_k = _make_simulated_runner(gate)
    base_mcp_http = _make_simulated_http_runner(gate)

    def bad_mcp_http(method, path, **kwargs):
        if path == "/mcp":
            headers = kwargs.get("headers", {})
            auth = headers.get("Authorization", "")
            if "invalid" in auth:
                return gate.HttpResponse(200, text="accepted invalid token")
        return base_mcp_http(method, path, **kwargs)

    rc = gate.main(argv, runner_override=fake_k, http_runner_override=bad_mcp_http)
    assert rc == 1
    verdict = json.loads((out / "production-deployment-verdict.json").read_text())
    assert verdict["status"] == "FAILED"
    assert "Invalid MCP token was not canonically denied" in verdict["reason"]


def test_workflow_restart_failure(tmp_path):
    gate = _gate_module()
    out = tmp_path / "out_wf_fail"
    argv = _valid_args(tmp_path, out)
    fake_k = _make_simulated_runner(gate)
    fake_http = _make_simulated_http_runner(gate, wf_ok=False)

    rc = gate.main(argv, runner_override=fake_k, http_runner_override=fake_http)
    assert rc == 1
    verdict = json.loads((out / "production-deployment-verdict.json").read_text())
    assert verdict["status"] == "FAILED"
    assert "Workflow" in verdict["reason"]


def test_worker_pod_uid_unchanged_fails_restart(tmp_path):
    gate = _gate_module()
    out = tmp_path / "out_wf_uid_fail"
    argv = _valid_args(tmp_path, out)
    fake_k = _make_simulated_runner(gate, worker_uid_changes=False)
    fake_http = _make_simulated_http_runner(gate)

    rc = gate.main(argv, runner_override=fake_k, http_runner_override=fake_http)
    assert rc == 1
    verdict = json.loads((out / "production-deployment-verdict.json").read_text())
    assert verdict["status"] == "FAILED"
    assert "Worker pod UID did not change" in verdict["reason"]


def test_dlq_replay_unauthorized_accepted_fails(tmp_path):
    gate = _gate_module()
    out = tmp_path / "out_dlq_fail"
    argv = _valid_args(tmp_path, out)
    fake_k = _make_simulated_runner(gate)
    base_dlq_http = _make_simulated_http_runner(gate)

    def bad_dlq_http(method, path, **kwargs):
        if path.startswith("/ops/dlq") and "replay" in path:
            headers = kwargs.get("headers", {})
            auth = headers.get("Authorization", "")
            if "operator" not in auth:
                return gate.HttpResponse(200, data={"replayed": "unauthorized-replay"})
        return base_dlq_http(method, path, **kwargs)

    rc = gate.main(argv, runner_override=fake_k, http_runner_override=bad_dlq_http)
    assert rc == 1
    verdict = json.loads((out / "production-deployment-verdict.json").read_text())
    assert verdict["status"] == "FAILED"
    assert "Unauthorized DLQ replay was not canonically denied" in verdict["reason"]


def test_empty_dlq_reports_incomplete_prerequisite(tmp_path):
    gate = _gate_module()
    out = tmp_path / "out_dlq_empty"
    argv = _valid_args(tmp_path, out)
    fake_k = _make_simulated_runner(gate)
    fake_http = _make_simulated_http_runner(gate, dlq_empty=True)

    rc = gate.main(argv, runner_override=fake_k, http_runner_override=fake_http)
    assert rc == 2
    verdict = json.loads((out / "production-deployment-verdict.json").read_text())
    assert verdict["status"] == "INCOMPLETE_PREREQUISITE"
    assert "No DLQ quarantined entries available" in verdict["checks"]["dlq-routing-and-replay"]["details"]


def test_rolling_restart_task_loss_fails(tmp_path):
    gate = _gate_module()
    out = tmp_path / "out_rolling_fail"
    argv = _valid_args(tmp_path, out)
    fake_k = _make_simulated_runner(gate)
    fake_http = _make_simulated_http_runner(gate, rolling_ok=False)

    rc = gate.main(argv, runner_override=fake_k, http_runner_override=fake_http)
    assert rc == 1
    verdict = json.loads((out / "production-deployment-verdict.json").read_text())
    assert verdict["status"] == "FAILED"
    assert "Task admitted before restart failed to reach COMPLETED" in verdict["reason"]


def test_api_pod_uid_unchanged_fails_rolling_restart(tmp_path):
    gate = _gate_module()
    out = tmp_path / "out_api_uid_fail"
    argv = _valid_args(tmp_path, out)
    fake_k = _make_simulated_runner(gate, api_uid_changes=False)
    fake_http = _make_simulated_http_runner(gate)

    rc = gate.main(argv, runner_override=fake_k, http_runner_override=fake_http)
    assert rc == 1
    verdict = json.loads((out / "production-deployment-verdict.json").read_text())
    assert verdict["status"] == "FAILED"
    assert "API deployment pod UIDs did not change" in verdict["reason"]


def test_broker_outage_target_unowned_is_incomplete(tmp_path):
    gate = _gate_module()
    out = tmp_path / "out_broker_unowned"
    argv = _valid_args(tmp_path, out) + [
        "--allow-broker-disruption",
        "--disrupt-broker-target", "statefulset/nats",
    ]
    fake_k = _make_simulated_runner(gate, broker_target_owned=False)
    fake_http = _make_simulated_http_runner(gate)

    rc = gate.main(argv, runner_override=fake_k, http_runner_override=fake_http)
    assert rc == 2
    verdict = json.loads((out / "production-deployment-verdict.json").read_text())
    assert verdict["status"] == "INCOMPLETE_PREREQUISITE"
    assert "lacks 'app.kubernetes.io/managed-by=a2amq-production-gate'" in verdict["checks"]["broker-outage-recovery"]["details"]


def test_broker_false_completion_during_outage_fails(tmp_path):
    gate = _gate_module()
    out = tmp_path / "out_broker_false_complete"
    argv = _valid_args(tmp_path, out) + [
        "--allow-broker-disruption",
        "--disrupt-broker-target", "statefulset/nats",
    ]
    fake_k = _make_simulated_runner(gate)
    fake_http = _make_simulated_http_runner(gate, broker_false_complete=True)

    rc = gate.main(argv, runner_override=fake_k, http_runner_override=fake_http)
    assert rc == 1
    verdict = json.loads((out / "production-deployment-verdict.json").read_text())
    assert verdict["status"] == "FAILED"
    assert "falsely completed while broker was down" in verdict["reason"]


def test_image_digest_mismatch_fails_rollout_readiness(tmp_path):
    gate = _gate_module()
    out = tmp_path / "out_img_mismatch"
    argv = _valid_args(tmp_path, out)
    fake_k = _make_simulated_runner(gate, image_digest_ok=False)
    fake_http = _make_simulated_http_runner(gate)

    rc = gate.main(argv, runner_override=fake_k, http_runner_override=fake_http)
    assert rc == 1
    verdict = json.loads((out / "production-deployment-verdict.json").read_text())
    assert verdict["status"] == "FAILED"
    assert "image digest mismatch" in verdict["reason"]


def test_postgresql_component_contract_violation_fails(tmp_path):
    gate = _gate_module()
    out = tmp_path / "out_state_v2_fail"
    argv = _valid_args(tmp_path, out)
    fake_k = _make_simulated_runner(gate, state_v2_ok=False)
    fake_http = _make_simulated_http_runner(gate)

    rc = gate.main(argv, runner_override=fake_k, http_runner_override=fake_http)
    assert rc == 1
    verdict = json.loads((out / "production-deployment-verdict.json").read_text())
    assert verdict["status"] == "FAILED"
    assert "workflow-state" in verdict["reason"]


def test_missing_ready_condition_fails_dapr_prerequisite(tmp_path):
    gate = _gate_module()
    out = tmp_path / "out_dapr_not_ready"
    argv = _valid_args(tmp_path, out)
    fake_k = _make_simulated_runner(gate, dapr_ready=False)
    fake_http = _make_simulated_http_runner(gate)

    rc = gate.main(argv, runner_override=fake_k, http_runner_override=fake_http)
    assert rc == 2
    verdict = json.loads((out / "production-deployment-verdict.json").read_text())
    assert verdict["status"] == "INCOMPLETE_PREREQUISITE"
    assert "dapr-control-plane" in verdict["missingPrerequisites"]


def test_missing_namespace_reports_incomplete_prerequisite(tmp_path):
    gate = _gate_module()
    out = tmp_path / "out_no_ns"
    argv = _valid_args(tmp_path, out)
    fake_k = _make_simulated_runner(gate, namespace_exists=False)
    fake_http = _make_simulated_http_runner(gate)

    rc = gate.main(argv, runner_override=fake_k, http_runner_override=fake_http)
    assert rc == 2
    verdict = json.loads((out / "production-deployment-verdict.json").read_text())
    assert verdict["status"] == "INCOMPLETE_PREREQUISITE"
    assert "namespace" in verdict["missingPrerequisites"]


def test_dirty_git_fails_live_run(tmp_path):
    gate = _gate_module()
    out = tmp_path / "out_dirty_git"
    fake_k = _make_simulated_runner(gate)
    fake_http = _make_simulated_http_runner(gate)

    runner = gate.ProductionDeploymentRunner(
        k=gate.Kubectl(runner=fake_k),
        http=gate.HttpClient("https://a2amq.example.com", runner=fake_http),
        output=out,
        namespace="a2amq-test-ns",
        tenant="test-tenant",
        broker="jetstream",
        image="registry.example.org/a2amq@sha256:" + "a" * 64,
        public_url="https://a2amq.example.com",
        issuer="https://auth.example.com",
        jwt_kid="prod-key-1",
        public_key=VALID_PUBLIC_KEY,
        owner_token="owner-token",
        foreign_owner_token="foreign-token",
        cross_tenant_token="cross-tenant-token",
        mcp_token="mcp-token",
        operator_token="operator-token",
        allow_broker_disruption=True,
        disrupt_broker_target="statefulset/nats",
        execution_mode="live",
        initial_git={"head": "abc", "tree": "def", "status": "dirty"},
    )
    with patch.object(gate, "get_git_provenance", return_value={"head": "abc", "tree": "def", "status": "dirty"}):
        verdict = runner.run_all()
        assert verdict["status"] == "FAILED"
        assert "dirty" in verdict["reason"]


def test_infrastructure_check_failures(tmp_path):
    gate = _gate_module()
    out = tmp_path / "out_dapr_fail"
    argv = _valid_args(tmp_path, out)
    fake_k = _make_simulated_runner(gate, dapr_ok=False)
    rc = gate.main(argv, runner_override=fake_k)
    assert rc == 2
    verdict = json.loads((out / "production-deployment-verdict.json").read_text())
    assert verdict["status"] == "INCOMPLETE_PREREQUISITE"
    assert "dapr-control-plane" in verdict["missingPrerequisites"]


def test_dlq_unrelated_only_reports_incomplete_prerequisite(tmp_path):
    gate = _gate_module()
    out = tmp_path / "out_dlq_unrelated"
    argv = _valid_args(tmp_path, out)
    fake_k = _make_simulated_runner(gate)
    fake_http = _make_simulated_http_runner(gate, dlq_unrelated_only=True)

    rc = gate.main(argv, runner_override=fake_k, http_runner_override=fake_http)
    assert rc == 2
    verdict = json.loads((out / "production-deployment-verdict.json").read_text())
    assert verdict["status"] == "INCOMPLETE_PREREQUISITE"
    assert "unrelated records cannot satisfy" in verdict["checks"]["dlq-routing-and-replay"]["details"]


def test_dlq_replay_downstream_null_fails(tmp_path):
    gate = _gate_module()
    out = tmp_path / "out_dlq_null_rep"
    argv = _valid_args(tmp_path, out)
    fake_k = _make_simulated_runner(gate)
    fake_http = _make_simulated_http_runner(gate, dlq_replay_downstream_ok=False)

    rc = gate.main(argv, runner_override=fake_k, http_runner_override=fake_http)
    assert rc == 1
    verdict = json.loads((out / "production-deployment-verdict.json").read_text())
    assert verdict["status"] == "FAILED"
    assert "replayedAt remains null" in verdict["checks"]["dlq-routing-and-replay"]["details"]


def test_broker_scale_down_never_reaches_zero_fails(tmp_path):
    gate = _gate_module()
    out = tmp_path / "out_scale_zero_fail"
    argv = _valid_args(tmp_path, out) + [
        "--allow-broker-disruption",
        "--disrupt-broker-target", "statefulset/nats",
    ]
    fake_k = _make_simulated_runner(gate, broker_scale_down_to_zero=False)
    fake_http = _make_simulated_http_runner(gate)

    rc = gate.main(argv, runner_override=fake_k, http_runner_override=fake_http)
    assert rc == 1
    verdict = json.loads((out / "production-deployment-verdict.json").read_text())
    assert verdict["status"] == "FAILED"
    assert verdict["checks"]["broker-outage-recovery"]["zeroReplicasObserved"] is False


def test_restored_broker_with_zero_pods_fails(tmp_path):
    gate = _gate_module()
    out = tmp_path / "out_broker_zero_pods"
    argv = _valid_args(tmp_path, out) + [
        "--allow-broker-disruption",
        "--disrupt-broker-target", "statefulset/nats",
    ]
    fake_k = _make_simulated_runner(gate, broker_restored_pods_ok=False)
    fake_http = _make_simulated_http_runner(gate)

    rc = gate.main(argv, runner_override=fake_k, http_runner_override=fake_http)
    assert rc == 1
    verdict = json.loads((out / "production-deployment-verdict.json").read_text())
    assert verdict["status"] == "FAILED"
    assert "not restored to Ready state" in verdict["checks"]["broker-outage-recovery"]["details"]


def test_broker_disruption_missing_allow_label_is_incomplete(tmp_path):
    gate = _gate_module()
    out = tmp_path / "out_broker_no_optin"
    argv = _valid_args(tmp_path, out) + [
        "--allow-broker-disruption",
        "--disrupt-broker-target", "statefulset/nats",
    ]
    fake_k = _make_simulated_runner(gate, broker_disruption_label_ok=False)
    fake_http = _make_simulated_http_runner(gate)

    rc = gate.main(argv, runner_override=fake_k, http_runner_override=fake_http)
    assert rc == 2
    verdict = json.loads((out / "production-deployment-verdict.json").read_text())
    assert verdict["status"] == "INCOMPLETE_PREREQUISITE"
    assert gate.ALLOW_DISRUPTION_LABEL in verdict["checks"]["broker-outage-recovery"]["details"]


def test_broker_disruption_wrong_namespace_is_incomplete(tmp_path):
    gate = _gate_module()
    out = tmp_path / "out_broker_wrong_ns"
    argv = _valid_args(tmp_path, out) + [
        "--allow-broker-disruption",
        "--disrupt-broker-target", "statefulset/nats",
    ]
    fake_k = _make_simulated_runner(gate, broker_target_namespace="other-ns")
    fake_http = _make_simulated_http_runner(gate)

    rc = gate.main(argv, runner_override=fake_k, http_runner_override=fake_http)
    assert rc == 2
    verdict = json.loads((out / "production-deployment-verdict.json").read_text())
    assert verdict["status"] == "INCOMPLETE_PREREQUISITE"
    assert "does not match gate namespace" in verdict["checks"]["broker-outage-recovery"]["details"]


def test_workflow_worker_missing_uid_fails(tmp_path):
    gate = _gate_module()
    out = tmp_path / "out_wf_no_uid"
    argv = _valid_args(tmp_path, out)
    fake_k = _make_simulated_runner(gate, worker_has_uid_before=False)
    fake_http = _make_simulated_http_runner(gate)

    rc = gate.main(argv, runner_override=fake_k, http_runner_override=fake_http)
    assert rc == 1
    verdict = json.loads((out / "production-deployment-verdict.json").read_text())
    assert verdict["status"] == "FAILED"
    assert "missing or empty UID" in verdict["checks"]["workflow-execution-and-restart"]["details"]


def test_workflow_worker_no_ready_replacement_fails(tmp_path):
    gate = _gate_module()
    out = tmp_path / "out_wf_not_ready"
    argv = _valid_args(tmp_path, out)
    fake_k = _make_simulated_runner(gate, worker_replacement_ready=False)
    fake_http = _make_simulated_http_runner(gate)

    rc = gate.main(argv, runner_override=fake_k, http_runner_override=fake_http)
    assert rc == 1
    verdict = json.loads((out / "production-deployment-verdict.json").read_text())
    assert verdict["status"] == "FAILED"
    assert "No Ready worker replacement pods found" in verdict["checks"]["workflow-execution-and-restart"]["details"]


def test_rolling_restart_empty_pre_uids_fails(tmp_path):
    gate = _gate_module()
    out = tmp_path / "out_roll_no_pre"
    argv = _valid_args(tmp_path, out)
    fake_k = _make_simulated_runner(gate, api_has_uids_before=False)
    fake_http = _make_simulated_http_runner(gate)

    rc = gate.main(argv, runner_override=fake_k, http_runner_override=fake_http)
    assert rc == 1
    verdict = json.loads((out / "production-deployment-verdict.json").read_text())
    assert verdict["status"] == "FAILED"
    assert "No Ready a2amq-api pods with valid UIDs found before rolling restart" in verdict["checks"]["rolling-restart-zero-loss"]["details"]


def test_rolling_restart_empty_post_uids_fails(tmp_path):
    gate = _gate_module()
    out = tmp_path / "out_roll_no_post"
    argv = _valid_args(tmp_path, out)
    fake_k = _make_simulated_runner(gate, api_has_uids_after=False)
    fake_http = _make_simulated_http_runner(gate)

    rc = gate.main(argv, runner_override=fake_k, http_runner_override=fake_http)
    assert rc == 1
    verdict = json.loads((out / "production-deployment-verdict.json").read_text())
    assert verdict["status"] == "FAILED"
    assert "No Ready a2amq-api replacement pods with valid UIDs found after rolling restart" in verdict["checks"]["rolling-restart-zero-loss"]["details"]


def test_pdb_wrong_selector_fails(tmp_path):
    gate = _gate_module()
    out = tmp_path / "out_pdb_sel_fail"
    argv = _valid_args(tmp_path, out)
    fake_k = _make_simulated_runner(gate, pdb_selector_ok=False)
    fake_http = _make_simulated_http_runner(gate)

    rc = gate.main(argv, runner_override=fake_k, http_runner_override=fake_http)
    assert rc == 1
    verdict = json.loads((out / "production-deployment-verdict.json").read_text())
    assert verdict["status"] == "FAILED"
    assert "selector mismatch" in verdict["reason"]


def test_cleanup_removes_dapr_components_isolated_by_run_id(tmp_path):
    gate = _gate_module()
    out = tmp_path / "out_cleanup"
    out.mkdir(parents=True, exist_ok=True)

    disposable_map = {
        "run-A": [{"metadata": {"name": "comp-A", "labels": {gate.GATE_RUN_ID_LABEL: "run-A"}}}],
        "run-B": [{"metadata": {"name": "comp-B", "labels": {gate.GATE_RUN_ID_LABEL: "run-B"}}}],
    }

    fake_k = _make_simulated_runner(gate, disposable_resources_map=disposable_map)
    runner_a = gate.ProductionDeploymentRunner(
        k=gate.Kubectl(runner=fake_k),
        http=gate.HttpClient("https://a2amq.example.com"),
        output=out,
        namespace="a2amq-test-ns",
        tenant="test-tenant",
        broker="jetstream",
        image="registry.example.org/a2amq@sha256:" + "a" * 64,
        public_url="https://a2amq.example.com",
        issuer="https://auth.example.com",
        jwt_kid="prod-key-1",
        public_key=VALID_PUBLIC_KEY,
        execution_mode="simulation",
    )
    runner_a.run_id = "run-A"

    cleanup_res = runner_a.cleanup(success=True)
    assert cleanup_res["attempted"] is True
    assert cleanup_res["resourcesDeleted"] is True
    assert cleanup_res["remainingGateResources"] == 0

    # Verify run-A resources were deleted, while run-B resources remain untouched
    assert "run-A" not in disposable_map
    assert "run-B" in disposable_map
    assert len(disposable_map["run-B"]) == 1

