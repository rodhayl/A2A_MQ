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


def _make_simulated_runner(gate, *, cluster_ok=True, dapr_ok=True, mtls_ok=True,
                           sched_ok=True, secrets_ok=True, dry_run_ok=True,
                           rollout_ok=True, pod_sec_ok=True, sidecar_ok=True,
                           pdb_ok=True, state_ok=True, broker_target_owned=True):
    """Factory creating a simulated Kubectl runner returning configurable responses."""
    worker_restart_count = [0]

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
            return gate.CommandResult(0, json.dumps({"items": [
                {
                    "metadata": {"name": "dapr-operator-xyz"},
                    "spec": {"containers": [{"image": "daprio/dapr:1.18.3"}]},
                    "status": {"phase": "Running", "containerStatuses": [{"ready": True}]},
                }
            ]}), "")

        if "dapr-config" in cmd_str:
            return gate.CommandResult(0, json.dumps({"spec": {"mtls": {"enabled": mtls_ok}}}), "")

        if "app=dapr-placement" in cmd_str:
            if not sched_ok:
                return gate.CommandResult(0, json.dumps({"items": []}), "")
            return gate.CommandResult(0, json.dumps({"items": [
                {"metadata": {"name": "dapr-placement-0"}, "status": {"phase": "Running"}}
            ]}), "")

        if "app=dapr-scheduler" in cmd_str:
            if not sched_ok:
                return gate.CommandResult(0, json.dumps({"items": []}), "")
            return gate.CommandResult(0, json.dumps({"items": [
                {"metadata": {"name": "dapr-scheduler-0"}, "status": {"phase": "Running"}}
            ]}), "")

        if "get namespace" in cmd_str:
            return gate.CommandResult(0, json.dumps({"metadata": {"name": "a2amq-test-ns"}}), "")

        if "get secrets" in cmd_str:
            names = ["a2amq-postgres", "a2amq-workflow-state", "dapr-api-token", "app-api-token"] if secrets_ok else ["a2amq-postgres"]
            return gate.CommandResult(0, json.dumps({"items": [{"metadata": {"name": n}} for n in names]}), "")

        if "delete pod a2amq-worker" in cmd_str:
            worker_restart_count[0] += 1
            return gate.CommandResult(0, "pod deleted", "")

        if "rollout status" in cmd_str:
            if not rollout_ok:
                return gate.CommandResult(1, "", "Deployment rollout timed out")
            return gate.CommandResult(0, "successfully rolled out", "")

        if "rollout restart" in cmd_str:
            return gate.CommandResult(0, "restarted", "")

        if "scale" in cmd_str:
            return gate.CommandResult(0, "scaled", "")

        if "get statefulset/nats" in cmd_str or "get deployment/nats" in cmd_str:
            labels = {"app.kubernetes.io/managed-by": "a2amq-production-gate"} if broker_target_owned else {}
            return gate.CommandResult(0, json.dumps({"metadata": {"name": "nats", "labels": labels}}), "")

        if "app=a2amq-worker" in cmd_str and "get pods" in cmd_str:
            pname = "a2amq-worker-pod-2" if worker_restart_count[0] > 0 else "a2amq-worker-pod-1"
            return gate.CommandResult(0, json.dumps({"items": [{"metadata": {"name": pname}}]}), "")

        if "app.kubernetes.io/name=a2a-mq" in cmd_str and "get pods" in cmd_str:
            containers = []
            c_statuses = []
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
                    "imageID": "registry.example.org/a2amq@sha256:" + "a" * 64,
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

            pod = {
                "metadata": {"name": "a2amq-api-xyz", "labels": {"app": "a2amq-api"}},
                "spec": {
                    "automountServiceAccountToken": False if pod_sec_ok else True,
                    "securityContext": {"seccompProfile": {"type": "RuntimeDefault" if pod_sec_ok else "Unconfined"}},
                    "containers": containers,
                },
                "status": {"containerStatuses": c_statuses},
            }
            return gate.CommandResult(0, json.dumps({"items": [pod]}), "")

        if "get pdb" in cmd_str:
            if not pdb_ok:
                return gate.CommandResult(0, json.dumps({"items": []}), "")
            items = []
            for n in ["a2amq-api", "a2amq-worker", "a2amq-dlq"]:
                items.append({
                    "metadata": {"name": n},
                    "spec": {"minAvailable": 1, "selector": {"matchLabels": {"app": n}}},
                    "status": {"currentHealthy": 2, "desiredHealthy": 1, "disruptionsAllowed": 1},
                })
            return gate.CommandResult(0, json.dumps({"items": items}), "")

        if "get components" in cmd_str:
            if not state_ok:
                return gate.CommandResult(0, json.dumps({"items": []}), "")
            return gate.CommandResult(0, json.dumps({"items": [
                {
                    "metadata": {"name": "workflow-state"},
                    "spec": {
                        "type": "state.postgresql",
                        "metadata": [{"name": "actorStateStore", "value": "true"}],
                    },
                    "scopes": ["a2amq-worker"],
                }
            ]}), "")

        return gate.CommandResult(0, "success", "")

    return fake_run


def _make_simulated_http_runner(gate, *, task_ok=True, auth_ok=True, mcp_ok=True,
                                wf_ok=True, dlq_ok=True, rolling_ok=True):
    """Factory creating a simulated HTTP client runner."""
    def fake_http(method: str, path: str, *, headers=None, json_data=None, timeout=30.0):
        headers = headers or {}
        auth = headers.get("Authorization", "")

        # 1. MCP endpoint
        if path == "/mcp":
            if "invalid" in auth or not auth:
                return gate.HttpResponse(401, text="Unauthorized")
            if not mcp_ok:
                return gate.HttpResponse(500, text="MCP internal error")
            method_call = (json_data or {}).get("method")
            if method_call == "tools/list":
                return gate.HttpResponse(200, data={"result": {"tools": [{"name": "tasks.get"}, {"name": "tasks.list"}]}})
            if method_call == "tools/call":
                return gate.HttpResponse(200, data={"result": {"id": "task-mcp-queried"}})
            return gate.HttpResponse(200, data={"result": "ok"})

        # 2. DLQ ops endpoint
        if path.startswith("/ops/dlq"):
            if "operator-token" not in auth:
                return gate.HttpResponse(403, text="Forbidden")
            if not dlq_ok:
                return gate.HttpResponse(500, text="DLQ error")
            if method == "GET":
                return gate.HttpResponse(200, data={"items": [{"entryId": "dlq-entry-1"}]})
            if method == "POST" and "replay" in path:
                return gate.HttpResponse(200, data={"replayed": "replayed-msg-1"})

        # 3. Tasks JSON-RPC endpoint
        if path == "/":
            method_call = (json_data or {}).get("method")
            params = (json_data or {}).get("params", {})

            # Foreign owner rejection test
            if "foreign-token" in auth or "cross-tenant" in auth:
                if not auth_ok:
                    return gate.HttpResponse(200, data={"result": {"id": params.get("id")}})
                return gate.HttpResponse(403, text="Forbidden")

            # Task submission
            if method_call == "tasks/send":
                if not task_ok:
                    return gate.HttpResponse(500, text="Task admission failed")
                mid = params.get("message", {}).get("messageId", "sim-task-1")
                return gate.HttpResponse(200, data={"result": {"id": f"task-{mid}", "status": {"state": "TASK_STATE_SUBMITTED"}}})

            # Task query
            if method_call == "tasks/get":
                task_id = params.get("id", "")
                if "roll-" in task_id and not rolling_ok:
                    return gate.HttpResponse(404, text="Task lost in restart")
                if "wf-" in task_id and not wf_ok:
                    return gate.HttpResponse(500, text="Workflow lost")
                return gate.HttpResponse(200, data={"result": {"id": task_id, "status": {"state": "TASK_STATE_COMPLETED"}}})

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
    assert "Task submission returned HTTP 500" in verdict["reason"]


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
    assert "Foreign owner was able to read" in verdict["reason"]


def test_authenticated_mcp_invalid_token_accepted_fails(tmp_path):
    gate = _gate_module()
    out = tmp_path / "out_mcp_fail"
    argv = _valid_args(tmp_path, out)
    fake_k = _make_simulated_runner(gate)

    def bad_mcp_http(method, path, **kwargs):
        if path == "/mcp":
            return gate.HttpResponse(200, text="accepted invalid token")
        return _make_simulated_http_runner(gate)(method, path, **kwargs)

    rc = gate.main(argv, runner_override=fake_k, http_runner_override=bad_mcp_http)
    assert rc == 1
    verdict = json.loads((out / "production-deployment-verdict.json").read_text())
    assert verdict["status"] == "FAILED"
    assert "Invalid MCP token was accepted" in verdict["reason"]


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


def test_dlq_replay_unauthorized_accepted_fails(tmp_path):
    gate = _gate_module()
    out = tmp_path / "out_dlq_fail"
    argv = _valid_args(tmp_path, out)
    fake_k = _make_simulated_runner(gate)

    def bad_dlq_http(method, path, **kwargs):
        if path.startswith("/ops/dlq") and "replay" in path:
            return gate.HttpResponse(200, data={"replayed": "unauthorized-replay"})
        return _make_simulated_http_runner(gate)(method, path, **kwargs)

    rc = gate.main(argv, runner_override=fake_k, http_runner_override=bad_dlq_http)
    assert rc == 1
    verdict = json.loads((out / "production-deployment-verdict.json").read_text())
    assert verdict["status"] == "FAILED"
    assert "Unauthorized identity was able to replay DLQ message" in verdict["reason"]


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
    assert "not completed after rolling restart" in verdict["reason"]


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
    assert "not labeled as gate-managed" in verdict["checks"]["broker-outage-recovery"]["details"]


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
