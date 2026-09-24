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


def _valid_args(tmp_path: Path, out_dir: Path) -> list[str]:
    key_file = _make_key_file(tmp_path)
    return [
        "--output", str(out_dir),
        "--image", "registry.example.org/a2amq@sha256:" + "a" * 64,
        "--public-url", "https://a2amq.example.com",
        "--issuer", "https://auth.example.com",
        "--jwt-kid", "prod-key-1",
        "--jwt-public-key-file", str(key_file),
        "--namespace", "a2amq-test-ns",
    ]


def test_manifest_rendering():
    gate = _gate_module()
    manifests = gate.render_production_manifests(
        namespace="test-ns",
        tenant="test-tenant",
        broker="jetstream",
        image="registry.example/a2amq@sha256:" + "b" * 64,
        public_url="https://agent.example",
        issuer="https://issuer.example",
        jwt_kid="key-1",
        public_key=VALID_PUBLIC_KEY,
    )
    assert manifests["kind"] == "List"
    items = manifests["items"]
    deployments = {item["metadata"]["name"] for item in items if item["kind"] == "Deployment"}
    assert deployments == {"a2amq-api", "a2amq-worker", "a2amq-dlq"}
    for item in items:
        assert item["metadata"]["labels"]["app.kubernetes.io/managed-by"] == "a2amq-production-gate"


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
    # Write invalid key containing PRIVATE KEY
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
                           pdb_ok=True, state_ok=True):
    """Factory creating a simulated Kubectl runner returning configurable responses."""
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

        if "dapr.io/control-plane=true" in cmd_str:
            if not dapr_ok:
                return gate.CommandResult(0, json.dumps({"items": []}), "")
            return gate.CommandResult(0, json.dumps({"items": [{"metadata": {"name": "dapr-operator"}}]}), "")

        if "dapr-config" in cmd_str:
            return gate.CommandResult(0, json.dumps({"spec": {"mtls": {"enabled": mtls_ok}}}), "")

        if "app=dapr-placement" in cmd_str:
            if not sched_ok:
                return gate.CommandResult(0, json.dumps({"items": []}), "")
            return gate.CommandResult(0, json.dumps({"items": [{"metadata": {"name": "dapr-placement-0"}}]}), "")

        if "app=dapr-scheduler" in cmd_str:
            if not sched_ok:
                return gate.CommandResult(0, json.dumps({"items": []}), "")
            return gate.CommandResult(0, json.dumps({"items": [{"metadata": {"name": "dapr-scheduler-0"}}]}), "")

        if "get namespace" in cmd_str:
            return gate.CommandResult(0, json.dumps({"metadata": {"name": "a2amq-test-ns"}}), "")

        if "get secrets" in cmd_str:
            names = ["a2amq-postgres", "a2amq-workflow-state", "dapr-api-token", "app-api-token"] if secrets_ok else ["a2amq-postgres"]
            return gate.CommandResult(0, json.dumps({"items": [{"metadata": {"name": n}} for n in names]}), "")

        if "rollout status" in cmd_str:
            if not rollout_ok:
                return gate.CommandResult(1, "", "Deployment rollout timed out")
            return gate.CommandResult(0, "successfully rolled out", "")

        if "app.kubernetes.io/name=a2a-mq" in cmd_str and "get pods" in cmd_str:
            containers = []
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
            if sidecar_ok:
                containers.append({"name": "daprd"})

            pod = {
                "metadata": {"name": "a2amq-api-xyz"},
                "spec": {
                    "automountServiceAccountToken": False if pod_sec_ok else True,
                    "securityContext": {"seccompProfile": {"type": "RuntimeDefault" if pod_sec_ok else "Unconfined"}},
                    "containers": containers,
                },
            }
            return gate.CommandResult(0, json.dumps({"items": [pod]}), "")

        if "get pdb" in cmd_str:
            names = ["a2amq-api", "a2amq-worker", "a2amq-dlq"] if pdb_ok else []
            return gate.CommandResult(0, json.dumps({"items": [{"metadata": {"name": n}} for n in names]}), "")

        if "get components" in cmd_str:
            names = ["workflow-state"] if state_ok else []
            return gate.CommandResult(0, json.dumps({"items": [{"metadata": {"name": n}} for n in names]}), "")

        return gate.CommandResult(0, "success", "")

    return fake_run


def test_runner_dapr_control_plane_missing(tmp_path):
    gate = _gate_module()
    out = tmp_path / "out_dapr_missing"
    argv = _valid_args(tmp_path, out)
    fake_run = _make_simulated_runner(gate, dapr_ok=False)
    rc = gate.main(argv, runner_override=fake_run)
    assert rc == 2
    verdict = json.loads((out / "production-deployment-verdict.json").read_text())
    assert verdict["status"] == "INCOMPLETE_PREREQUISITE"
    assert "dapr-control-plane" in verdict["missingPrerequisites"]


def test_runner_dapr_mtls_missing(tmp_path):
    gate = _gate_module()
    out = tmp_path / "out_mtls_missing"
    argv = _valid_args(tmp_path, out)
    fake_run = _make_simulated_runner(gate, mtls_ok=False)
    rc = gate.main(argv, runner_override=fake_run)
    assert rc == 2
    verdict = json.loads((out / "production-deployment-verdict.json").read_text())
    assert verdict["status"] == "INCOMPLETE_PREREQUISITE"
    assert "dapr-mtls" in verdict["missingPrerequisites"]


def test_runner_placement_scheduler_missing(tmp_path):
    gate = _gate_module()
    out = tmp_path / "out_sched_missing"
    argv = _valid_args(tmp_path, out)
    fake_run = _make_simulated_runner(gate, sched_ok=False)
    rc = gate.main(argv, runner_override=fake_run)
    assert rc == 2
    verdict = json.loads((out / "production-deployment-verdict.json").read_text())
    assert verdict["status"] == "INCOMPLETE_PREREQUISITE"
    assert "dapr-placement" in verdict["missingPrerequisites"]


def test_runner_secrets_missing(tmp_path):
    gate = _gate_module()
    out = tmp_path / "out_secrets_missing"
    argv = _valid_args(tmp_path, out)
    fake_run = _make_simulated_runner(gate, secrets_ok=False)
    rc = gate.main(argv, runner_override=fake_run)
    assert rc == 2
    verdict = json.loads((out / "production-deployment-verdict.json").read_text())
    assert verdict["status"] == "INCOMPLETE_PREREQUISITE"
    assert "app-api-token" in verdict["missingPrerequisites"]


def test_runner_dry_run_failure(tmp_path):
    gate = _gate_module()
    out = tmp_path / "out_dry_fail"
    argv = _valid_args(tmp_path, out)
    fake_run = _make_simulated_runner(gate, dry_run_ok=False)
    rc = gate.main(argv, runner_override=fake_run)
    assert rc == 1
    verdict = json.loads((out / "production-deployment-verdict.json").read_text())
    assert verdict["status"] == "FAILED"
    assert "dry-run rejected" in verdict["reason"]


def test_runner_rollout_failure(tmp_path):
    gate = _gate_module()
    out = tmp_path / "out_rollout_fail"
    argv = _valid_args(tmp_path, out)
    fake_run = _make_simulated_runner(gate, rollout_ok=False)
    rc = gate.main(argv, runner_override=fake_run)
    assert rc == 1
    verdict = json.loads((out / "production-deployment-verdict.json").read_text())
    assert verdict["status"] == "FAILED"
    assert "rollout failed" in verdict["reason"]


def test_runner_pod_security_context_violation(tmp_path):
    gate = _gate_module()
    out = tmp_path / "out_sec_fail"
    argv = _valid_args(tmp_path, out)
    fake_run = _make_simulated_runner(gate, pod_sec_ok=False)
    rc = gate.main(argv, runner_override=fake_run)
    assert rc == 1
    verdict = json.loads((out / "production-deployment-verdict.json").read_text())
    assert verdict["status"] == "FAILED"
    assert "security violation" in verdict["reason"] or "securityContext" in verdict["reason"]


def test_runner_sidecar_missing(tmp_path):
    gate = _gate_module()
    out = tmp_path / "out_sidecar_fail"
    argv = _valid_args(tmp_path, out)
    fake_run = _make_simulated_runner(gate, sidecar_ok=False)
    rc = gate.main(argv, runner_override=fake_run)
    assert rc == 1
    verdict = json.loads((out / "production-deployment-verdict.json").read_text())
    assert verdict["status"] == "FAILED"
    assert "missing sidecar" in verdict["reason"]


def test_runner_pdb_missing(tmp_path):
    gate = _gate_module()
    out = tmp_path / "out_pdb_fail"
    argv = _valid_args(tmp_path, out)
    fake_run = _make_simulated_runner(gate, pdb_ok=False)
    rc = gate.main(argv, runner_override=fake_run)
    assert rc == 1
    verdict = json.loads((out / "production-deployment-verdict.json").read_text())
    assert verdict["status"] == "FAILED"
    assert "PDBs not active" in verdict["reason"]


def test_runner_state_store_missing(tmp_path):
    gate = _gate_module()
    out = tmp_path / "out_state_fail"
    argv = _valid_args(tmp_path, out)
    fake_run = _make_simulated_runner(gate, state_ok=False)
    rc = gate.main(argv, runner_override=fake_run)
    assert rc == 1
    verdict = json.loads((out / "production-deployment-verdict.json").read_text())
    assert verdict["status"] == "FAILED"
    assert "Workflow state store" in verdict["reason"]


def test_runner_default_disruption_not_authorized(tmp_path):
    gate = _gate_module()
    out = tmp_path / "out_no_disrupt"
    argv = _valid_args(tmp_path, out)
    fake_run = _make_simulated_runner(gate)
    rc = gate.main(argv, runner_override=fake_run)
    # Default is allow_broker_disruption=False, which safely stops with INCOMPLETE_PREREQUISITE
    assert rc == 2
    verdict = json.loads((out / "production-deployment-verdict.json").read_text())
    assert verdict["status"] == "INCOMPLETE_PREREQUISITE"
    assert "broker-disruption-opt-in" in verdict["missingPrerequisites"]


def test_runner_all_pass_with_broker_disruption_opt_in(tmp_path):
    gate = _gate_module()
    out = tmp_path / "out_all_pass"
    argv = _valid_args(tmp_path, out) + ["--allow-broker-disruption"]
    fake_run = _make_simulated_runner(gate)
    rc = gate.main(argv, runner_override=fake_run)
    assert rc == 0
    verdict = json.loads((out / "production-deployment-verdict.json").read_text())
    assert verdict["status"] == "PRODUCTION_DEPLOYMENT_PASS"
    assert verdict["productionCertified"] is False
    assert verdict["executedCheckCount"] == 19
    assert verdict["missingChecks"] == []
    assert (out / "sha256.json").is_file()


def test_runner_missing_required_check_prevents_pass(tmp_path):
    gate = _gate_module()
    out = tmp_path / "out_missing_check"
    k = gate.Kubectl()
    runner = gate.ProductionDeploymentRunner(
        k=k,
        output=out,
        namespace="test-ns",
        broker="jetstream",
        image="registry.example/a2amq@sha256:" + "a" * 64,
        public_url="https://a2amq.example.com",
        issuer="https://auth.example.com",
        jwt_kid="key-1",
        public_key=VALID_PUBLIC_KEY,
    )
    # Only record 1 check
    runner.record("manifest-synthesis-and-validation", "PASSED")
    # Attempting to return PASS with missing checks
    verdict = runner._build_verdict("PRODUCTION_DEPLOYMENT_PASS", "All checks passed")
    assert verdict["status"] == "FAILED"
    assert "Required checks missing from verdict" in verdict["reason"]
