import json
from unittest.mock import patch

import scripts.production_deployment_gate as gate


def test_manifest_rendering():
    manifests = gate.render_production_manifests(
        namespace="test-ns",
        tenant="test-tenant",
        broker="jetstream",
        image="registry.example/a2amq@sha256:" + "b" * 64,
        public_url="https://agent.example",
        issuer="https://issuer.example",
        jwt_kid="key-1",
        public_key="-----BEGIN PUBLIC KEY-----\nfixture\n-----END PUBLIC KEY-----\n",
    )
    assert manifests["kind"] == "List"
    items = manifests["items"]
    deployments = {item["metadata"]["name"] for item in items if item["kind"] == "Deployment"}
    assert deployments == {"a2amq-api", "a2amq-worker", "a2amq-dlq"}


def test_prerequisites_missing_kubectl():
    with patch("shutil.which", return_value=None):
        missing = gate.check_prerequisites()
        assert missing == ["kubectl"]


def test_cli_rejects_in_repo_output(tmp_path):
    repo_output = gate.ROOT / "tests" / "scratch_output"
    with patch("sys.argv", ["gate", "--output", str(repo_output)]):
        rc = gate.main()
        assert rc == 1


def test_cli_rejects_existing_output(tmp_path):
    existing = tmp_path / "already_exists"
    existing.mkdir()
    with patch("sys.argv", ["gate", "--output", str(existing)]):
        rc = gate.main()
        assert rc == 1


def test_cli_missing_kubectl_clean_exit(tmp_path):
    out = tmp_path / "gate_evidence"
    with patch("sys.argv", ["gate", "--output", str(out)]):
        rc = gate.main()
        assert rc == 2
        verdict_file = out / "production-deployment-verdict.json"
        assert verdict_file.is_file()
        verdict = json.loads(verdict_file.read_text(encoding="utf-8"))
        assert verdict["status"] == "INCOMPLETE_PREREQUISITE"
        assert verdict["productionCertified"] is False
        assert "kubectl" in verdict["missingPrerequisites"]
        assert (out / "sha256.json").is_file()
