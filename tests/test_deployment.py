import importlib.util
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location("deployment", ROOT / "deploy/generate.py")
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


def test_distinct_consumer_identities_and_stream():
    output = MODULE.render()
    assert output["generated/stream.json"]["retention"] == "limits"
    groups = []
    for role in ("worker-echo", "observer-a", "observer-b"):
        values = {item["name"]: item["value"] for item in output[f"generated/{role}/bus.yaml"]["spec"]["metadata"]}
        assert values["durableName"] == values["queueGroupName"]
        groups.append(values["durableName"])
    assert len(set(groups)) == 3
    services = output["compose.yaml"]["services"]
    assert services["worker-a"]["environment"]["A2A_CONSUMER"] == services["worker-b"]["environment"]["A2A_CONSUMER"]
    assert services["worker-a"]["volumes"] == services["worker-b"]["volumes"]
    for service in services.values():
        assert all(port.startswith("127.0.0.1:") for port in service.get("ports", []))


def test_generation_and_retry_policy(tmp_path):
    MODULE.generate(tmp_path)
    assert (tmp_path / "compose.yaml").exists()
    worker = MODULE.render()["generated/worker-echo/resiliency.yaml"]
    assert worker["spec"]["policies"]["retries"]["bounded"]["maxRetries"] == 20
    assert "inbound" in worker["spec"]["targets"]["components"]["bus"]
