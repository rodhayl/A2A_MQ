"""Opt-in destructive tests for the named disposable Compose fixture only."""
import json
import os
from pathlib import Path
import shutil
import subprocess
import time

import httpx
import pytest

from a2a_mq.domain import command
from a2a_mq.reference import echo_events
from a2a_mq.transport import DaprPublisher

ROOT = Path(__file__).resolve().parents[1]
pytestmark = pytest.mark.live


def compose(*args):
    subprocess.run(["docker", "compose", "-f", str(ROOT / "deploy/compose.yaml"), *args], check=True, timeout=180)


def wait_ready(url):
    deadline = time.monotonic() + 90
    while time.monotonic() < deadline:
        try:
            if httpx.get(url, timeout=2).status_code == 200:
                return
        except httpx.HTTPError:
            pass
        time.sleep(0.5)
    raise AssertionError("fixture never became ready: " + url)


async def test_offline_workers_competition_and_independent_observers():
    if os.getenv("A2A_LIVE") != "1" or not shutil.which("docker"):
        pytest.skip("requires A2A_LIVE=1, Docker and disposable local stack")
    subprocess.run(["python", str(ROOT / "deploy/generate.py")], check=True)
    compose("down", "-v")
    try:
        compose("up", "--build", "-d", "producer", "producer-dapr")
        wait_ready("http://127.0.0.1:18000/health/ready")
        envelope = command("offline worker experiment")
        bus = DaprPublisher()
        try:
            await bus.publish(envelope)
        finally:
            await bus.close()
        compose("up", "--build", "-d", "worker-a", "worker-a-dapr", "worker-b", "worker-b-dapr",
                "observer-a", "observer-a-dapr", "observer-b", "observer-b-dapr")
        expected = {e.as_dict()["messageId"] for e in echo_events(envelope)}
        for port in (18001, 18002):
            wait_ready(f"http://127.0.0.1:{port}/health/ready")
            deadline = time.monotonic() + 60
            observed = set()
            while time.monotonic() < deadline:
                response = httpx.get(f"http://127.0.0.1:{port}/internal/observations", timeout=3)
                observed = {event["id"] for event in response.json()}
                if expected <= observed:
                    break
                time.sleep(0.2)
            assert expected <= observed, json.dumps(sorted(observed))
    finally:
        compose("down", "-v")
