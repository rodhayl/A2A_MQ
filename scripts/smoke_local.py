#!/usr/bin/env python3
"""Small end-to-end Local-lite smoke against an already running profile."""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys
import time
from uuid import uuid4

import httpx

from a2a_mq.configuration import read_file
from a2a_mq.local_runtime import _load_profile


TERMINAL = {
    "TASK_STATE_COMPLETED",
    "TASK_STATE_FAILED",
    "TASK_STATE_CANCELED",
    "TASK_STATE_REJECTED",
}


def _headers(token: str) -> dict[str, str]:
    return {"Authorization": "Bearer " + token, "A2A-Version": "1.0"}


def _rpc(client: httpx.Client, token: str, method: str, params: dict) -> dict:
    response = client.post(
        "/rpc",
        headers={**_headers(token), "Content-Type": "application/json"},
        json={"jsonrpc": "2.0", "id": "local-smoke", "method": method, "params": params},
    )
    response.raise_for_status()
    value = response.json()
    if value.get("error"):
        raise RuntimeError(f"{method} returned JSON-RPC error: {value['error']}")
    result = value.get("result")
    if not isinstance(result, dict):
        raise RuntimeError(f"{method} returned an invalid result")
    return result


def _task(value: dict) -> dict:
    task = value.get("task", value)
    if not isinstance(task, dict) or not isinstance(task.get("id"), str):
        raise RuntimeError("A2A response did not contain a task")
    return task


def _state(task: dict) -> str:
    status = task.get("status")
    return status.get("state", "") if isinstance(status, dict) else ""


def _pass(name: str, detail: str = "") -> None:
    suffix = f"  {detail}" if detail else ""
    print(f"PASS  {name}{suffix}", flush=True)


def smoke(root: Path, *, timeout: float = 20.0) -> dict:
    root = root.absolute()
    profile = _load_profile(root, "local-lite")
    token = read_file(str(root / "client.jwt"), secret=True, limit=16384).strip()
    if not token:
        raise RuntimeError("client.jwt is empty")

    started = time.monotonic()
    with httpx.Client(base_url=profile["publicUrl"], timeout=5.0) as client:
        live = client.get("/health/live")
        live.raise_for_status()
        _pass("health/live")

        ready = client.get("/health/ready")
        ready.raise_for_status()
        if ready.json().get("status") != "ready":
            raise RuntimeError("health/ready did not report ready")
        _pass("health/ready")

        card = client.get("/.well-known/agent-card.json")
        card.raise_for_status()
        if not isinstance(card.json(), dict):
            raise RuntimeError("Agent Card response is not an object")
        _pass("Agent Card")

        message_id = "local-smoke-" + uuid4().hex
        admitted = _task(_rpc(client, token, "SendMessage", {
            "message": {
                "messageId": message_id,
                "role": "ROLE_USER",
                "parts": [{"text": "A2A-MQ local smoke"}],
            },
            "configuration": {"returnImmediately": True},
        }))
        task_id = admitted["id"]
        _pass("SendMessage", task_id)

        deadline = time.monotonic() + timeout
        current = admitted
        while _state(current) != "TASK_STATE_COMPLETED":
            state = _state(current)
            if state in TERMINAL:
                raise RuntimeError(f"task reached unexpected terminal state: {state}")
            if time.monotonic() >= deadline:
                raise RuntimeError(f"task did not complete within {timeout:g}s")
            time.sleep(0.2)
            current = _task(_rpc(client, token, "GetTask", {"id": task_id, "historyLength": 100}))

        _pass("GetTask", "TASK_STATE_COMPLETED")

        status = client.get("/operations/status", headers=_headers(token))
        status.raise_for_status()
        if not isinstance(status.json(), dict):
            raise RuntimeError("operations/status response is not an object")
        _pass("operations/status")

    result = {
        "verdict": "PASS_LOCAL_SMOKE",
        "taskId": task_id,
        "elapsedSeconds": round(time.monotonic() - started, 3),
    }
    print(json.dumps(result, sort_keys=True))
    print("PASS_LOCAL_SMOKE")
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("profile", type=Path)
    parser.add_argument("--timeout", type=float, default=20.0)
    args = parser.parse_args()
    if not 1 <= args.timeout <= 120:
        parser.error("--timeout must be between 1 and 120 seconds")
    try:
        smoke(args.profile, timeout=args.timeout)
    except (httpx.HTTPError, RuntimeError, ValueError) as exc:
        print(f"FAIL_LOCAL_SMOKE: {exc}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
