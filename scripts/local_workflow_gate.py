"""Formal local Workflow acceptance gate: Resource, Functional, and Resilience validation.

Exercises the real 5-process topology (app + daprd + nats + placement + scheduler)
through official Dapr Workflow runtime and native durable dispatch.
"""
from __future__ import annotations

import argparse
import asyncio
from contextlib import closing
import json
import os
from pathlib import Path
import shutil
import socket
import subprocess
import sys
import time
from typing import Any

import httpx
from cryptography.hazmat.primitives import serialization
import jwt

from a2a_mq.domain import require
from a2a_mq.local_runtime import (
    APP_ID,
    DAPR_INTERNAL_GRPC_PORT,
    _load_profile,
    _runtime_environments,
)
from a2a_mq.local_workflow_runtime import (
    PLACEMENT_PORT,
    SCHEDULER_PORT,
    _placement_command,
    _scheduler_command,
    prepare as prepare_workflow,
)


def _check_sqlite_integrity(db_path: Path) -> dict[str, Any]:
    import sqlite3
    if not db_path.exists():
        return {"clean": False, "error": "DB_NOT_FOUND"}
    with closing(sqlite3.connect(str(db_path))) as conn:
        conn.row_factory = sqlite3.Row
        integ = [r[0] for r in conn.execute("PRAGMA integrity_check").fetchall()]
        quick = [r[0] for r in conn.execute("PRAGMA quick_check").fetchall()]
        fk_errs = len(conn.execute("PRAGMA foreign_key_check").fetchall())
        tasks = conn.execute("SELECT COUNT(*) FROM a2amq_tasks").fetchone()[0]
        ends = conn.execute("SELECT COUNT(*) FROM a2amq_runtime_end").fetchone()[0]
        pending_actions = conn.execute("SELECT COUNT(*) FROM a2amq_actions WHERE delivered_at IS NULL").fetchone()[0]
        duplicate_ends = conn.execute(
            "SELECT task_id, COUNT(*) c FROM a2amq_runtime_end GROUP BY task_id HAVING c > 1"
        ).fetchall()
        clean = (
            integ == ["ok"]
            and quick == ["ok"]
            and fk_errs == 0
            and len(duplicate_ends) == 0
            and pending_actions == 0
        )
        return {
            "clean": clean,
            "tasksCount": tasks,
            "runtimeEndsCount": ends,
            "pendingActionsCount": pending_actions,
            "duplicateEnds": [r[0] for r in duplicate_ends],
        }


class ManagedWorkflowStack:
    """Deterministic lifecycle controller for the five-process native Workflow stack."""

    def __init__(self, profile_dir: Path, tools_bin: Path, env_overrides: dict[str, str] | None = None):
        self.profile_dir = profile_dir.resolve()
        self.tools_bin = tools_bin.resolve()
        self.env_overrides = env_overrides or {}
        self.proc_nats: subprocess.Popen | None = None
        self.proc_placement: subprocess.Popen | None = None
        self.proc_scheduler: subprocess.Popen | None = None
        self.proc_daprd: subprocess.Popen | None = None
        self.proc_app: subprocess.Popen | None = None
        self.profile: dict[str, Any] = {}
        self.app_env: dict[str, str] = {}
        self.sidecar_env: dict[str, str] = {}
        self.child_env: dict[str, str] = {}
        self.log_file: Any = None

    def _close_log(self) -> None:
        if self.log_file is not None and not getattr(self.log_file, "closed", False):
            try:
                self.log_file.flush()
                self.log_file.close()
            except Exception:
                pass
        self.log_file = None

    def _wait_port(self, port: int, timeout: float = 15.0) -> None:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            probe = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            probe.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            try:
                probe.connect(("127.0.0.1", port))
                probe.close()
                return
            except OSError:
                probe.close()
                time.sleep(0.1)
        raise TimeoutError(f"Port {port} failed to become available within {timeout}s")

    def _wait_port_free(self, port: int, timeout: float = 10.0) -> None:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            probe = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            probe.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            try:
                probe.bind(("127.0.0.1", port))
                probe.close()
                return
            except OSError:
                probe.close()
                time.sleep(0.1)
        raise TimeoutError(f"Port {port} failed to become free within {timeout}s")

    def start(self, timeout: float = 30.0) -> None:
        self.stop()
        for port in (18000, 3500, 50001, 50002, 50005, 50006, 4222):
            self._wait_port_free(port)

        self.profile = _load_profile(self.profile_dir, "local-workflow")
        self.app_env, self.sidecar_env, self.child_env = _runtime_environments(self.profile_dir, self.profile)
        env_base = os.environ.copy()
        env_base["PATH"] = str(self.tools_bin) + os.pathsep + env_base.get("PATH", "")
        env_base.update(self.env_overrides)

        log_path = self.profile_dir / "workflow_stack.log"
        self.log_file = log_path.open("a", encoding="utf-8")

        # 1. NATS
        self.proc_nats = subprocess.Popen(
            [str(self.tools_bin / "nats-server"), "-c", str(self.profile_dir / "nats.conf")],
            cwd=self.profile_dir, env={**env_base, **self.child_env},
            stdout=self.log_file, stderr=subprocess.STDOUT
        )
        self._wait_port(4222)

        # 2. Provision NATS stream
        subprocess.run([
            sys.executable, "-m", "a2a_mq.provision",
            "--nats-url", "nats://127.0.0.1:4222",
            "--password-file", str(self.profile_dir / "nats-token"),
            "--plan", str(self.profile_dir / "broker-plan.json"),
            "--development",
        ], cwd=self.profile_dir, env={**env_base, **self.child_env}, check=True, stdout=subprocess.DEVNULL)

        # 3. Placement
        self.proc_placement = subprocess.Popen(
            _placement_command(str(self.tools_bin / "placement")),
            cwd=self.profile_dir, env={**env_base, **self.child_env},
            stdout=self.log_file, stderr=subprocess.STDOUT
        )
        self._wait_port(PLACEMENT_PORT)

        # 4. Scheduler
        self.proc_scheduler = subprocess.Popen(
            _scheduler_command(str(self.tools_bin / "scheduler"), self.profile_dir),
            cwd=self.profile_dir, env={**env_base, **self.child_env},
            stdout=self.log_file, stderr=subprocess.STDOUT
        )
        self._wait_port(SCHEDULER_PORT)

        # 5. Daprd
        sidecar_command = [
            str(self.tools_bin / "daprd"), "--mode", "standalone", "--app-id", APP_ID,
            "--app-port", "18000", "--app-protocol", "http",
            "--dapr-http-port", "3500", "--dapr-grpc-port", "50001",
            "--dapr-internal-grpc-port", str(DAPR_INTERNAL_GRPC_PORT),
            "--dapr-internal-grpc-listen-address", "127.0.0.1",
            "--resources-path", str((self.profile_dir / "components").resolve()),
            "--dapr-listen-addresses", "127.0.0.1",
            "--placement-host-address", f"127.0.0.1:{PLACEMENT_PORT}",
            "--scheduler-host-address", f"127.0.0.1:{SCHEDULER_PORT}",
            "--app-max-concurrency", "2", "--max-body-size", "1Mi",
            "--enable-metrics=false", "--log-level", "warn",
        ]
        self.proc_daprd = subprocess.Popen(
            sidecar_command, cwd=self.profile_dir, env={**env_base, **self.sidecar_env},
            stdout=self.log_file, stderr=subprocess.STDOUT
        )
        self._wait_port(3500)

        # 6. App
        self.start_app(env_base)

        # Wait for stack ready
        deadline = time.monotonic() + timeout
        ready = False
        while time.monotonic() < deadline:
            try:
                r = httpx.get("http://127.0.0.1:18000/health/ready", timeout=0.5)
                if r.status_code == 200 and r.json().get("status") == "ready":
                    ready = True
                    break
            except Exception:
                pass
            time.sleep(0.2)
        if not ready:
            self.stop()
            raise RuntimeError(f"Workflow stack failed to become ready within {timeout}s")

    def start_app(self, env_base: dict[str, str] | None = None) -> None:
        if env_base is None:
            env_base = os.environ.copy()
            env_base["PATH"] = str(self.tools_bin) + os.pathsep + env_base.get("PATH", "")
            env_base.update(self.env_overrides)
        app_full_env = {**env_base, **self.app_env}
        app_cmd = [
            sys.executable, "-m", "uvicorn", "a2a_mq.local_workflow:from_env",
            "--factory", "--host", "127.0.0.1", "--port", "18000", "--log-level", "warning"
        ]
        self.proc_app = subprocess.Popen(
            app_cmd, cwd=self.profile_dir, env=app_full_env,
            stdout=self.log_file, stderr=subprocess.STDOUT
        )

    def restart_app(self, timeout: float = 20.0) -> None:
        if self.proc_app and self.proc_app.poll() is None:
            self.proc_app.terminate()
            try:
                self.proc_app.wait(timeout=2.0)
            except Exception:
                self.proc_app.kill()
                self.proc_app.wait()
        self.proc_app = None
        self._wait_port_free(18000)
        self.start_app()
        deadline = time.monotonic() + timeout
        ready = False
        while time.monotonic() < deadline:
            try:
                r = httpx.get("http://127.0.0.1:18000/health/ready", timeout=0.5)
                if r.status_code == 200 and r.json().get("status") == "ready":
                    ready = True
                    break
            except Exception:
                pass
            time.sleep(0.2)
        require(ready, "APP_RESTART_FAILED_READINESS")

    def restart_daprd(self, timeout: float = 20.0) -> None:
        if self.proc_daprd and self.proc_daprd.poll() is None:
            self.proc_daprd.terminate()
            try:
                self.proc_daprd.wait(timeout=3.0)
            except Exception:
                self.proc_daprd.kill()
                self.proc_daprd.wait()
        self.proc_daprd = None
        self._wait_port_free(3500)
        self._wait_port_free(50001)

        env_base = os.environ.copy()
        env_base["PATH"] = str(self.tools_bin) + os.pathsep + env_base.get("PATH", "")
        sidecar_command = [
            str(self.tools_bin / "daprd"), "--mode", "standalone", "--app-id", APP_ID,
            "--app-port", "18000", "--app-protocol", "http",
            "--dapr-http-port", "3500", "--dapr-grpc-port", "50001",
            "--dapr-internal-grpc-port", str(DAPR_INTERNAL_GRPC_PORT),
            "--dapr-internal-grpc-listen-address", "127.0.0.1",
            "--resources-path", str((self.profile_dir / "components").resolve()),
            "--dapr-listen-addresses", "127.0.0.1",
            "--placement-host-address", f"127.0.0.1:{PLACEMENT_PORT}",
            "--scheduler-host-address", f"127.0.0.1:{SCHEDULER_PORT}",
            "--app-max-concurrency", "2", "--max-body-size", "1Mi",
            "--enable-metrics=false", "--log-level", "warn",
        ]
        self.proc_daprd = subprocess.Popen(
            sidecar_command, cwd=self.profile_dir, env={**env_base, **self.sidecar_env},
            stdout=self.log_file, stderr=subprocess.STDOUT
        )
        self._wait_port(3500)
        # Verify app ready
        deadline = time.monotonic() + timeout
        ready = False
        while time.monotonic() < deadline:
            try:
                r = httpx.get("http://127.0.0.1:18000/health/ready", timeout=0.5)
                if r.status_code == 200 and r.json().get("status") == "ready":
                    ready = True
                    break
            except Exception:
                pass
            time.sleep(0.2)
        require(ready, "DAPRD_RESTART_FAILED_READINESS")

    def restart_placement(self, timeout: float = 20.0) -> None:
        if self.proc_placement and self.proc_placement.poll() is None:
            self.proc_placement.terminate()
            try:
                self.proc_placement.wait(timeout=2.0)
            except Exception:
                self.proc_placement.kill()
                self.proc_placement.wait()
        self.proc_placement = None
        self._wait_port_free(PLACEMENT_PORT)

        env_base = os.environ.copy()
        env_base["PATH"] = str(self.tools_bin) + os.pathsep + env_base.get("PATH", "")
        self.proc_placement = subprocess.Popen(
            _placement_command(str(self.tools_bin / "placement")),
            cwd=self.profile_dir, env={**env_base, **self.child_env},
            stdout=self.log_file, stderr=subprocess.STDOUT
        )
        self._wait_port(PLACEMENT_PORT)
        time.sleep(0.5)

    def restart_scheduler(self, timeout: float = 20.0) -> None:
        if self.proc_scheduler and self.proc_scheduler.poll() is None:
            self.proc_scheduler.terminate()
            try:
                self.proc_scheduler.wait(timeout=3.0)
            except Exception:
                self.proc_scheduler.kill()
                self.proc_scheduler.wait()
        self.proc_scheduler = None
        self._wait_port_free(SCHEDULER_PORT)

        env_base = os.environ.copy()
        env_base["PATH"] = str(self.tools_bin) + os.pathsep + env_base.get("PATH", "")
        self.proc_scheduler = subprocess.Popen(
            _scheduler_command(str(self.tools_bin / "scheduler"), self.profile_dir),
            cwd=self.profile_dir, env={**env_base, **self.child_env},
            stdout=self.log_file, stderr=subprocess.STDOUT
        )
        self._wait_port(SCHEDULER_PORT)
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            try:
                r = httpx.get("http://127.0.0.1:58081/healthz", timeout=0.5)
                if r.status_code == 200:
                    break
            except Exception:
                pass
            time.sleep(0.2)
        time.sleep(1.0)

    def stop(self) -> None:
        for p in (self.proc_app, self.proc_daprd, self.proc_scheduler, self.proc_placement, self.proc_nats):
            if p is not None and p.poll() is None:
                p.terminate()
                try:
                    p.wait(timeout=2)
                except Exception:
                    p.kill()
        self.proc_app = self.proc_daprd = self.proc_scheduler = self.proc_placement = self.proc_nats = None
        self._close_log()
        time.sleep(0.3)

    def client_jwt(self, scope: str = "a2a:send a2a:read a2a:cancel a2a:operate a2a:replay a2a:approve",
                   subject: str = "local-owner", tenant: str = "demo", expires_in: int = 3600) -> str:
        pem_bytes = (self.profile_dir / "jwt-private.pem").read_bytes()
        key: Any = serialization.load_pem_private_key(pem_bytes, password=None)
        profile = json.loads((self.profile_dir / "profile.json").read_text())
        now = int(time.time())
        claims = {
            "iss": profile["issuer"],
            "sub": subject,
            "tenant": tenant,
            "scope": scope,
            "iat": now,
            "nbf": now,
            "exp": now + expires_in,
            "aud": profile["publicUrl"],
        }
        return jwt.encode(claims, key, algorithm="RS256", headers={"kid": "local", "typ": "JWT"})


async def run_workflow_functional(stack: ManagedWorkflowStack, profile_dir: Path) -> dict[str, Any]:
    """Execute all official workflow scenarios through real public A2A interfaces."""
    headers = {"Authorization": f"Bearer {stack.client_jwt()}", "A2A-Version": "1.0"}
    db_path = profile_dir / "state/a2amq.db"
    scenarios: dict[str, Any] = {}

    # 1. Basic Workflow
    req_basic = {
        "message": {"messageId": "wf-fn-basic", "role": "ROLE_USER", "parts": [{"text": "basic workflow text"}]},
        "configuration": {"returnImmediately": True},
        "metadata": {"a2amq": {"transform": "upper"}},
    }
    r = httpx.post("http://127.0.0.1:18000/rpc", json={"jsonrpc": "2.0", "id": 1, "method": "SendMessage", "params": req_basic}, headers=headers, timeout=5)
    require(r.status_code == 200, "SUBMIT_BASIC_FAILED")
    tid_basic = r.json()["result"]["task"]["id"]

    final_basic = None
    for _ in range(60):
        r = httpx.post("http://127.0.0.1:18000/rpc", json={"jsonrpc": "2.0", "id": 2, "method": "GetTask", "params": {"id": tid_basic}}, headers=headers, timeout=5)
        if r.status_code == 200 and r.json().get("result", {}).get("status", {}).get("state") == "TASK_STATE_COMPLETED":
            final_basic = r.json()["result"]
            break
        await asyncio.sleep(0.2)
    require(final_basic is not None, "BASIC_WORKFLOW_FAILED_COMPLETION")
    assert final_basic is not None
    require(final_basic["artifacts"][0]["parts"][0]["text"] == "BASIC WORKFLOW TEXT", "BASIC_WORKFLOW_TRANSFORM_FAILED")
    scenarios["basicWorkflow"] = {"verdict": "PASS", "taskId": tid_basic, "state": "TASK_STATE_COMPLETED"}

    # 2. Durable Timer
    t0 = time.monotonic()
    req_timer = {
        "message": {"messageId": "wf-fn-timer", "role": "ROLE_USER", "parts": [{"text": "timer wait text"}]},
        "configuration": {"returnImmediately": True},
        "metadata": {"a2amq": {"delaySeconds": 2, "transform": "upper"}},
    }
    r = httpx.post("http://127.0.0.1:18000/rpc", json={"jsonrpc": "2.0", "id": 3, "method": "SendMessage", "params": req_timer}, headers=headers, timeout=5)
    require(r.status_code == 200, "SUBMIT_TIMER_FAILED")
    tid_timer = r.json()["result"]["task"]["id"]

    final_timer = None
    for _ in range(80):
        r = httpx.post("http://127.0.0.1:18000/rpc", json={"jsonrpc": "2.0", "id": 4, "method": "GetTask", "params": {"id": tid_timer}}, headers=headers, timeout=5)
        if r.status_code == 200 and r.json().get("result", {}).get("status", {}).get("state") == "TASK_STATE_COMPLETED":
            final_timer = r.json()["result"]
            break
        await asyncio.sleep(0.2)
    elapsed_timer = time.monotonic() - t0
    require(final_timer is not None and elapsed_timer >= 2.0, "DURABLE_TIMER_FAILED_COMPLETION")
    assert final_timer is not None
    scenarios["durableTimer"] = {"verdict": "PASS", "taskId": tid_timer, "elapsedSeconds": round(elapsed_timer, 2)}

    # 3. Input Required + Continuation
    req_input = {
        "message": {"messageId": "wf-fn-input", "role": "ROLE_USER", "parts": [{"text": "initial part"}]},
        "configuration": {"returnImmediately": True},
        "metadata": {"a2amq": {"inputRequired": True, "transform": "upper"}},
    }
    r = httpx.post("http://127.0.0.1:18000/rpc", json={"jsonrpc": "2.0", "id": 5, "method": "SendMessage", "params": req_input}, headers=headers, timeout=5)
    require(r.status_code == 200, "SUBMIT_INPUT_REQ_FAILED")
    tid_input = r.json()["result"]["task"]["id"]

    input_waiting = False
    for _ in range(60):
        r = httpx.post("http://127.0.0.1:18000/rpc", json={"jsonrpc": "2.0", "id": 6, "method": "GetTask", "params": {"id": tid_input}}, headers=headers, timeout=5)
        if r.status_code == 200 and r.json().get("result", {}).get("status", {}).get("state") == "TASK_STATE_INPUT_REQUIRED":
            input_waiting = True
            break
        await asyncio.sleep(0.2)
    require(input_waiting, "TASK_DID_NOT_ENTER_INPUT_REQUIRED")

    # Send continuation input
    cont_req = {
        "message": {"messageId": "wf-fn-input-cont", "role": "ROLE_USER", "taskId": tid_input, "parts": [{"text": "continued part"}]},
        "configuration": {"returnImmediately": True},
    }
    r_cont = httpx.post("http://127.0.0.1:18000/rpc", json={"jsonrpc": "2.0", "id": 7, "method": "SendMessage", "params": cont_req}, headers=headers, timeout=5)
    require(r_cont.status_code == 200, "SUBMIT_CONTINUATION_FAILED")

    final_input = None
    for _ in range(60):
        r = httpx.post("http://127.0.0.1:18000/rpc", json={"jsonrpc": "2.0", "id": 8, "method": "GetTask", "params": {"id": tid_input}}, headers=headers, timeout=5)
        if r.status_code == 200 and r.json().get("result", {}).get("status", {}).get("state") == "TASK_STATE_COMPLETED":
            final_input = r.json()["result"]
            break
        await asyncio.sleep(0.2)
    require(final_input is not None, "INPUT_CONTINUATION_FAILED_COMPLETION")
    assert final_input is not None
    require(final_input["artifacts"][0]["parts"][0]["text"] == "CONTINUED PART", "CONTINUATION_TEXT_TRANSFORM_FAILED")
    require(len(final_input.get("history", [])) == 2, "CONTINUATION_HISTORY_LENGTH_MISMATCH")
    scenarios["inputRequiredContinuation"] = {"verdict": "PASS", "taskId": tid_input}

    # 4. Auth Required + Approve
    req_auth_appr = {
        "message": {"messageId": "wf-fn-auth-appr", "role": "ROLE_USER", "parts": [{"text": "auth approve text"}]},
        "configuration": {"returnImmediately": True},
        "metadata": {"a2amq": {"authRequired": True, "transform": "upper"}},
    }
    r = httpx.post("http://127.0.0.1:18000/rpc", json={"jsonrpc": "2.0", "id": 9, "method": "SendMessage", "params": req_auth_appr}, headers=headers, timeout=5)
    tid_auth_appr = r.json()["result"]["task"]["id"]

    auth_waiting = False
    for _ in range(60):
        r = httpx.post("http://127.0.0.1:18000/rpc", json={"jsonrpc": "2.0", "id": 10, "method": "GetTask", "params": {"id": tid_auth_appr}}, headers=headers, timeout=5)
        if r.status_code == 200 and r.json().get("result", {}).get("status", {}).get("state") == "TASK_STATE_AUTH_REQUIRED":
            auth_waiting = True
            break
        await asyncio.sleep(0.2)
    require(auth_waiting, "TASK_DID_NOT_ENTER_AUTH_REQUIRED")

    # Send Approval: POST /operations/tasks/{task_id}/approve
    r_appr = httpx.post(f"http://127.0.0.1:18000/operations/tasks/{tid_auth_appr}/approve",
                        json={"requestId": "appr-req-1", "approved": True}, headers=headers, timeout=5)
    require(r_appr.status_code in {200, 202}, f"APPROVAL_POST_FAILED: {r_appr.status_code} {r_appr.text}")

    final_auth_appr = None
    for _ in range(60):
        r = httpx.post("http://127.0.0.1:18000/rpc", json={"jsonrpc": "2.0", "id": 11, "method": "GetTask", "params": {"id": tid_auth_appr}}, headers=headers, timeout=5)
        if r.status_code == 200 and r.json().get("result", {}).get("status", {}).get("state") == "TASK_STATE_COMPLETED":
            final_auth_appr = r.json()["result"]
            break
        await asyncio.sleep(0.2)
    require(final_auth_appr is not None, "AUTH_APPROVE_FAILED_COMPLETION")
    assert final_auth_appr is not None
    scenarios["authRequiredApprove"] = {"verdict": "PASS", "taskId": tid_auth_appr}

    # 5. Auth Required + Deny
    req_auth_deny = {
        "message": {"messageId": "wf-fn-auth-deny", "role": "ROLE_USER", "parts": [{"text": "auth deny text"}]},
        "configuration": {"returnImmediately": True},
        "metadata": {"a2amq": {"authRequired": True, "transform": "upper"}},
    }
    r = httpx.post("http://127.0.0.1:18000/rpc", json={"jsonrpc": "2.0", "id": 12, "method": "SendMessage", "params": req_auth_deny}, headers=headers, timeout=5)
    tid_auth_deny = r.json()["result"]["task"]["id"]

    for _ in range(60):
        r = httpx.post("http://127.0.0.1:18000/rpc", json={"jsonrpc": "2.0", "id": 13, "method": "GetTask", "params": {"id": tid_auth_deny}}, headers=headers, timeout=5)
        if r.status_code == 200 and r.json().get("result", {}).get("status", {}).get("state") == "TASK_STATE_AUTH_REQUIRED":
            break
        await asyncio.sleep(0.2)

    r_deny = httpx.post(f"http://127.0.0.1:18000/operations/tasks/{tid_auth_deny}/approve",
                        json={"requestId": "appr-req-2", "approved": False}, headers=headers, timeout=5)
    require(r_deny.status_code in {200, 202}, f"DENIAL_POST_FAILED: {r_deny.status_code} {r_deny.text}")

    final_auth_deny = None
    for _ in range(60):
        r = httpx.post("http://127.0.0.1:18000/rpc", json={"jsonrpc": "2.0", "id": 14, "method": "GetTask", "params": {"id": tid_auth_deny}}, headers=headers, timeout=5)
        if r.status_code == 200 and r.json().get("result", {}).get("status", {}).get("state") == "TASK_STATE_REJECTED":
            final_auth_deny = r.json()["result"]
            break
        await asyncio.sleep(0.2)
    require(final_auth_deny is not None, "AUTH_DENIAL_FAILED_REJECTION")
    assert final_auth_deny is not None
    scenarios["authRequiredDeny"] = {"verdict": "PASS", "taskId": tid_auth_deny}

    # 6. Cancellation
    req_cancel = {
        "message": {"messageId": "wf-fn-cancel", "role": "ROLE_USER", "parts": [{"text": "cancel test text"}]},
        "configuration": {"returnImmediately": True},
        "metadata": {"a2amq": {"delaySeconds": 5, "transform": "upper"}},
    }
    r = httpx.post("http://127.0.0.1:18000/rpc", json={"jsonrpc": "2.0", "id": 15, "method": "SendMessage", "params": req_cancel}, headers=headers, timeout=5)
    tid_cancel = r.json()["result"]["task"]["id"]

    for _ in range(60):
        r = httpx.post("http://127.0.0.1:18000/rpc", json={"jsonrpc": "2.0", "id": 16, "method": "GetTask", "params": {"id": tid_cancel}}, headers=headers, timeout=5)
        if r.status_code == 200 and r.json().get("result", {}).get("status", {}).get("state") == "TASK_STATE_WORKING":
            break
        await asyncio.sleep(0.1)

    r_canc = httpx.post("http://127.0.0.1:18000/rpc", json={"jsonrpc": "2.0", "id": 17, "method": "CancelTask", "params": {"id": tid_cancel}}, headers=headers, timeout=5)
    require(r_canc.status_code == 200, "CANCEL_RPC_FAILED")

    final_cancel = None
    for _ in range(60):
        r = httpx.post("http://127.0.0.1:18000/rpc", json={"jsonrpc": "2.0", "id": 18, "method": "GetTask", "params": {"id": tid_cancel}}, headers=headers, timeout=5)
        if r.status_code == 200 and r.json().get("result", {}).get("status", {}).get("state") == "TASK_STATE_CANCELED":
            final_cancel = r.json()["result"]
            break
        await asyncio.sleep(0.2)
    require(final_cancel is not None, "WORKFLOW_CANCELLATION_FAILED")
    assert final_cancel is not None
    scenarios["cancellation"] = {"verdict": "PASS", "taskId": tid_cancel}

    # 7. Fan-out / Fan-in
    parts_input = [{"text": f"item {i}"} for i in range(4)]
    req_fanout = {
        "message": {"messageId": "wf-fn-fanout", "role": "ROLE_USER", "parts": parts_input},
        "configuration": {"returnImmediately": True},
        "metadata": {"a2amq": {"transform": "upper"}},
    }
    r = httpx.post("http://127.0.0.1:18000/rpc", json={"jsonrpc": "2.0", "id": 19, "method": "SendMessage", "params": req_fanout}, headers=headers, timeout=5)
    tid_fanout = r.json()["result"]["task"]["id"]

    final_fanout = None
    for _ in range(60):
        r = httpx.post("http://127.0.0.1:18000/rpc", json={"jsonrpc": "2.0", "id": 20, "method": "GetTask", "params": {"id": tid_fanout}}, headers=headers, timeout=5)
        if r.status_code == 200 and r.json().get("result", {}).get("status", {}).get("state") == "TASK_STATE_COMPLETED":
            final_fanout = r.json()["result"]
            break
        await asyncio.sleep(0.2)
    require(final_fanout is not None, "FANOUT_WORKFLOW_FAILED_COMPLETION")
    assert final_fanout is not None
    transformed_texts = [p["text"] for p in final_fanout["artifacts"][0]["parts"]]
    require(transformed_texts == ["ITEM 0", "ITEM 1", "ITEM 2", "ITEM 3"], f"FANOUT_RESULT_MISMATCH: {transformed_texts}")
    scenarios["fanOutFanIn"] = {"verdict": "PASS", "taskId": tid_fanout, "partsCount": 4}

    # Verify SQLite integrity
    check = _check_sqlite_integrity(db_path)
    require(check["clean"], "SQLITE_INTEGRITY_COMPROMISED_AFTER_FUNCTIONAL")
    scenarios["sqliteIntegrity"] = check
    scenarios["verdict"] = "PASS_LOCAL_WORKFLOW_FUNCTIONAL"
    return scenarios


async def run_workflow_resilience(stack: ManagedWorkflowStack, profile_dir: Path) -> dict[str, Any]:
    """Execute resilience restart scenarios across the five-process workflow topology."""
    headers = {"Authorization": f"Bearer {stack.client_jwt()}", "A2A-Version": "1.0"}
    db_path = profile_dir / "state/a2amq.db"
    scenarios: dict[str, Any] = {}

    # 1. App restart during workflow
    req_r1 = {
        "message": {"messageId": "wf-res-app-restart", "role": "ROLE_USER", "parts": [{"text": "app restart text"}]},
        "configuration": {"returnImmediately": True},
        "metadata": {"a2amq": {"delaySeconds": 4, "transform": "upper"}},
    }
    r = httpx.post("http://127.0.0.1:18000/rpc", json={"jsonrpc": "2.0", "id": 101, "method": "SendMessage", "params": req_r1}, headers=headers, timeout=5)
    tid_r1 = r.json()["result"]["task"]["id"]

    for _ in range(50):
        r = httpx.post("http://127.0.0.1:18000/rpc", json={"jsonrpc": "2.0", "id": 102, "method": "GetTask", "params": {"id": tid_r1}}, headers=headers, timeout=5)
        if r.status_code == 200 and r.json().get("result", {}).get("status", {}).get("state") == "TASK_STATE_WORKING":
            break
        await asyncio.sleep(0.1)

    # Kill and restart app
    stack.restart_app()

    final_r1 = None
    for _ in range(80):
        try:
            r = httpx.post("http://127.0.0.1:18000/rpc", json={"jsonrpc": "2.0", "id": 103, "method": "GetTask", "params": {"id": tid_r1}}, headers=headers, timeout=5)
            if r.status_code == 200 and r.json().get("result", {}).get("status", {}).get("state") == "TASK_STATE_COMPLETED":
                final_r1 = r.json()["result"]
                break
        except Exception:
            pass
        await asyncio.sleep(0.2)
    require(final_r1 is not None, "TASK_FAILED_AFTER_APP_RESTART")
    assert final_r1 is not None
    require(final_r1["artifacts"][0]["parts"][0]["text"] == "APP RESTART TEXT", "TRANSFORM_CORRUPTED_R1")
    scenarios["appRestartDuringWorkflow"] = {"verdict": "PASS", "taskId": tid_r1}

    # 2. Daprd restart during workflow
    req_r2 = {
        "message": {"messageId": "wf-res-daprd-restart", "role": "ROLE_USER", "parts": [{"text": "daprd restart text"}]},
        "configuration": {"returnImmediately": True},
        "metadata": {"a2amq": {"delaySeconds": 4, "transform": "upper"}},
    }
    r = httpx.post("http://127.0.0.1:18000/rpc", json={"jsonrpc": "2.0", "id": 104, "method": "SendMessage", "params": req_r2}, headers=headers, timeout=5)
    tid_r2 = r.json()["result"]["task"]["id"]

    for _ in range(50):
        r = httpx.post("http://127.0.0.1:18000/rpc", json={"jsonrpc": "2.0", "id": 105, "method": "GetTask", "params": {"id": tid_r2}}, headers=headers, timeout=5)
        if r.status_code == 200 and r.json().get("result", {}).get("status", {}).get("state") == "TASK_STATE_WORKING":
            break
        await asyncio.sleep(0.1)

    # Kill and restart daprd sidecar
    stack.restart_daprd()

    final_r2 = None
    for _ in range(80):
        try:
            r = httpx.post("http://127.0.0.1:18000/rpc", json={"jsonrpc": "2.0", "id": 106, "method": "GetTask", "params": {"id": tid_r2}}, headers=headers, timeout=5)
            if r.status_code == 200 and r.json().get("result", {}).get("status", {}).get("state") == "TASK_STATE_COMPLETED":
                final_r2 = r.json()["result"]
                break
        except Exception:
            pass
        await asyncio.sleep(0.2)
    require(final_r2 is not None, "TASK_FAILED_AFTER_DAPRD_RESTART")
    assert final_r2 is not None
    require(final_r2["artifacts"][0]["parts"][0]["text"] == "DAPRD RESTART TEXT", "TRANSFORM_CORRUPTED_R2")
    scenarios["daprdRestartDuringWorkflow"] = {"verdict": "PASS", "taskId": tid_r2}

    # 3. Placement restart
    req_r3 = {
        "message": {"messageId": "wf-res-placement-restart", "role": "ROLE_USER", "parts": [{"text": "placement restart text"}]},
        "configuration": {"returnImmediately": True},
        "metadata": {"a2amq": {"delaySeconds": 4, "transform": "upper"}},
    }
    r = httpx.post("http://127.0.0.1:18000/rpc", json={"jsonrpc": "2.0", "id": 107, "method": "SendMessage", "params": req_r3}, headers=headers, timeout=5)
    tid_r3 = r.json()["result"]["task"]["id"]

    for _ in range(50):
        r = httpx.post("http://127.0.0.1:18000/rpc", json={"jsonrpc": "2.0", "id": 108, "method": "GetTask", "params": {"id": tid_r3}}, headers=headers, timeout=5)
        if r.status_code == 200 and r.json().get("result", {}).get("status", {}).get("state") == "TASK_STATE_WORKING":
            break
        await asyncio.sleep(0.1)

    # Kill and restart placement
    stack.restart_placement()

    final_r3 = None
    for _ in range(80):
        try:
            r = httpx.post("http://127.0.0.1:18000/rpc", json={"jsonrpc": "2.0", "id": 109, "method": "GetTask", "params": {"id": tid_r3}}, headers=headers, timeout=5)
            if r.status_code == 200 and r.json().get("result", {}).get("status", {}).get("state") == "TASK_STATE_COMPLETED":
                final_r3 = r.json()["result"]
                break
        except Exception:
            pass
        await asyncio.sleep(0.2)
    require(final_r3 is not None, "TASK_FAILED_AFTER_PLACEMENT_RESTART")
    assert final_r3 is not None
    require(final_r3["artifacts"][0]["parts"][0]["text"] == "PLACEMENT RESTART TEXT", "TRANSFORM_CORRUPTED_R3")
    scenarios["placementRestart"] = {"verdict": "PASS", "taskId": tid_r3}

    # 4. Scheduler restart while durable timer exists
    req_r4 = {
        "message": {"messageId": "wf-res-scheduler-restart", "role": "ROLE_USER", "parts": [{"text": "scheduler restart text"}]},
        "configuration": {"returnImmediately": True},
        "metadata": {"a2amq": {"delaySeconds": 5, "transform": "upper"}},
    }
    r = httpx.post("http://127.0.0.1:18000/rpc", json={"jsonrpc": "2.0", "id": 110, "method": "SendMessage", "params": req_r4}, headers=headers, timeout=5)
    tid_r4 = r.json()["result"]["task"]["id"]

    for _ in range(50):
        r = httpx.post("http://127.0.0.1:18000/rpc", json={"jsonrpc": "2.0", "id": 111, "method": "GetTask", "params": {"id": tid_r4}}, headers=headers, timeout=5)
        if r.status_code == 200 and r.json().get("result", {}).get("status", {}).get("state") == "TASK_STATE_WORKING":
            break
        await asyncio.sleep(0.1)

    # Kill and restart scheduler
    stack.restart_scheduler()

    final_r4 = None
    for _ in range(120):
        try:
            r = httpx.post("http://127.0.0.1:18000/rpc", json={"jsonrpc": "2.0", "id": 112, "method": "GetTask", "params": {"id": tid_r4}}, headers=headers, timeout=5)
            if r.status_code == 200 and r.json().get("result", {}).get("status", {}).get("state") == "TASK_STATE_COMPLETED":
                final_r4 = r.json()["result"]
                break
        except Exception:
            pass
        await asyncio.sleep(0.25)
    require(final_r4 is not None, "TASK_FAILED_AFTER_SCHEDULER_RESTART")
    assert final_r4 is not None
    require(final_r4["artifacts"][0]["parts"][0]["text"] == "SCHEDULER RESTART TEXT", "TRANSFORM_CORRUPTED_R4")
    scenarios["schedulerRestartDuringTimer"] = {"verdict": "PASS", "taskId": tid_r4}

    # 5. Sequential app -> daprd -> Placement -> Scheduler restart on one long-lived workflow
    req_r5 = {
        "message": {"messageId": "wf-res-seq-restart", "role": "ROLE_USER", "parts": [{"text": "sequential restart text"}]},
        "configuration": {"returnImmediately": True},
        "metadata": {"a2amq": {"inputRequired": True, "transform": "upper"}},
    }
    r = httpx.post("http://127.0.0.1:18000/rpc", json={"jsonrpc": "2.0", "id": 113, "method": "SendMessage", "params": req_r5}, headers=headers, timeout=5)
    tid_r5 = r.json()["result"]["task"]["id"]

    for _ in range(50):
        r = httpx.post("http://127.0.0.1:18000/rpc", json={"jsonrpc": "2.0", "id": 114, "method": "GetTask", "params": {"id": tid_r5}}, headers=headers, timeout=5)
        if r.status_code == 200 and r.json().get("result", {}).get("status", {}).get("state") == "TASK_STATE_INPUT_REQUIRED":
            break
        await asyncio.sleep(0.1)

    # Step 1: Restart app
    stack.restart_app()
    # Step 2: Restart daprd
    stack.restart_daprd()
    # Step 3: Restart placement
    stack.restart_placement()
    # Step 4: Restart scheduler
    stack.restart_scheduler()

    # Re-verify task is still waiting in INPUT_REQUIRED state after all 4 component restarts
    r_check = httpx.post("http://127.0.0.1:18000/rpc", json={"jsonrpc": "2.0", "id": 115, "method": "GetTask", "params": {"id": tid_r5}}, headers=headers, timeout=5)
    require(r_check.status_code == 200 and r_check.json().get("result", {}).get("status", {}).get("state") == "TASK_STATE_INPUT_REQUIRED",
            "TASK_LOST_INPUT_REQUIRED_STATE_AFTER_SEQUENTIAL_RESTARTS")

    # Send continuation input to resume workflow
    cont_r5 = {
        "message": {"messageId": "wf-res-seq-cont", "role": "ROLE_USER", "taskId": tid_r5, "parts": [{"text": "sequential resumed"}]},
        "configuration": {"returnImmediately": True},
    }
    r_cont = httpx.post("http://127.0.0.1:18000/rpc", json={"jsonrpc": "2.0", "id": 116, "method": "SendMessage", "params": cont_r5}, headers=headers, timeout=5)
    require(r_cont.status_code == 200, "CONTINUATION_FAILED_AFTER_SEQUENTIAL_RESTARTS")

    final_r5 = None
    for _ in range(120):
        try:
            r = httpx.post("http://127.0.0.1:18000/rpc", json={"jsonrpc": "2.0", "id": 117, "method": "GetTask", "params": {"id": tid_r5}}, headers=headers, timeout=5)
            if r.status_code == 200 and r.json().get("result", {}).get("status", {}).get("state") == "TASK_STATE_COMPLETED":
                final_r5 = r.json()["result"]
                break
        except Exception:
            pass
        await asyncio.sleep(0.25)
    require(final_r5 is not None, "SEQUENTIAL_RESTARTS_FAILED_COMPLETION")
    assert final_r5 is not None
    require(final_r5["artifacts"][0]["parts"][0]["text"] == "SEQUENTIAL RESUMED", "TRANSFORM_CORRUPTED_R5")
    scenarios["sequentialAllRestarts"] = {"verdict": "PASS", "taskId": tid_r5}

    # Verify SQLite integrity
    check = _check_sqlite_integrity(db_path)
    require(check["clean"], "SQLITE_INTEGRITY_COMPROMISED_AFTER_RESILIENCE")
    scenarios["sqliteIntegrity"] = check
    scenarios["verdict"] = "PASS_LOCAL_WORKFLOW_RESILIENCE"
    return scenarios


async def run_workflow_gate(tools_dir: Path, native_verification: Path, work_dir: Path) -> dict[str, Any]:
    tools_bin = tools_dir / "bin"
    require(tools_bin.is_dir(), f"TOOLS_BIN_NOT_FOUND: {tools_bin}")

    work_dir = work_dir.resolve()
    profile_dir = work_dir / "profile-workflow"
    if profile_dir.exists():
        shutil.rmtree(profile_dir)

    prepare_workflow(profile_dir, tenant="demo", source="local-worker", public_url="http://127.0.0.1:18000")

    # Fast redelivery for workflow
    bus_yaml = profile_dir / "components/bus.yaml"
    bus_yaml.write_text(bus_yaml.read_text().replace("ackWait\n      value: 60s", "ackWait\n      value: 4s"))

    results: dict[str, Any] = {}
    suite_start = time.monotonic()

    # 1. Resource Gate (probe)
    print("[1/3] Running Workflow Resource Gate Probe...")
    probe_script = Path(__file__).resolve().parent / "local_stack_probe.py"
    probe_json = work_dir / "workflow_resource_probe.json"
    probe_log = work_dir / "workflow_resource_probe.log"

    probe_profile_dir = work_dir / "profile-workflow-probe"
    if probe_profile_dir.exists():
        shutil.rmtree(probe_profile_dir)
    prepare_workflow(probe_profile_dir, tenant="demo", source="local-worker", public_url="http://127.0.0.1:18000")

    cmd = [
        sys.executable, str(probe_script),
        "--profile", str(probe_profile_dir),
        "--json", str(probe_json),
        "--log", str(probe_log),
        "--tools-dir", str(tools_dir),
        "--native-verification", str(native_verification),
    ]
    p = subprocess.run(cmd, capture_output=True, text=True)
    require(p.returncode == 0 and probe_json.exists(), f"WORKFLOW_RESOURCE_PROBE_FAILED: {p.stderr}")
    probe_data = json.loads(probe_json.read_text(encoding="utf-8"))
    results["resourceGate"] = {
        "verdict": "PASS_LOCAL_WORKFLOW_RESOURCE",
        "probe": probe_data,
    }

    # Start main managed stack for functional and resilience testing
    stack = ManagedWorkflowStack(profile_dir, tools_bin)
    try:
        stack.start()

        # 2. Functional Gate
        print("[2/3] Running Workflow Functional Gate (6 scenarios)...")
        results["functionalGate"] = await run_workflow_functional(stack, profile_dir)

        # 3. Resilience Gate
        print("[3/3] Running Workflow Resilience Gate (5 restart scenarios)...")
        results["resilienceGate"] = await run_workflow_resilience(stack, profile_dir)

    finally:
        stack.stop()

    resource_ok = results.get("resourceGate", {}).get("verdict") == "PASS_LOCAL_WORKFLOW_RESOURCE"
    functional_ok = results.get("functionalGate", {}).get("verdict") == "PASS_LOCAL_WORKFLOW_FUNCTIONAL"
    resilience_ok = results.get("resilienceGate", {}).get("verdict") == "PASS_LOCAL_WORKFLOW_RESILIENCE"

    overall_pass = resource_ok and functional_ok and resilience_ok
    total_elapsed = round(time.monotonic() - suite_start, 2)

    return {
        "verdict": "PASS_LOCAL_WORKFLOW" if overall_pass else "FAIL_LOCAL_WORKFLOW",
        "resourceVerdict": "PASS_LOCAL_WORKFLOW_RESOURCE" if resource_ok else "FAIL_LOCAL_WORKFLOW_RESOURCE",
        "functionalVerdict": "PASS_LOCAL_WORKFLOW_FUNCTIONAL" if functional_ok else "FAIL_LOCAL_WORKFLOW_FUNCTIONAL",
        "resilienceVerdict": "PASS_LOCAL_WORKFLOW_RESILIENCE" if resilience_ok else "FAIL_LOCAL_WORKFLOW_RESILIENCE",
        "elapsedSeconds": total_elapsed,
        "details": results,
    }


def _prepare_evidence_paths(json_path: Path, log_path: Path) -> None:
    for path in (json_path, log_path):
        require(not path.exists() and not path.is_symlink(), "EVIDENCE_OUTPUT_EXISTS")
    require(json_path.resolve() != log_path.resolve(), "EVIDENCE_PATH_CONFLICT")
    json_path.parent.mkdir(parents=True, exist_ok=True)
    log_path.parent.mkdir(parents=True, exist_ok=True)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--tools-dir", required=True, type=Path)
    parser.add_argument("--native-verification", required=True, type=Path)
    parser.add_argument("--work-dir", required=True, type=Path)
    parser.add_argument("--json", dest="json_path", required=True, type=Path)
    parser.add_argument("--log", required=True, type=Path)
    args = parser.parse_args()

    args.work_dir.mkdir(parents=True, exist_ok=True)
    _prepare_evidence_paths(args.json_path, args.log)

    result = asyncio.run(run_workflow_gate(args.tools_dir, args.native_verification, args.work_dir))
    with args.json_path.open("x", encoding="utf-8") as f:
        json.dump(result, f, indent=2, sort_keys=True)
    with args.log.open("x", encoding="utf-8") as f:
        f.write(json.dumps(result, indent=2, sort_keys=True) + "\n")

    print(json.dumps({
        "verdict": result["verdict"],
        "resourceVerdict": result["resourceVerdict"],
        "functionalVerdict": result["functionalVerdict"],
        "resilienceVerdict": result["resilienceVerdict"],
        "elapsedSeconds": result["elapsedSeconds"],
    }, sort_keys=True))
    return 0 if result["verdict"] == "PASS_LOCAL_WORKFLOW" else 1


if __name__ == "__main__":
    raise SystemExit(main())
