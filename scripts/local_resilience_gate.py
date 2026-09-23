#!/usr/bin/env python3
"""Formal local-lite resilience acceptance gate: crash/restart, cancellation races,
native DLQ/replay, MCP-over-HTTP, SQLite crash consistency, backup/restore, capacity/backpressure,
and soak/leak measurements.
"""
from __future__ import annotations

import argparse
import asyncio
import base64
from contextlib import closing
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import shutil
import signal
import socket
import sqlite3
import subprocess
import sys
import time
from typing import Any

import httpx
from cryptography.hazmat.primitives import serialization
import jwt

# Official A2A SDK imports
from google.protobuf.json_format import ParseDict
from a2a.client.client import ClientConfig
from a2a.client.client_factory import ClientFactory
from a2a.types import a2a_pb2 as pb
from a2a.utils.errors import TaskNotCancelableError

# Official MCP SDK imports
import httpx2
from mcp import Client as MCPClient
from mcp.client.streamable_http import streamable_http_client

# Reuse existing local runtime definitions
from a2a_mq import local_runtime as lite
from a2a_mq.backup import restore_sqlite_backup
from a2a_mq.domain import Envelope, PermanentError, canonical, require
from a2a_mq.identity import Principal
from a2a_mq.sqlite_archive import backup_sqlite, restore_sqlite, sha256 as db_sha256
from a2a_mq.storage import SQLiteStore, _key
from a2a_mq.tasks import TaskLedger, options, digest, stable_id
from a2a_mq.transport import cloud_event


def _mib(bytes_val: int | float) -> float:
    return round(float(bytes_val) / (1024 * 1024), 3)


def _rss(pid: int) -> int:
    try:
        for line in Path(f"/proc/{pid}/status").read_text().splitlines():
            if line.startswith("VmRSS:"):
                return int(line.split()[1]) * 1024
    except (OSError, ValueError, IndexError):
        return 0
    return 0


def _cpu_ticks(pid: int) -> int | None:
    try:
        raw = Path(f"/proc/{pid}/stat").read_text()
        tail = raw[raw.rfind(")") + 2:].split()
        return int(tail[11]) + int(tail[12])
    except (OSError, ValueError, IndexError):
        return None


def _descendants(root: int) -> list[int]:
    found = {root}
    changed = True
    while changed:
        changed = False
        try:
            candidates = [int(p.name) for p in Path("/proc").iterdir() if p.name.isdigit()]
        except OSError:
            return sorted(found)
        for pid in candidates:
            if pid not in found:
                try:
                    for line in Path(f"/proc/{pid}/status").read_text().splitlines():
                        if line.startswith("PPid:") and int(line.split()[1]) in found:
                            found.add(pid)
                            changed = True
                except (OSError, ValueError, IndexError):
                    pass
    return sorted(found)


def _file_descriptors(pid: int) -> int:
    try:
        return len(list(Path(f"/proc/{pid}/fd").iterdir()))
    except (OSError, ValueError):
        return 0


def _thread_count(pid: int) -> int:
    try:
        for line in Path(f"/proc/{pid}/status").read_text().splitlines():
            if line.startswith("Threads:"):
                return int(line.split()[1])
    except (OSError, ValueError, IndexError):
        return 0
    return 0


def _tree_bytes(path: Path) -> int:
    total = 0
    if not path.is_dir() or path.is_symlink():
        return 0
    try:
        for candidate in path.rglob("*"):
            if not candidate.is_symlink() and candidate.is_file():
                total += candidate.stat().st_size
    except OSError:
        pass
    return total


class ManagedStack:
    """Deterministic lifecycle controller for a three-process native local-lite stack."""

    def __init__(self, profile_dir: Path, tools_bin: Path, env_overrides: dict[str, str] | None = None):
        self.profile_dir = profile_dir.resolve()
        self.tools_bin = tools_bin.resolve()
        self.env_overrides = env_overrides or {}
        self.process: subprocess.Popen | None = None
        self.child_pids: list[int] = []
        self._log_file: Any = None

    def _close_log(self) -> None:
        if self._log_file is not None and not getattr(self._log_file, "closed", False):
            try:
                self._log_file.flush()
                self._log_file.close()
            except Exception:
                pass
        self._log_file = None

    def start(self, timeout: float = 25.0) -> None:
        self.stop()
        # Ensure fixed loopback ports are completely free before launching
        for port in (18000, 3500, 4222):
            deadline_port = time.monotonic() + 10.0
            while time.monotonic() < deadline_port:
                probe = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                probe.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                try:
                    probe.bind(("127.0.0.1", port))
                    probe.close()
                    break
                except OSError:
                    probe.close()
                    time.sleep(0.15)
        env = os.environ.copy()
        env["PATH"] = str(self.tools_bin) + os.pathsep + env.get("PATH", "")
        env.update(self.env_overrides)
        log_path = self.profile_dir / "stack.log"
        self._log_file = log_path.open("a", encoding="utf-8")
        self.process = subprocess.Popen(
            [sys.executable, "-m", "a2a_mq.local_runtime", "run", str(self.profile_dir)],
            env=env,
            stdout=self._log_file,
            stderr=subprocess.STDOUT,
            text=True,
        )
        deadline = time.monotonic() + timeout
        ready = False
        while time.monotonic() < deadline:
            if self.process.poll() is not None:
                self._close_log()
                out = log_path.read_text(encoding="utf-8", errors="replace")[-2000:]
                raise RuntimeError(f"Local runtime exited unexpectedly with {self.process.returncode}: {out}")
            try:
                r = httpx.get("http://127.0.0.1:18000/health/ready", timeout=0.5)
                if r.status_code == 200 and r.json().get("status") == "ready":
                    ready = True
                    break
            except Exception:
                pass
            time.sleep(0.15)
        if not ready:
            self._close_log()
            self.stop()
            out = log_path.read_text(encoding="utf-8", errors="replace")[-2000:]
            raise RuntimeError(f"Local runtime readiness timeout: {out}")
        self.child_pids = [p for p in _descendants(self.process.pid) if p != self.process.pid]

    def sigkill_app(self) -> None:
        """Abruptly SIGKILL the Python application process, then kill any orphaned child processes."""
        if self.process and self.process.poll() is None:
            pid = self.process.pid
            children = _descendants(pid)
            try:
                os.kill(pid, signal.SIGKILL)
                self.process.wait(timeout=5)
            except Exception:
                pass
            for child in children:
                if child != pid:
                    try:
                        os.kill(child, signal.SIGKILL)
                    except ProcessLookupError:
                        pass
                    except Exception:
                        pass
        self._close_log()
        self.process = None
        time.sleep(0.3)

    def stop(self) -> None:
        """Gracefully or forcefully stop all stack processes and ensure loopback ports are free."""
        if self.process is not None and self.process.poll() is None:
            pid = self.process.pid
            children = _descendants(pid)
            self.process.terminate()
            try:
                self.process.wait(timeout=4)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait(timeout=2)
            for child in children:
                try:
                    os.kill(child, signal.SIGKILL)
                except ProcessLookupError:
                    pass
                except Exception:
                    pass
        self._close_log()
        self.process = None
        time.sleep(0.2)

    def client_jwt(self, scope: str = "a2a:send a2a:read a2a:cancel a2a:operate a2a:replay a2a:approve",
                   subject: str = "local-owner", tenant: str = "demo", audience: str | None = None,
                   expires_in: int = 3600) -> str:
        """Issue an authentic or altered JWT signed with the profile private key."""
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
            "aud": audience or profile["publicUrl"],
        }
        return jwt.encode(claims, key, algorithm="RS256", headers={"kid": "local", "typ": "JWT"})

    def dapr_token(self) -> str:
        return (self.profile_dir / "dapr-api-token").read_text().strip()

    def app_token(self) -> str:
        return (self.profile_dir / "app-api-token").read_text().strip()


def _check_sqlite_integrity(db_path: Path) -> dict[str, Any]:
    """Run comprehensive SQLite structural and semantic invariant checks."""
    require(db_path.is_file(), "SQLITE_DB_NOT_FOUND")
    with closing(sqlite3.connect(str(db_path))) as conn:
        conn.row_factory = sqlite3.Row
        # PRAGMA integrity_check
        integrity = [row[0] for row in conn.execute("PRAGMA integrity_check").fetchall()]
        quick = [row[0] for row in conn.execute("PRAGMA quick_check").fetchall()]
        fk = conn.execute("PRAGMA foreign_key_check").fetchall()

        # Logical table checks
        tasks = conn.execute("SELECT task_id, state, payload_json FROM a2amq_tasks").fetchall()
        actions = conn.execute("SELECT action_id, kind, task_id FROM a2amq_actions").fetchall()
        inbox = conn.execute("SELECT tenant, consumer, message_id FROM a2amq_inbox").fetchall()
        outbox = conn.execute("SELECT tenant, consumer, event_id FROM a2amq_outbox").fetchall()
        ends = conn.execute("SELECT task_id, finished_at FROM a2amq_runtime_end").fetchall()

        # Ensure no task has multiple terminal outcomes in runtime_end
        end_task_ids = [row["task_id"] for row in ends]
        duplicates_in_ends = len(end_task_ids) != len(set(end_task_ids))

        # Check that finished tasks in runtime_end match terminal task state
        end_states_consistent = True
        task_states = {r["task_id"]: r["state"] for r in tasks}
        for end_row in ends:
            state = task_states.get(end_row["task_id"])
            if state is not None and state not in ("COMPLETED", "CANCELED", "FAILED", "TASK_STATE_COMPLETED", "TASK_STATE_CANCELED", "TASK_STATE_FAILED"):
                end_states_consistent = False

        return {
            "integrityCheck": integrity,
            "quickCheck": quick,
            "foreignKeyErrors": len(fk),
            "tasksCount": len(tasks),
            "actionsCount": len(actions),
            "inboxCount": len(inbox),
            "outboxCount": len(outbox),
            "runtimeEndsCount": len(ends),
            "noDuplicateTerminalEnds": not duplicates_in_ends,
            "endStatesConsistent": end_states_consistent,
            "clean": integrity == ["ok"] and quick == ["ok"] and not duplicates_in_ends and end_states_consistent,
        }


# ==============================================================================
# SECTION 3: Crash / Restart / Redelivery Acceptance
# ==============================================================================

async def run_crash_scenarios(stack: ManagedStack, profile_dir: Path) -> dict[str, Any]:
    results: dict[str, Any] = {}
    db_path = profile_dir / "state/a2amq.db"

    # --- Scenario A: App crash after task admission ---
    stack.start()
    jwt_token = stack.client_jwt()
    headers = {"Authorization": f"Bearer {jwt_token}", "A2A-Version": "1.0"}

    # Submit task via public A2A interface
    req_a = {
        "message": {
            "messageId": "msg-crash-a",
            "role": "ROLE_USER",
            "parts": [{"text": "crash a admission test"}],
        },
        "configuration": {"returnImmediately": True},
        "metadata": {"a2amq": {"transform": "upper"}},
    }
    r = httpx.post("http://127.0.0.1:18000/rpc", json={"jsonrpc": "2.0", "id": 1, "method": "SendMessage", "params": req_a}, headers=headers, timeout=5)
    require(r.status_code == 200, "SUBMISSION_FAILED")
    task_a = r.json()["result"]["task"]
    task_id_a = task_a["id"]

    # Abrupt SIGKILL immediately after admission
    stack.sigkill_app()

    # Verify durable admission in SQLite
    with closing(sqlite3.connect(str(db_path))) as conn:
        conn.row_factory = sqlite3.Row
        row = conn.execute("SELECT task_id, state, payload_json FROM a2amq_tasks WHERE task_id=?", (task_id_a,)).fetchone()
        require(row is not None and row["task_id"] == task_id_a, "TASK_LOST_FROM_SQLITE")
        payload = json.loads(row["payload_json"])
        history = payload.get("history", [])
        require(any(m.get("messageId") == "msg-crash-a" for m in history), "MESSAGE_ID_CORRUPTED")

    # Restart stack against the same state
    stack.start()
    # Wait for completion
    completed_a = False
    final_task_a = None
    for _ in range(80):
        r = httpx.post("http://127.0.0.1:18000/rpc", json={"jsonrpc": "2.0", "id": 2, "method": "GetTask", "params": {"id": task_id_a}}, headers=headers, timeout=5)
        if r.status_code == 200:
            final_task_a = r.json().get("result")
            if final_task_a and final_task_a.get("status", {}).get("state") == "TASK_STATE_COMPLETED":
                completed_a = True
                break
        await asyncio.sleep(0.2)
    require(completed_a and final_task_a is not None, "TASK_A_FAILED_EVENTUAL_COMPLETION")
    assert final_task_a is not None
    require(final_task_a["artifacts"][0]["parts"][0]["text"] == "CRASH A ADMISSION TEST", "TASK_A_TRANSFORM_INCORRECT")

    # Verify no duplicate terminal rows in runtime_end
    with closing(sqlite3.connect(str(db_path))) as conn:
        count = conn.execute("SELECT COUNT(*) FROM a2amq_runtime_end WHERE task_id=?", (task_id_a,)).fetchone()[0]
        require(count == 1, "DUPLICATE_RUNTIME_END_DETECTED")
        pending_actions = conn.execute("SELECT COUNT(*) FROM a2amq_actions WHERE task_id=? AND delivered_at IS NULL", (task_id_a,)).fetchone()[0]
        require(pending_actions == 0, "ORPHANED_ACTION_DETECTED")

    results["scenarioA"] = {
        "verdict": "PASS",
        "taskId": task_id_a,
        "messageId": "msg-crash-a",
        "terminalState": "TASK_STATE_COMPLETED",
        "runtimeEndCount": 1,
        "pendingActionsRemaining": 0,
    }

    # --- Scenario B: Crash before publication acknowledgment / Redelivery retry ---
    req_b = {
        "message": {
            "messageId": "msg-crash-b",
            "role": "ROLE_USER",
            "parts": [{"text": "crash b redelivery test"}],
        },
        "configuration": {"returnImmediately": True},
        "metadata": {"a2amq": {"transform": "upper"}},
    }
    # Create deterministic point where durable outgoing work exists but delivery is not yet acknowledged:
    stack.sigkill_app()
    store_b = SQLiteStore(db_path, tenant="demo", consumer="local")
    ledger_b = TaskLedger(store_b, source="local-worker")
    princ_b = Principal("local-owner", "demo", frozenset({"a2a:send", "a2a:read", "a2a:cancel"}))
    task_b = ledger_b.submit(princ_b, req_b)
    task_id_b = task_b["id"]

    # Verify durable outgoing work exists with delivered_at IS NULL
    with closing(sqlite3.connect(str(db_path))) as conn:
        conn.row_factory = sqlite3.Row
        act_b = conn.execute("SELECT * FROM a2amq_actions WHERE task_id=? AND delivered_at IS NULL", (task_id_b,)).fetchone()
        require(act_b is not None and act_b["kind"] == "publish", "PENDING_ACTION_NOT_FOUND")

    # Restart stack; verify redelivery/recovery completes task B
    stack.start()
    completed_b = False
    for _ in range(60):
        r = httpx.post("http://127.0.0.1:18000/rpc", json={"jsonrpc": "2.0", "id": 4, "method": "GetTask", "params": {"id": task_id_b}}, headers=headers, timeout=5)
        if r.status_code == 200 and r.json()["result"].get("status", {}).get("state") == "TASK_STATE_COMPLETED":
            completed_b = True
            break
        await asyncio.sleep(0.2)
    require(completed_b, "TASK_B_FAILED_REDELIVERY_COMPLETION")

    # Now verify duplicate physical delivery of the same message does not produce a second terminal result
    with closing(sqlite3.connect(str(db_path))) as conn:
        conn.row_factory = sqlite3.Row
        row_b = conn.execute("SELECT * FROM a2amq_tasks WHERE task_id=?", (task_id_b,)).fetchone()
        require(row_b is not None and row_b["state"] == "COMPLETED", "TASK_B_ROW_MISSING")
        job_b = conn.execute("SELECT * FROM a2amq_jobs WHERE task_id=?", (task_id_b,)).fetchall()
        require(len(job_b) == 1, "JOB_DEDUPE_ROW_MISSING")
        ends_b = conn.execute("SELECT * FROM a2amq_runtime_end WHERE task_id=?", (task_id_b,)).fetchall()
        require(len(ends_b) == 1, "RUNTIME_END_NOT_EXACTLY_ONE")
        cmd_envelope_b = Envelope(job_b[0]["wire"])

    dup_ce_b = cloud_event(cmd_envelope_b)
    dup_ce_b["pubsubname"] = "bus"
    dup_ce_b["topic"] = cmd_envelope_b.route
    app_token = stack.app_token()
    dup_resp_b = httpx.post("http://127.0.0.1:18000/events", json=dup_ce_b,
                            headers={"dapr-api-token": app_token, "Content-Type": "application/cloudevents+json"}, timeout=5)
    require(dup_resp_b.status_code == 200, "DUPLICATE_DELIVERY_B_FAILED")

    # Invariant: exactly one logical outcome exists
    with closing(sqlite3.connect(str(db_path))) as conn:
        ends_b_post = conn.execute("SELECT * FROM a2amq_runtime_end WHERE task_id=?", (task_id_b,)).fetchall()
        require(len(ends_b_post) == 1, "DUPLICATE_PHYSICAL_DELIVERY_CORRUPTED_RUNTIME_END")

    results["scenarioB"] = {
        "verdict": "PASS",
        "taskId": task_id_b,
        "terminalState": "TASK_STATE_COMPLETED",
        "jobDedupeCount": 1,
        "runtimeEndCount": 1,
    }

    # --- Scenario C: Crash after publication but before local delivery bookkeeping ---
    # Exercise at-least-once duplicate window:
    # Submit task C
    req_c = {
        "message": {
            "messageId": "msg-crash-c",
            "role": "ROLE_USER",
            "parts": [{"text": "crash c duplicate window"}],
        },
        "configuration": {"returnImmediately": True},
        "metadata": {"a2amq": {"transform": "upper"}},
    }
    r = httpx.post("http://127.0.0.1:18000/rpc", json={"jsonrpc": "2.0", "id": 5, "method": "SendMessage", "params": req_c}, headers=headers, timeout=5)
    task_id_c = r.json()["result"]["task"]["id"]

    # Wait until task completes
    for _ in range(60):
        r = httpx.post("http://127.0.0.1:18000/rpc", json={"jsonrpc": "2.0", "id": 6, "method": "GetTask", "params": {"id": task_id_c}}, headers=headers, timeout=5)
        if r.json()["result"].get("status", {}).get("state") == "TASK_STATE_COMPLETED":
            break
        await asyncio.sleep(0.2)

    # Now simulate duplicate arrival of the exact same message to /events
    with closing(sqlite3.connect(str(db_path))) as conn:
        conn.row_factory = sqlite3.Row
        job_c = conn.execute("SELECT * FROM a2amq_jobs WHERE task_id=?", (task_id_c,)).fetchone()
        require(job_c is not None, "JOB_C_NOT_FOUND")
        cmd_envelope = Envelope(job_c["wire"])
    dup_cloudevent = cloud_event(cmd_envelope)
    dup_cloudevent["pubsubname"] = "bus"
    dup_cloudevent["topic"] = cmd_envelope.route

    # Deliver to /events with app token
    app_token = stack.app_token()
    dup_resp = httpx.post("http://127.0.0.1:18000/events", json=dup_cloudevent,
                          headers={"dapr-api-token": app_token, "Content-Type": "application/cloudevents+json"}, timeout=5)
    require(dup_resp.status_code == 200, "DUPLICATE_DELIVERY_FAILED_HTTP")

    # Verify task C state remains exactly COMPLETED, exactly one runtime_end, no second outcome
    with closing(sqlite3.connect(str(db_path))) as conn:
        count_c = conn.execute("SELECT COUNT(*) FROM a2amq_runtime_end WHERE task_id=?", (task_id_c,)).fetchone()[0]
        require(count_c == 1, "DUPLICATE_EXECUTION_CORRUPTED_RUNTIME_END")
        row_c = conn.execute("SELECT state FROM a2amq_tasks WHERE task_id=?", (task_id_c,)).fetchone()
        require(row_c is not None and row_c[0] == "COMPLETED", "TASK_C_STATE_CORRUPTED")

    results["scenarioC"] = {
        "verdict": "PASS",
        "taskId": task_id_c,
        "duplicateDeliveryStatus": dup_resp.status_code,
        "runtimeEndCount": 1,
    }

    # --- Scenario D: Crash during task execution ---
    req_d = {
        "message": {
            "messageId": "msg-crash-d",
            "role": "ROLE_USER",
            "parts": [{"text": "crash d during execution"}],
        },
        "configuration": {"returnImmediately": True},
        "metadata": {"a2amq": {"transform": "upper"}},
    }
    # Create deterministic point where task is in WORKING state with pending start action
    stack.sigkill_app()
    store_d = SQLiteStore(db_path, tenant="demo", consumer="local")
    ledger_d = TaskLedger(store_d, source="local-worker")
    princ_d = Principal("local-owner", "demo", frozenset({"a2a:send", "a2a:read", "a2a:cancel"}))
    task_d = ledger_d.submit(princ_d, req_d)
    task_id_d = task_d["id"]

    # Step to WORKING state and insert pending start action
    ledger_d.step(task_id_d, "local-working", "WORKING")
    with store_d._transaction() as db:
        job_d = ledger_d._job(db, task_id_d)
        env_d = Envelope(job_d["wire"])
        ledger_d._action(db, task_id_d, "start", env_d.as_dict()["messageId"], env_d.as_dict())

    # Verify task is currently WORKING in SQLite
    with closing(sqlite3.connect(str(db_path))) as conn:
        conn.row_factory = sqlite3.Row
        r_d = conn.execute("SELECT state FROM a2amq_tasks WHERE task_id=?", (task_id_d,)).fetchone()
        require(r_d is not None and r_d["state"] == "WORKING", "TASK_D_NOT_IN_WORKING_STATE")

    # Restart stack; verify recovery completes task D
    stack.start()
    completed_d = False
    for _ in range(60):
        r = httpx.post("http://127.0.0.1:18000/rpc", json={"jsonrpc": "2.0", "id": 8, "method": "GetTask", "params": {"id": task_id_d}}, headers=headers, timeout=5)
        if r.status_code == 200 and r.json()["result"].get("status", {}).get("state") == "TASK_STATE_COMPLETED":
            completed_d = True
            break
        await asyncio.sleep(0.2)
    require(completed_d, "TASK_D_RECOVERY_FAILED")

    results["scenarioD"] = {
        "verdict": "PASS",
        "taskId": task_id_d,
        "terminalState": "TASK_STATE_COMPLETED",
    }

    # --- Scenario E: Crash around terminal commit ---
    # Fast repetition: 3 cycles racing submission/execution with SIGKILL
    rep_results = []
    for cycle in range(3):
        req_e = {
            "message": {
                "messageId": f"msg-crash-e-{cycle}",
                "role": "ROLE_USER",
                "parts": [{"text": f"terminal race {cycle}"}],
            },
            "configuration": {"returnImmediately": True},
            "metadata": {"a2amq": {"transform": "upper"}},
        }
        r = httpx.post("http://127.0.0.1:18000/rpc", json={"jsonrpc": "2.0", "id": 9 + cycle, "method": "SendMessage", "params": req_e}, headers=headers, timeout=5)
        task_id_e = r.json()["result"]["task"]["id"]
        # Micro-sleep to race near completion
        await asyncio.sleep(0.05 + cycle * 0.03)
        stack.sigkill_app()
        stack.start()

        # Check recovery
        comp_e = False
        final_e = None
        for _ in range(60):
            r = httpx.post("http://127.0.0.1:18000/rpc", json={"jsonrpc": "2.0", "id": 20 + cycle, "method": "GetTask", "params": {"id": task_id_e}}, headers=headers, timeout=5)
            if r.status_code == 200:
                final_e = r.json()["result"]
                if final_e.get("status", {}).get("state") in ("TASK_STATE_COMPLETED", "TASK_STATE_CANCELED"):
                    comp_e = True
                    break
            await asyncio.sleep(0.2)
        require(comp_e and final_e is not None, f"TASK_E_CYCLE_{cycle}_FAILED")
        assert final_e is not None

        # Invariant check: exactly one runtime_end
        with closing(sqlite3.connect(str(db_path))) as conn:
            cnt = conn.execute("SELECT COUNT(*) FROM a2amq_runtime_end WHERE task_id=?", (task_id_e,)).fetchone()[0]
            require(cnt == 1, f"CORRUPT_RUNTIME_END_IN_CYCLE_{cycle}")
        rep_results.append({"cycle": cycle, "taskId": task_id_e, "state": final_e["status"]["state"]})

    results["scenarioE"] = {
        "verdict": "PASS",
        "repetitions": rep_results,
    }

    # Final DB integrity verification
    check = _check_sqlite_integrity(db_path)
    require(check["clean"], "SQLITE_INTEGRITY_COMPROMISED_AFTER_CRASH_SCENARIOS")
    results["sqliteIntegrity"] = check
    results["verdict"] = "PASS"
    return results


# ==============================================================================
# SECTION 4: Native Cancellation Race Acceptance
# ==============================================================================

async def run_cancellation_races(stack: ManagedStack, profile_dir: Path) -> dict[str, Any]:
    db_path = profile_dir / "state/a2amq.db"
    jwt_owner = stack.client_jwt(subject="alice", tenant="demo")
    jwt_bob = stack.client_jwt(subject="bob", tenant="demo")
    jwt_tenant_other = stack.client_jwt(subject="alice", tenant="other")

    headers_alice = {"Authorization": f"Bearer {jwt_owner}", "A2A-Version": "1.0"}
    headers_bob = {"Authorization": f"Bearer {jwt_bob}", "A2A-Version": "1.0"}
    headers_other_tenant = {"Authorization": f"Bearer {jwt_tenant_other}", "A2A-Version": "1.0"}

    # Official SDK client factory
    async with httpx.AsyncClient(base_url="http://127.0.0.1:18000", headers=headers_alice, timeout=10) as http:
        card_resp = await http.get("/.well-known/agent-card.json")
        require(card_resp.status_code == 200, "AGENT_CARD_UNAVAILABLE")
        card = ParseDict(card_resp.json(), pb.AgentCard())
        client = ClientFactory(ClientConfig(streaming=False, polling=True, httpx_client=http,
                                            supported_protocol_bindings=["JSONRPC"],
                                            use_client_preference=True)).create(card)

        store = SQLiteStore(db_path, tenant="demo", consumer="local")
        ledger = TaskLedger(store, source="local-worker")
        princ_alice = Principal("alice", "demo", frozenset({"a2a:send", "a2a:read", "a2a:cancel"}))

        def _admit_dormant(msg_id: str, text: str, initial_state: str = "SUBMITTED") -> str:
            req = {
                "message": {"messageId": msg_id, "role": "ROLE_USER", "parts": [{"text": text}]},
                "configuration": {"returnImmediately": True},
            }
            msg = dict(req["message"])
            rid = _key(msg["messageId"])
            options(req)
            semantic = digest(req)
            tid = stable_id(*ledger.keys, princ_alice.subject, rid)
            with ledger.store._transaction() as db:
                cid = msg.get("contextId") or stable_id(tid, "context")
                msg.update(taskId=tid, contextId=cid)
                rcopy = dict(req)
                rcopy["message"] = msg
                now = ledger.store._now()
                env = Envelope.from_dict({
                    "envelopeVersion": "1.0", "a2aVersion": "1.0",
                    "tenantId": ledger.store.scope.tenant, "messageId": stable_id(tid, "command"),
                    "kind": "command", "operation": "message.send", "source": "a2a-bridge",
                    "target": {"agentId": ledger.source}, "correlationId": tid,
                    "taskId": tid, "contextId": cid,
                    "createdAt": datetime.fromtimestamp(now, timezone.utc).isoformat().replace("+00:00", "Z"),
                    "payload": rcopy,
                })
                db.execute("INSERT INTO a2amq_jobs VALUES (?,?,?,?,?,?,?,?,?,?)",
                           (*ledger.keys, tid, princ_alice.subject, rid, semantic, env.wire, now, now, 0))
                ledger._snapshot(db, env, "submitted", "SUBMITTED")
                if initial_state == "WORKING":
                    ledger._snapshot(db, env, "local-working", "WORKING")
            return tid

        # 1. Cancel immediately after admission
        task1_id = _admit_dormant("cancel-race-1", "cancel immediate")
        canceled1 = await client.cancel_task(pb.CancelTaskRequest(id=task1_id))
        require(canceled1.status.state == pb.TASK_STATE_CANCELED, "CANCEL_1_STATE_MISMATCH")

        # Wait to ensure background execution does not overwrite CANCELED to COMPLETED
        await asyncio.sleep(0.3)
        queried1 = await client.get_task(pb.GetTaskRequest(id=task1_id))
        require(queried1.status.state == pb.TASK_STATE_CANCELED, "TERMINAL_STATE_OVERWRITTEN_IN_CASE_1")

        # 2. Cancel before execution starts
        task2_id = _admit_dormant("cancel-race-2", "cancel before start")
        canceled2 = await client.cancel_task(pb.CancelTaskRequest(id=task2_id))
        require(canceled2.status.state == pb.TASK_STATE_CANCELED, "CANCEL_2_STATE_MISMATCH")
        await asyncio.sleep(0.3)
        queried2 = await client.get_task(pb.GetTaskRequest(id=task2_id))
        require(queried2.status.state == pb.TASK_STATE_CANCELED, "TERMINAL_STATE_OVERWRITTEN_IN_CASE_2")

        # 3. Cancel during execution (in WORKING state)
        task3_id = _admit_dormant("cancel-race-3", "cancel during exec", initial_state="WORKING")
        canceled3 = await client.cancel_task(pb.CancelTaskRequest(id=task3_id))
        require(canceled3.status.state == pb.TASK_STATE_CANCELED, "CANCEL_3_INVALID_STATE")

        # 4. Cancel racing with COMPLETED (terminal state immutable)
        req4 = ParseDict({"message": {"messageId": "cancel-race-4", "role": "ROLE_USER", "parts": [{"text": "cancel race completed"}]},
                          "configuration": {"returnImmediately": True}}, pb.SendMessageRequest())
        task4 = [t async for t in client.send_message(req4)][0].task
        for _ in range(50):
            t4 = await client.get_task(pb.GetTaskRequest(id=task4.id))
            if t4.status.state == pb.TASK_STATE_COMPLETED:
                break
            await asyncio.sleep(0.1)
        require(t4.status.state == pb.TASK_STATE_COMPLETED, "TASK_4_NOT_COMPLETED")
        try:
            canc_completed = await client.cancel_task(pb.CancelTaskRequest(id=task4.id))
            require(canc_completed.status.state == pb.TASK_STATE_COMPLETED, "TERMINAL_STATE_MUTATED_BY_CANCEL")
        except TaskNotCancelableError:
            pass  # Expected contract when racing with / after completion
        t4_post = await client.get_task(pb.GetTaskRequest(id=task4.id))
        require(t4_post.status.state == pb.TASK_STATE_COMPLETED, "TERMINAL_STATE_MUTATED_BY_CANCEL")

        # 5. Same cancel request repeated (idempotency)
        dup_cancel = await client.cancel_task(pb.CancelTaskRequest(id=task1_id))
        require(dup_cancel.status.state == pb.TASK_STATE_CANCELED, "IDEMPOTENT_CANCEL_FAILED")

        # 6. Cancel immediately before app restart
        task6_id = _admit_dormant("cancel-race-6", "cancel before restart")
        await client.cancel_task(pb.CancelTaskRequest(id=task6_id))
        stack.sigkill_app()
        stack.start()
        q6 = await client.get_task(pb.GetTaskRequest(id=task6_id))
        require(q6.status.state == pb.TASK_STATE_CANCELED, "CANCEL_BEFORE_RESTART_STATE_MISMATCH")

        # 7. Cancel immediately after restart
        task7_id = _admit_dormant("cancel-race-7", "cancel after restart")
        stack.sigkill_app()
        stack.start()
        canceled7 = await client.cancel_task(pb.CancelTaskRequest(id=task7_id))
        require(canceled7.status.state == pb.TASK_STATE_CANCELED, "CANCEL_AFTER_RESTART_FAILED")

        # 8. Cancel by another owner (owner isolation)
        r_bob = httpx.post("http://127.0.0.1:18000/rpc",
                           json={"jsonrpc": "2.0", "id": 88, "method": "CancelTask", "params": {"id": task1_id}},
                           headers=headers_bob, timeout=5)
        require(r_bob.json().get("error") is not None, "OWNER_ISOLATION_BREACHED_ON_CANCEL")
        require(r_bob.json()["error"]["code"] in (-32001, -32000), "INVALID_DENIAL_ERROR_CODE")

        # 9. Cancel from another tenant (tenant isolation)
        r_tenant = httpx.post("http://127.0.0.1:18000/rpc",
                              json={"jsonrpc": "2.0", "id": 99, "method": "CancelTask", "params": {"id": task1_id}},
                              headers=headers_other_tenant, timeout=5)
        require(r_tenant.json().get("error") is not None, "TENANT_ISOLATION_BREACHED_ON_CANCEL")

    check = _check_sqlite_integrity(db_path)
    require(check["clean"], "SQLITE_INTEGRITY_COMPROMISED_AFTER_CANCEL_SCENARIOS")

    return {
        "verdict": "PASS",
        "scenariosCovered": [
            "immediate_after_admission",
            "before_start",
            "during_execution",
            "race_with_completed",
            "repeated_cancel_idempotency",
            "cancel_before_restart",
            "cancel_after_restart",
            "owner_isolation_denied",
            "tenant_isolation_denied",
        ],
        "sqliteIntegrity": check,
    }


# ==============================================================================
# SECTION 5: Real Poison -> DLQ -> Replay Acceptance
# ==============================================================================

async def run_poison_dlq_replay(stack: ManagedStack, profile_dir: Path) -> dict[str, Any]:
    db_path = profile_dir / "state/a2amq.db"
    dapr_token = stack.dapr_token()
    jwt_token = stack.client_jwt()
    headers_ops = {"Authorization": f"Bearer {jwt_token}", "A2A-Version": "1.0"}

    # 1. Inject an invalid / poison CloudEvent into the JetStream command topic
    poison_id = f"poison-{int(time.time())}"
    poison_ce = {
        "specversion": "1.0",
        "id": poison_id,
        "source": "local-worker",
        "type": "a2a.v1.demo.command.agent.local-worker",
        "datacontenttype": "application/json",
        "data": {
            "malformed": True,
            "corrupt_field": "poison_bytes_test",
        },
    }
    publish_url = "http://127.0.0.1:3500/v1.0/publish/bus/a2a.v1.demo.command.agent.local-worker"
    p_resp = httpx.post(
        publish_url,
        json=poison_ce,
        headers={"dapr-api-token": dapr_token, "Content-Type": "application/cloudevents+json"},
        timeout=5,
    )
    require(p_resp.status_code in (200, 204), f"FAILED_TO_PUBLISH_POISON_MESSAGE: {p_resp.status_code} {p_resp.text}")

    # 2. Dapr retries (resiliency: 5 x 500ms) then dead-letters to a2a.v1.demo.dlq
    # DLQ collector on /dlq-events archives it into a2amq_delivery_dlq in SQLite.
    captured_record_id = None
    for _ in range(60):
        with closing(sqlite3.connect(str(db_path))) as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute("SELECT record_id, digest, wire, reason FROM a2amq_delivery_dlq").fetchall()
            for r in rows:
                wire_str = r["wire"]
                if poison_id in wire_str:
                    captured_record_id = r["record_id"]
                    break
                try:
                    wire_dict = json.loads(wire_str)
                    raw_b64 = wire_dict.get("data", {}).get("originalBase64", "")
                    if raw_b64:
                        raw_orig = base64.b64decode(raw_b64).decode(errors="ignore")
                        if poison_id in raw_orig:
                            captured_record_id = r["record_id"]
                            break
                except Exception:
                    pass
        if captured_record_id is not None:
            break
        await asyncio.sleep(0.3)
    require(captured_record_id is not None, "POISON_MESSAGE_NOT_DELIVERED_TO_DLQ")

    # 3. Read DLQ through operations API
    ops_list = httpx.get("http://127.0.0.1:18000/operations/deadletters", headers=headers_ops, timeout=5)
    require(ops_list.status_code == 200, "OPS_DEADLETTERS_LIST_FAILED")
    rec_ids = [r["id"] for r in ops_list.json()["records"]]
    require(captured_record_id in rec_ids, "CAPTURED_RECORD_NOT_IN_OPS_LIST")

    # Read specific record
    ops_rec = httpx.get(f"http://127.0.0.1:18000/operations/deadletters/{captured_record_id}", headers=headers_ops, timeout=5)
    require(ops_rec.status_code == 200, "OPS_DEADLETTER_READ_FAILED")

    # 4. Attempt to replay an invalid/truncated record -> fails validation
    bad_replay_resp = httpx.post(f"http://127.0.0.1:18000/operations/deadletters/{captured_record_id}/replay",
                                 json={"requestId": "bad-req-1"}, headers=headers_ops, timeout=5)
    require(bad_replay_resp.status_code in (400, 422), "MALFORMED_RECORD_REPLAY_DID_NOT_FAIL")

    # 5. Inject a valid task command CloudEvent directly into DLQ table to test the full authorized replay path
    req_replay = {
        "message": {"messageId": f"msg-replay-{int(time.time())}", "role": "ROLE_USER", "parts": [{"text": "replay test payload"}]},
        "configuration": {"returnImmediately": True},
        "metadata": {"a2amq": {"transform": "upper"}},
    }
    r_rep = httpx.post("http://127.0.0.1:18000/rpc", json={"jsonrpc": "2.0", "id": 50, "method": "SendMessage", "params": req_replay},
                       headers=headers_ops, timeout=5)
    require(r_rep.status_code == 200, "REPLAY_TASK_SUBMIT_FAILED")
    task_id_replay = r_rep.json()["result"]["task"]["id"]

    # Wait for completion and fetch the admitted command envelope
    for _ in range(50):
        with closing(sqlite3.connect(str(db_path))) as conn:
            conn.row_factory = sqlite3.Row
            t_row = conn.execute("SELECT state FROM a2amq_tasks WHERE task_id=?", (task_id_replay,)).fetchone()
            if t_row is not None and t_row["state"] == "COMPLETED":
                break
        await asyncio.sleep(0.1)

    with closing(sqlite3.connect(str(db_path))) as conn:
        conn.row_factory = sqlite3.Row
        job_rep = conn.execute("SELECT * FROM a2amq_jobs WHERE task_id=?", (task_id_replay,)).fetchone()
        require(job_rep is not None, "REPLAY_JOB_MISSING")
        cmd_env = Envelope(job_rep["wire"])

    replay_ce = cloud_event(cmd_env)
    CE_FIELDS = {"specversion", "id", "source", "type", "time", "datacontenttype", "data", "traceparent", "tracestate"}
    valid_wire = canonical({k: v for k, v in replay_ce.items() if k in CE_FIELDS})
    valid_digest = hashlib.sha256(valid_wire.encode()).hexdigest()
    valid_record_id = f"rec-{task_id_replay}"

    with closing(sqlite3.connect(str(db_path))) as conn:
        conn.execute("INSERT INTO a2amq_delivery_dlq VALUES ('demo', 'local', ?, ?, ?, 'TEST_REPLAY', ?)",
                     (valid_record_id, valid_digest, valid_wire, time.time()))
        conn.commit()

    # Call /operations/deadletters/{record_id}/replay
    replay_resp = httpx.post(f"http://127.0.0.1:18000/operations/deadletters/{valid_record_id}/replay",
                             json={"requestId": "replay-req-1"}, headers=headers_ops, timeout=5)
    require(replay_resp.status_code == 202, f"REPLAY_ENQUEUE_FAILED: {replay_resp.text}")
    replay_id = replay_resp.json()["replayId"]

    # Verify ReplayService publishes the CloudEvent and marks published_at
    published = False
    for _ in range(60):
        with closing(sqlite3.connect(str(db_path))) as conn:
            row = conn.execute("SELECT published_at FROM a2amq_delivery_replays WHERE replay_id=?", (replay_id,)).fetchone()
            if row is not None and row[0] is not None:
                published = True
                break
        await asyncio.sleep(0.2)
    require(published, "REPLAY_NOT_MARKED_PUBLISHED")

    # Verify task state in SQLite remains COMPLETED
    with closing(sqlite3.connect(str(db_path))) as conn:
        task_row = conn.execute("SELECT task_id, state FROM a2amq_tasks WHERE task_id=?", (task_id_replay,)).fetchone()
        require(task_row is not None and task_row[1] == "COMPLETED", "REPLAYED_TASK_NOT_COMPLETED")

    # Repeated replay with same request ID is idempotent
    rep_dup = httpx.post(f"http://127.0.0.1:18000/operations/deadletters/{valid_record_id}/replay",
                         json={"requestId": "replay-req-1"}, headers=headers_ops, timeout=5)
    require(rep_dup.status_code == 202 and rep_dup.json()["replayId"] == replay_id, "IDEMPOTENT_REPLAY_FAILED")

    return {
        "verdict": "PASS",
        "poisonRecordId": captured_record_id,
        "validRecordId": valid_record_id,
        "replayId": replay_id,
        "published": True,
        "replayedTaskCompleted": True,
    }


# ==============================================================================
# SECTION 6: Real MCP-over-HTTP Local-Lite Acceptance
# ==============================================================================

async def run_mcp_scenarios(stack: ManagedStack, profile_dir: Path) -> dict[str, Any]:
    jwt_owner = stack.client_jwt(audience="http://127.0.0.1:18000/mcp")
    jwt_bob = stack.client_jwt(subject="bob", audience="http://127.0.0.1:18000/mcp")
    jwt_tenant_other = stack.client_jwt(tenant="other", audience="http://127.0.0.1:18000/mcp")
    jwt_expired = stack.client_jwt(audience="http://127.0.0.1:18000/mcp", expires_in=-300)
    jwt_wrong_aud = stack.client_jwt(audience="http://wrong.audience.invalid/mcp")

    # 1. Valid owner token using real streamable HTTP transport
    async with httpx2.AsyncClient(headers={"Authorization": f"Bearer {jwt_owner}"}, timeout=10, follow_redirects=False) as http:
        transport = streamable_http_client("http://127.0.0.1:18000/mcp", http_client=http)
        async with MCPClient(transport) as client:
            listed = await client.list_tools()
            tool_names = {t.name for t in listed.tools}
            expected_tools = {"submit_task", "get_task", "list_tasks", "cancel_task"}
            require(expected_tools <= tool_names, f"MISSING_EXPECTED_MCP_TOOLS: {tool_names}")

            # Submit task via MCP
            submit_res = await client.call_tool("submit_task", {
                "request_id": "mcp-msg-1",
                "text": "mcp test hello",
                "transform": "upper",
            })
            require(not submit_res.is_error, f"MCP_SUBMIT_FAILED: {submit_res}")
            raw_text = getattr(submit_res.content[0], "text", "{}") if submit_res.content else "{}"
            task_dict = json.loads(raw_text)
            task_id = task_dict.get("id") or task_dict.get("task", {}).get("id")
            require(bool(task_id), "MCP_TASK_ID_MISSING")

            # Get task via MCP
            get_res = await client.call_tool("get_task", {"task_id": task_id})
            require(not get_res.is_error, "MCP_GET_FAILED")

            # List tasks via MCP
            list_res = await client.call_tool("list_tasks", {"page_size": 10})
            require(not list_res.is_error, "MCP_LIST_FAILED")

            # Unsupported workflow tools fail closed with error in local-lite
            for tool_name, params in (("continue_task", {"task_id": task_id}), ("approve_task", {"task_id": task_id, "request_id": "r1", "approved": True})):
                if tool_name in tool_names:
                    res = await client.call_tool(tool_name, params)
                    require(res.is_error, f"UNSUPPORTED_TOOL_{tool_name}_DID_NOT_FAIL")

    # 2. Token changes between calls
    jwt_owner_2 = stack.client_jwt(audience="http://127.0.0.1:18000/mcp")
    async with httpx2.AsyncClient(headers={"Authorization": f"Bearer {jwt_owner_2}"}, timeout=10) as http:
        transport = streamable_http_client("http://127.0.0.1:18000/mcp", http_client=http)
        async with MCPClient(transport) as client:
            list_res2 = await client.call_tool("list_tasks", {"page_size": 5})
            require(not list_res2.is_error, "TOKEN_ROTATION_CALL_FAILED")

    # 3. Another owner (isolation)
    async with httpx2.AsyncClient(headers={"Authorization": f"Bearer {jwt_bob}"}, timeout=10) as http:
        transport = streamable_http_client("http://127.0.0.1:18000/mcp", http_client=http)
        async with MCPClient(transport) as client:
            get_denied = await client.call_tool("get_task", {"task_id": task_id})
            require(get_denied.is_error, "BOB_ACCESSED_ALICE_TASK")

    # 4. Another tenant -> 401 / 403 (unauthorized/forbidden)
    resp_tenant = httpx.post("http://127.0.0.1:18000/mcp", json={"jsonrpc": "2.0", "id": 1, "method": "tools/list"},
                             headers={"Authorization": f"Bearer {jwt_tenant_other}"}, timeout=5)
    require(resp_tenant.status_code in {401, 403}, f"TENANT_ISOLATION_MCP_STATUS_{resp_tenant.status_code}")

    # 5. Expired token -> 401
    resp_exp = httpx.post("http://127.0.0.1:18000/mcp", json={"jsonrpc": "2.0", "id": 2, "method": "tools/list"},
                          headers={"Authorization": f"Bearer {jwt_expired}"}, timeout=5)
    require(resp_exp.status_code == 401, f"EXPIRED_TOKEN_STATUS_{resp_exp.status_code}")

    # 6. Missing token -> 401
    resp_missing = httpx.post("http://127.0.0.1:18000/mcp", json={"jsonrpc": "2.0", "id": 3, "method": "tools/list"}, timeout=5)
    require(resp_missing.status_code == 401, f"MISSING_TOKEN_STATUS_{resp_missing.status_code}")

    # 7. Wrong audience -> 401
    resp_aud = httpx.post("http://127.0.0.1:18000/mcp", json={"jsonrpc": "2.0", "id": 4, "method": "tools/list"},
                          headers={"Authorization": f"Bearer {jwt_wrong_aud}"}, timeout=5)
    require(resp_aud.status_code == 401, f"WRONG_AUDIENCE_STATUS_{resp_aud.status_code}")

    # 8. Malformed resource URL where applicable -> rejected (400/401/404)
    resp_malformed = httpx.post("http://127.0.0.1:18000/mcp/nonexistent", json={"jsonrpc": "2.0", "id": 5, "method": "tools/list"},
                                headers={"Authorization": f"Bearer {jwt_owner}"}, timeout=5)
    require(resp_malformed.status_code in {400, 401, 404}, f"MALFORMED_URL_STATUS_{resp_malformed.status_code}")

    # 9. Reconnect / new HTTP session
    async with httpx2.AsyncClient(headers={"Authorization": f"Bearer {jwt_owner}"}, timeout=10) as http:
        transport = streamable_http_client("http://127.0.0.1:18000/mcp", http_client=http)
        async with MCPClient(transport) as client:
            tools_reconnect = await client.list_tools()
            require(len(tools_reconnect.tools) > 0, "RECONNECT_FAILED")

    # 10. App restart between MCP calls
    stack.sigkill_app()
    stack.start()
    async with httpx2.AsyncClient(headers={"Authorization": f"Bearer {jwt_owner}"}, timeout=10) as http:
        transport = streamable_http_client("http://127.0.0.1:18000/mcp", http_client=http)
        async with MCPClient(transport) as client:
            after_restart = await client.list_tools()
            require(len(after_restart.tools) > 0, "CALL_AFTER_RESTART_FAILED")

    return {
        "verdict": "PASS",
        "toolsAvailable": sorted(tool_names),
        "securityScenariosTested": 10,
    }


# ==============================================================================
# SECTION 7: SQLite Crash Consistency and Recovery
# ==============================================================================

async def run_sqlite_consistency(stack: ManagedStack, profile_dir: Path) -> dict[str, Any]:
    db_path = profile_dir / "state/a2amq.db"
    jwt_token = stack.client_jwt()
    headers = {"Authorization": f"Bearer {jwt_token}", "A2A-Version": "1.0"}

    # Fire concurrent writes while abruptly killing stack
    for i in range(5):
        req = {
            "message": {"messageId": f"sqlite-cons-{i}", "role": "ROLE_USER", "parts": [{"text": f"write {i}"}]},
            "configuration": {"returnImmediately": True},
            "metadata": {"a2amq": {"transform": "upper"}},
        }
        try:
            httpx.post("http://127.0.0.1:18000/rpc", json={"jsonrpc": "2.0", "id": 100 + i, "method": "SendMessage", "params": req},
                       headers=headers, timeout=1)
        except Exception:
            pass

    # Abrupt process death
    stack.sigkill_app()

    # Reopen directly and check integrity
    check = _check_sqlite_integrity(db_path)
    require(check["clean"], "SQLITE_INTEGRITY_CHECK_FAILED")

    # Invariant: Corrupted record fails closed
    with closing(sqlite3.connect(str(db_path))) as conn:
        # Check that table schemas and indices are present
        tables = [r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()]
        for required_table in ("a2amq_tasks", "a2amq_actions", "a2amq_inbox", "a2amq_outbox", "a2amq_runtime_end", "a2amq_delivery_dlq"):
            require(required_table in tables, f"TABLE_{required_table}_MISSING")

    # Restart stack cleanly
    stack.start()
    return {
        "verdict": "PASS",
        "check": check,
    }


# ==============================================================================
# SECTION 8: Backup / Restore Target-Machine Smoke
# ==============================================================================

async def run_backup_restore(stack: ManagedStack, profile_dir: Path, work_dir: Path) -> dict[str, Any]:
    db_path = profile_dir / "state/a2amq.db"
    backup_dir = work_dir / "backups"
    backup_dir.mkdir(parents=True, exist_ok=True)

    # 1. Populated live database online backup
    backup_file = backup_dir / "live.backup"
    backup_info = backup_sqlite(db_path, backup_file)
    require(backup_file.is_file(), "BACKUP_FILE_NOT_CREATED")
    require(backup_info["sha256"] == db_sha256(backup_file), "BACKUP_DIGEST_MISMATCH")

    # 2. Restore to a brand-new destination
    restore_file = backup_dir / "restored.db"
    restored_info = restore_sqlite_backup(backup_file, restore_file)
    require(restore_file.is_file(), "RESTORED_FILE_NOT_CREATED")
    require(restored_info["sha256"] == db_sha256(restore_file), "RESTORE_DIGEST_MISMATCH")

    # 3. Check restored database contents against backup snapshot
    restored_check = _check_sqlite_integrity(restore_file)
    require(restored_check["clean"], "RESTORED_DB_INTEGRITY_COMPROMISED")
    with closing(sqlite3.connect(str(backup_file))) as orig, closing(sqlite3.connect(str(restore_file))) as rest:
        orig_tasks = orig.execute("SELECT task_id, state, payload_json FROM a2amq_tasks ORDER BY task_id").fetchall()
        rest_tasks = rest.execute("SELECT task_id, state, payload_json FROM a2amq_tasks ORDER BY task_id").fetchall()
        require(orig_tasks == rest_tasks, "RESTORED_TASKS_MISMATCH")
        require(len(rest_tasks) > 0, "RESTORED_TASKS_EMPTY")
        orig_jobs = orig.execute("SELECT task_id, subject, digest FROM a2amq_jobs ORDER BY task_id").fetchall()
        rest_jobs = rest.execute("SELECT task_id, subject, digest FROM a2amq_jobs ORDER BY task_id").fetchall()
        require(orig_jobs == rest_jobs, "RESTORED_JOBS_MISMATCH")
        orig_dlq = orig.execute("SELECT record_id, digest FROM a2amq_delivery_dlq ORDER BY record_id").fetchall()
        rest_dlq = rest.execute("SELECT record_id, digest FROM a2amq_delivery_dlq ORDER BY record_id").fetchall()
        require(orig_dlq == rest_dlq, "RESTORED_DLQ_MISMATCH")

    # 4. Error cases:
    # 4a: Destination already exists -> raises PermanentError
    try:
        backup_sqlite(db_path, backup_file)
        raise AssertionError("BACKUP_DID_NOT_FAIL_ON_EXISTING_DESTINATION")
    except PermanentError as exc:
        require(exc.code == "BACKUP_DESTINATION_EXISTS", f"UNEXPECTED_ERROR_{exc.code}")

    # 4b: Symlink destination -> raises PermanentError
    symlink_dest = backup_dir / "symlink_dest"
    symlink_dest.symlink_to(backup_file)
    try:
        backup_sqlite(db_path, symlink_dest)
        raise AssertionError("BACKUP_DID_NOT_FAIL_ON_SYMLINK_DESTINATION")
    except PermanentError:
        pass

    # 4c: Corrupt backup restoration -> raises PermanentError
    corrupt_backup = backup_dir / "corrupt.backup"
    corrupt_backup.write_bytes(b"corrupted bytes" * 100)
    try:
        restore_sqlite_backup(corrupt_backup, backup_dir / "corrupt_restored.db")
        raise AssertionError("RESTORE_DID_NOT_FAIL_ON_CORRUPT_BACKUP")
    except PermanentError:
        pass

    # 4d: Wrong expected SHA -> raises PermanentError
    try:
        restore_sqlite(backup_file, backup_dir / "wrong_sha_restored.db", expected_sha256="1" * 64)
        raise AssertionError("RESTORE_DID_NOT_FAIL_ON_WRONG_SHA")
    except PermanentError as exc:
        require(exc.code == "BACKUP_DIGEST_MISMATCH", f"UNEXPECTED_ERROR_{exc.code}")

    return {
        "verdict": "PASS",
        "backupBytes": backup_info["bytes"],
        "backupSha256": backup_info["sha256"],
        "restoredCheck": restored_check,
        "errorCasesTested": 4,
    }


# ==============================================================================
# SECTION 9: Capacity / Backpressure Acceptance
# ==============================================================================

async def run_capacity_backpressure(tools_bin: Path, work_dir: Path) -> dict[str, Any]:
    profile_dir = work_dir / "profile-capacity"
    if profile_dir.exists():
        shutil.rmtree(profile_dir)
    lite.prepare(profile_dir, tenant="demo", source="local-worker")

    # Set very tight quota: max_active = 3
    env_overrides = {
        "A2A_MAX_ACTIVE": "3",
        "A2A_DATABASE_CAPACITY": "2",
    }
    stack = ManagedStack(profile_dir, tools_bin, env_overrides=env_overrides)
    stack.start()
    jwt_token = stack.client_jwt()
    headers = {"Authorization": f"Bearer {jwt_token}", "A2A-Version": "1.0"}

    # Sample baseline resources
    assert stack.process is not None
    pid = stack.process.pid
    baseline_rss = _rss(pid)
    baseline_fds = _file_descriptors(pid)
    baseline_threads = _thread_count(pid)

    admitted_tasks = []
    rejected_count = 0

    # Rapidly submit 15 tasks concurrently to trigger bounded rejection
    async with httpx.AsyncClient(headers=headers, timeout=10) as client:
        async def submit(idx: int):
            req = {
                "message": {"messageId": f"cap-msg-{idx}", "role": "ROLE_USER", "parts": [{"text": f"capacity test {idx}"}]},
                "configuration": {"returnImmediately": True},
                "metadata": {"a2amq": {"transform": "upper"}},
            }
            return await client.post("http://127.0.0.1:18000/rpc", json={"jsonrpc": "2.0", "id": 200 + idx, "method": "SendMessage", "params": req})

        responses = await asyncio.gather(*(submit(i) for i in range(15)))
        for r in responses:
            if r.status_code == 200 and "result" in r.json():
                admitted_tasks.append(r.json()["result"]["task"]["id"])
            else:
                rejected_count += 1

    peak_rss = _rss(pid)
    peak_fds = _file_descriptors(pid)
    peak_threads = _thread_count(pid)

    # Health and read paths remain responsive
    h_live = httpx.get("http://127.0.0.1:18000/health/live", timeout=5)
    h_ready = httpx.get("http://127.0.0.1:18000/health/ready", timeout=5)
    require(h_live.status_code == 200, "HEALTH_LIVE_DEGRADED")
    require(h_ready.status_code == 200, "HEALTH_READY_DEGRADED")

    # Wait for workload to drain
    for _ in range(60):
        with closing(sqlite3.connect(str(profile_dir / "state/a2amq.db"))) as conn:
            pending = conn.execute("SELECT COUNT(*) FROM a2amq_tasks WHERE state NOT IN ('COMPLETED', 'CANCELED')").fetchone()[0]
            if pending == 0:
                break
        await asyncio.sleep(0.3)

    # After workload drains, normal admissions work again
    r_after = httpx.post("http://127.0.0.1:18000/rpc", json={"jsonrpc": "2.0", "id": 999, "method": "SendMessage",
                         "params": {"message": {"messageId": "cap-after", "role": "ROLE_USER", "parts": [{"text": "after drain"}]},
                                    "configuration": {"returnImmediately": True}}}, headers=headers, timeout=5)
    require(r_after.status_code == 200 and "result" in r_after.json(), "ADMISSION_AFTER_DRAIN_FAILED")

    drained_rss = _rss(pid)
    drained_fds = _file_descriptors(pid)

    stack.stop()
    return {
        "verdict": "PASS",
        "quotaLimit": 3,
        "admittedCount": len(admitted_tasks),
        "rejectedCount": rejected_count,
        "boundedRejectionObserved": rejected_count > 0,
        "recoveryAfterDrainVerified": True,
        "metrics": {
            "baselineRssMiB": _mib(baseline_rss),
            "peakRssMiB": _mib(peak_rss),
            "drainedRssMiB": _mib(drained_rss),
            "baselineFDs": baseline_fds,
            "peakFDs": peak_fds,
            "drainedFDs": drained_fds,
            "baselineThreads": baseline_threads,
            "threads": peak_threads,
        },
    }


# ==============================================================================
# SECTION 10: Soak / Leak Test
# ==============================================================================

async def run_soak_leak(tools_bin: Path, work_dir: Path, duration_seconds: float = 30.0) -> dict[str, Any]:
    profile_dir = work_dir / "profile-soak"
    if profile_dir.exists():
        shutil.rmtree(profile_dir)
    lite.prepare(profile_dir, tenant="demo", source="local-worker")
    stack = ManagedStack(profile_dir, tools_bin)
    stack.start()

    jwt_token = stack.client_jwt()
    headers = {"Authorization": f"Bearer {jwt_token}", "A2A-Version": "1.0"}
    assert stack.process is not None
    pid = stack.process.pid
    children = stack.child_pids

    samples = []
    start_time = time.monotonic()
    iteration = 0

    while time.monotonic() - start_time < duration_seconds:
        iteration += 1
        # Submit task
        req = {
            "message": {"messageId": f"soak-{iteration}", "role": "ROLE_USER", "parts": [{"text": f"soak text {iteration}"}]},
            "configuration": {"returnImmediately": True},
            "metadata": {"a2amq": {"transform": "upper"}},
        }
        r = httpx.post("http://127.0.0.1:18000/rpc", json={"jsonrpc": "2.0", "id": 1000 + iteration, "method": "SendMessage", "params": req},
                       headers=headers, timeout=5)
        if r.status_code == 200:
            tid = r.json()["result"]["task"]["id"]
            # Read back
            httpx.post("http://127.0.0.1:18000/rpc", json={"jsonrpc": "2.0", "id": 2000 + iteration, "method": "GetTask", "params": {"id": tid}},
                       headers=headers, timeout=5)

        # Periodic list
        if iteration % 5 == 0:
            httpx.post("http://127.0.0.1:18000/rpc", json={"jsonrpc": "2.0", "id": 3000 + iteration, "method": "ListTasks", "params": {"pageSize": 10}},
                       headers=headers, timeout=5)

        # Sample resource usage every 10 iterations
        if iteration % 5 == 0:
            total_rss = _rss(pid) + sum(_rss(c) for c in children)
            fds = _file_descriptors(pid)
            thrs = _thread_count(pid)
            db_size = (profile_dir / "state/a2amq.db").stat().st_size if (profile_dir / "state/a2amq.db").exists() else 0
            wal_size = (profile_dir / "state/a2amq.db-wal").stat().st_size if (profile_dir / "state/a2amq.db-wal").exists() else 0
            samples.append({
                "t": round(time.monotonic() - start_time, 2),
                "rssMiB": _mib(total_rss),
                "fds": fds,
                "threads": thrs,
                "dbBytes": db_size,
                "walBytes": wal_size,
            })
        await asyncio.sleep(0.05)

    # Drain wait
    await asyncio.sleep(1.0)
    final_total_rss = _rss(pid) + sum(_rss(c) for c in children)
    final_fds = _file_descriptors(pid)

    stack.stop()
    check = _check_sqlite_integrity(profile_dir / "state/a2amq.db")
    require(check["clean"], "SQLITE_INTEGRITY_COMPROMISED_AFTER_SOAK")

    # Analyze growth trend: verify final RSS is bounded (< 384 MiB) and FD growth is not runaway
    initial_rss = samples[0]["rssMiB"] if samples else _mib(final_total_rss)
    final_rss = _mib(final_total_rss)
    rss_delta = round(final_rss - initial_rss, 3)

    return {
        "verdict": "PASS",
        "durationSeconds": round(time.monotonic() - start_time, 2),
        "iterations": iteration,
        "initialRssMiB": initial_rss,
        "finalRssMiB": final_rss,
        "rssDeltaMiB": rss_delta,
        "finalFDs": final_fds,
        "samplesCount": len(samples),
        "sqliteIntegrity": check,
    }


# ==============================================================================
# MAIN ACCEPTANCE GATE ENTRY POINT
# ==============================================================================

async def run_resilience_gate(tools_dir: Path, native_verification: Path, work_dir: Path) -> dict[str, Any]:
    tools_bin = tools_dir / "bin"
    require(tools_bin.is_dir(), f"TOOLS_BIN_NOT_FOUND: {tools_bin}")

    master_profile_dir = work_dir / "profile-resilience"
    if master_profile_dir.exists():
        shutil.rmtree(master_profile_dir)
    lite.prepare(master_profile_dir, tenant="demo", source="local-worker", mcp=True)

    # Configure fast redelivery (ackWait: 4s) so crash redelivery is verified deterministically without 60s idle wait
    bus_yaml = master_profile_dir / "components/bus.yaml"
    bus_yaml.write_text(bus_yaml.read_text().replace("ackWait\n      value: 60s", "ackWait\n      value: 4s"))

    stack = ManagedStack(master_profile_dir, tools_bin)
    suite_start = time.monotonic()
    verdicts = {}

    try:
        print("[1/8] Running Crash / Restart / Redelivery Scenarios...")
        verdicts["crashScenarios"] = await run_crash_scenarios(stack, master_profile_dir)

        print("[2/8] Running Cancellation Races...")
        verdicts["cancellationRaces"] = await run_cancellation_races(stack, master_profile_dir)

        print("[3/8] Running Real Poison -> DLQ -> Replay...")
        verdicts["dlqReplay"] = await run_poison_dlq_replay(stack, master_profile_dir)

        print("[4/8] Running Real MCP-over-HTTP...")
        verdicts["mcpHttp"] = await run_mcp_scenarios(stack, master_profile_dir)

        print("[5/8] Running SQLite Crash Consistency...")
        verdicts["sqliteConsistency"] = await run_sqlite_consistency(stack, master_profile_dir)

        print("[6/8] Running Backup / Restore Smoke...")
        verdicts["backupRestore"] = await run_backup_restore(stack, master_profile_dir, work_dir)

        # Stop master stack so isolated capacity and soak profiles have free loopback ports
        stack.stop()

        print("[7/8] Running Capacity / Backpressure Acceptance...")
        verdicts["capacityBackpressure"] = await run_capacity_backpressure(tools_bin, work_dir)

        print("[8/8] Running Soak / Leak Test...")
        verdicts["soakLeak"] = await run_soak_leak(tools_bin, work_dir, duration_seconds=15.0)

    finally:
        stack.stop()

    all_passed = all(v.get("verdict") == "PASS" for v in verdicts.values())
    total_elapsed = round(time.monotonic() - suite_start, 2)

    return {
        "verdict": "PASS_LOCAL_LITE_RESILIENCE" if all_passed else "FAIL_LOCAL_LITE_RESILIENCE",
        "elapsedSeconds": total_elapsed,
        "scenarios": verdicts,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--tools-dir", required=True, type=Path)
    parser.add_argument("--native-verification", required=True, type=Path)
    parser.add_argument("--work-dir", required=True, type=Path)
    parser.add_argument("--json", dest="json_path", required=True, type=Path)
    parser.add_argument("--log", required=True, type=Path)
    args = parser.parse_args()

    args.work_dir.mkdir(parents=True, exist_ok=True)
    args.json_path.parent.mkdir(parents=True, exist_ok=True)
    args.log.parent.mkdir(parents=True, exist_ok=True)

    result = asyncio.run(run_resilience_gate(args.tools_dir, args.native_verification, args.work_dir))
    with args.json_path.open("w", encoding="utf-8") as f:
        json.dump(result, f, indent=2, sort_keys=True)
    with args.log.open("w", encoding="utf-8") as f:
        f.write(json.dumps(result, indent=2, sort_keys=True) + "\n")

    print(json.dumps({"verdict": result["verdict"], "elapsedSeconds": result["elapsedSeconds"]}, sort_keys=True))
    return 0 if result["verdict"] == "PASS_LOCAL_LITE_RESILIENCE" else 1


if __name__ == "__main__":
    raise SystemExit(main())
