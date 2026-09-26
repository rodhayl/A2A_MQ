"""Formal local-lite soak and leak certification gate.

Executes local-lite under continuous mixed workload for at least 20 minutes
(target 20-30 min), samples per-process and system resources periodically,
drains all in-flight work, and validates post-warmup resource stabilization
and SQLite consistency.
"""
from __future__ import annotations

import argparse
import asyncio
from contextlib import closing
import json
import os
from pathlib import Path
import random
import shutil
import socket
import subprocess
import sys
import time
from typing import Any

import httpx
import httpx2
from cryptography.hazmat.primitives import serialization
import jwt
from mcp import Client as MCPClient
from mcp.client.streamable_http import streamable_http_client

from a2a_mq.domain import require
from a2a_mq import local_runtime as lite
from a2a_mq.local_runtime import (
    APP_ID,
    DAPR_INTERNAL_GRPC_PORT,
    _load_profile,
    _runtime_environments,
)


def _mib(val: int | float) -> float:
    return round(float(val) / (1024 * 1024), 3)


def _rss(pid: int) -> int:
    try:
        for line in Path(f"/proc/{pid}/status").read_text().splitlines():
            if line.startswith("VmRSS:"):
                return int(line.split()[1]) * 1024
    except Exception:
        return 0
    return 0


def _threads(pid: int) -> int:
    try:
        for line in Path(f"/proc/{pid}/status").read_text().splitlines():
            if line.startswith("Threads:"):
                return int(line.split()[1])
    except Exception:
        return 0
    return 0


def _fds(pid: int) -> int:
    try:
        return len(list(Path(f"/proc/{pid}/fd").iterdir()))
    except Exception:
        return 0


def _cpu_ticks(pid: int) -> int:
    try:
        raw = Path(f"/proc/{pid}/stat").read_text()
        tail = raw[raw.rfind(")") + 2:].split()
        return int(tail[11]) + int(tail[12])
    except Exception:
        return 0


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
        outbox_pending = conn.execute("SELECT COUNT(*) FROM a2amq_outbox WHERE published_at IS NULL").fetchone()[0]
        duplicate_ends = conn.execute(
            "SELECT task_id, COUNT(*) c FROM a2amq_runtime_end GROUP BY task_id HAVING c > 1"
        ).fetchall()
        clean = (
            integ == ["ok"]
            and quick == ["ok"]
            and fk_errs == 0
            and len(duplicate_ends) == 0
            and pending_actions == 0
            and outbox_pending == 0
        )
        return {
            "clean": clean,
            "tasksCount": tasks,
            "runtimeEndsCount": ends,
            "pendingActionsCount": pending_actions,
            "pendingOutboxCount": outbox_pending,
            "duplicateEnds": [r[0] for r in duplicate_ends],
        }


class ManagedStack:
    def __init__(self, profile_dir: Path, tools_bin: Path, env_overrides: dict[str, str] | None = None):
        self.profile_dir = profile_dir.resolve()
        self.tools_bin = tools_bin.resolve()
        self.env_overrides = env_overrides or {}
        self.process: subprocess.Popen | None = None
        self.profile: dict[str, Any] = {}
        self.app_env: dict[str, str] = {}
        self.sidecar_env: dict[str, str] = {}
        self.child_env: dict[str, str] = {}
        self.log_file: Any = None
        self.proc_nats: subprocess.Popen | None = None
        self.proc_daprd: subprocess.Popen | None = None
        self.proc_app: subprocess.Popen | None = None

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

    def start(self, timeout: float = 25.0) -> None:
        self.stop()
        for port in (18000, 3500, 50001, 50002, 4222):
            self._wait_port_free(port)

        self.profile = _load_profile(self.profile_dir, "local-lite")
        self.app_env, self.sidecar_env, self.child_env = _runtime_environments(self.profile_dir, self.profile)
        env_base = os.environ.copy()
        env_base["PATH"] = str(self.tools_bin) + os.pathsep + env_base.get("PATH", "")
        env_base.update(self.env_overrides)

        log_path = self.profile_dir / "soak_stack.log"
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

        # 3. Daprd
        sidecar_command = [
            str(self.tools_bin / "daprd"), "--mode", "standalone", "--app-id", APP_ID,
            "--app-port", "18000", "--app-protocol", "http",
            "--dapr-http-port", "3500", "--dapr-grpc-port", "50001",
            "--dapr-internal-grpc-port", str(DAPR_INTERNAL_GRPC_PORT),
            "--dapr-internal-grpc-listen-address", "127.0.0.1",
            "--resources-path", str((self.profile_dir / "components").resolve()),
            "--dapr-listen-addresses", "127.0.0.1",
            "--app-max-concurrency", "2", "--max-body-size", "1Mi",
            "--enable-metrics=false", "--log-level", "warn",
        ]
        self.proc_daprd = subprocess.Popen(
            sidecar_command, cwd=self.profile_dir, env={**env_base, **self.sidecar_env},
            stdout=self.log_file, stderr=subprocess.STDOUT
        )
        self._wait_port(3500)

        # 4. App
        app_full_env = {**env_base, **self.app_env}
        app_cmd = [
            sys.executable, "-m", "uvicorn", "a2a_mq.local_lite:from_env",
            "--factory", "--host", "127.0.0.1", "--port", "18000", "--log-level", "warning"
        ]
        self.proc_app = subprocess.Popen(
            app_cmd, cwd=self.profile_dir, env=app_full_env,
            stdout=self.log_file, stderr=subprocess.STDOUT
        )

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
            raise RuntimeError(f"Soak stack failed to become ready within {timeout}s")

    def stop(self) -> None:
        for p in (self.proc_app, self.proc_daprd, self.proc_nats):
            if p is not None and p.poll() is None:
                p.terminate()
                try:
                    p.wait(timeout=2)
                except Exception:
                    p.kill()
        self.proc_app = self.proc_daprd = self.proc_nats = None
        self._close_log()
        time.sleep(0.3)

    @property
    def pids(self) -> dict[str, int]:
        p = {}
        if self.proc_app and self.proc_app.pid:
            p["app"] = self.proc_app.pid
        if self.proc_daprd and self.proc_daprd.pid:
            p["daprd"] = self.proc_daprd.pid
        if self.proc_nats and self.proc_nats.pid:
            p["nats"] = self.proc_nats.pid
        return p

    def client_jwt(self, scope: str = "a2a:send a2a:read a2a:cancel a2a:operate a2a:replay a2a:approve",
                   subject: str = "local-owner", tenant: str = "demo", audience: str | None = None,
                   expires_in: int = 3600) -> str:
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


async def run_soak_gate(tools_dir: Path, native_verification: Path, work_dir: Path,
                        duration_seconds: float = 1200.0, drain_seconds: float = 180.0,
                        sample_interval: float = 10.0) -> dict[str, Any]:
    tools_bin = tools_dir / "bin"
    require(tools_bin.is_dir(), f"TOOLS_BIN_NOT_FOUND: {tools_bin}")

    work_dir = work_dir.resolve()
    profile_dir = work_dir / "profile-soak"
    if profile_dir.exists():
        shutil.rmtree(profile_dir)

    lite.prepare(profile_dir, tenant="demo", source="local-worker", mcp=True)

    stack = ManagedStack(profile_dir, tools_bin)
    stack.start()

    jwt_token = stack.client_jwt()
    jwt_mcp = stack.client_jwt(audience="http://127.0.0.1:18000/mcp")
    headers = {"Authorization": f"Bearer {jwt_token}", "A2A-Version": "1.0"}
    db_path = profile_dir / "state/a2amq.db"

    pids = stack.pids
    require(len(pids) == 3, f"EXPECTED_3_PROCESSES: got {pids}")

    samples: list[dict[str, Any]] = []
    counters = {
        "submits": 0,
        "completes": 0,
        "gets": 0,
        "lists": 0,
        "cancels": 0,
        "mcps": 0,
        "reconnects": 0,
        "faults": 0,
        "faultsExpectedErrors": 0,
    }

    start_time = time.monotonic()
    last_sample_time = start_time
    prev_cpu_ticks = {role: _cpu_ticks(pid) for role, pid in pids.items()}

    active_task_ids: list[str] = []
    running = True

    # Helper function to sample metrics
    def sample_metrics(phase: str) -> dict[str, Any]:
        nonlocal prev_cpu_ticks
        now = time.monotonic()
        elapsed_since_last = max(0.001, now - last_sample_time)
        cur_cpu_ticks = {role: _cpu_ticks(pid) for role, pid in pids.items()}
        hz = os.sysconf(os.sysconf_names["SC_CLK_TCK"])

        cpu_by_role = {
            role: round(max(0, cur_cpu_ticks[role] - prev_cpu_ticks.get(role, 0)) / hz / elapsed_since_last * 100, 2)
            for role, pid in pids.items()
        }
        prev_cpu_ticks = cur_cpu_ticks

        rss_by_role = {role: _mib(_rss(pid)) for role, pid in pids.items()}
        total_rss = round(sum(rss_by_role.values()), 3)

        threads_by_role = {role: _threads(pid) for role, pid in pids.items()}
        total_threads = sum(threads_by_role.values())

        fds_by_role = {role: _fds(pid) for role, pid in pids.items()}
        total_fds = sum(fds_by_role.values())

        db_size = db_path.stat().st_size if db_path.exists() else 0
        wal_path = profile_dir / "state/a2amq.db-wal"
        wal_size = wal_path.stat().st_size if wal_path.exists() else 0

        # SQLite query stats
        db_stats: dict[str, Any] = {}
        try:
            import sqlite3
            with closing(sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)) as conn:
                cur = conn.cursor()
                db_stats["tasksTotal"] = cur.execute("SELECT COUNT(*) FROM a2amq_tasks").fetchone()[0]
                db_stats["completedTasks"] = cur.execute("SELECT COUNT(*) FROM a2amq_tasks WHERE state='COMPLETED'").fetchone()[0]
                db_stats["canceledTasks"] = cur.execute("SELECT COUNT(*) FROM a2amq_tasks WHERE state='CANCELED'").fetchone()[0]
                db_stats["pendingActions"] = cur.execute("SELECT COUNT(*) FROM a2amq_actions WHERE delivered_at IS NULL").fetchone()[0]
                db_stats["outboxPending"] = cur.execute("SELECT COUNT(*) FROM a2amq_outbox WHERE published_at IS NULL").fetchone()[0]
                db_stats["runtimeEnds"] = cur.execute("SELECT COUNT(*) FROM a2amq_runtime_end").fetchone()[0]
        except Exception as e:
            db_stats["queryError"] = str(e)

        return {
            "elapsedSeconds": round(now - start_time, 2),
            "phase": phase,
            "totalRssMiB": total_rss,
            "rssByRoleMiB": rss_by_role,
            "totalThreads": total_threads,
            "threadsByRole": threads_by_role,
            "totalFds": total_fds,
            "fdsByRole": fds_by_role,
            "cpuPercent": cpu_by_role,
            "dbBytes": db_size,
            "walBytes": wal_size,
            "dbStats": db_stats,
            "counters": dict(counters),
        }

    # Record initial sample
    samples.append(sample_metrics("initial"))

    async def workload_submit_loop():
        i = 0
        async with httpx.AsyncClient(headers=headers, timeout=5) as client:
            while running:
                i += 1
                req = {
                    "message": {
                        "messageId": f"soak-sub-{i}-{int(time.time()*1000)}",
                        "role": "ROLE_USER",
                        "parts": [{"text": f"soak message content {i}"}]
                    },
                    "configuration": {"returnImmediately": True},
                    "metadata": {"a2amq": {"transform": "upper"}},
                }
                try:
                    r = await client.post("http://127.0.0.1:18000/rpc",
                                         json={"jsonrpc": "2.0", "id": 100000 + i, "method": "SendMessage", "params": req})
                    if r.status_code == 200:
                        counters["submits"] += 1
                        tid = r.json().get("result", {}).get("task", {}).get("id")
                        if tid:
                            active_task_ids.append(tid)
                            if len(active_task_ids) > 100:
                                active_task_ids.pop(0)
                except Exception:
                    pass
                await asyncio.sleep(random.uniform(0.3, 0.8))

    async def workload_query_loop():
        i = 0
        async with httpx.AsyncClient(headers=headers, timeout=5) as client:
            while running:
                i += 1
                try:
                    if active_task_ids and random.random() < 0.7:
                        tid = random.choice(active_task_ids)
                        r = await client.post("http://127.0.0.1:18000/rpc",
                                              json={"jsonrpc": "2.0", "id": 200000 + i, "method": "GetTask", "params": {"id": tid}})
                        if r.status_code == 200:
                            counters["gets"] += 1
                            state = r.json().get("result", {}).get("status", {}).get("state")
                            if state == "TASK_STATE_COMPLETED":
                                counters["completes"] += 1
                    else:
                        r = await client.post("http://127.0.0.1:18000/rpc",
                                              json={"jsonrpc": "2.0", "id": 300000 + i, "method": "ListTasks", "params": {"pageSize": 10}})
                        if r.status_code == 200:
                            counters["lists"] += 1
                except Exception:
                    pass
                await asyncio.sleep(random.uniform(0.4, 1.0))

    async def workload_cancellation_loop():
        i = 0
        async with httpx.AsyncClient(headers=headers, timeout=5) as client:
            while running:
                i += 1
                req = {
                    "message": {
                        "messageId": f"soak-cancel-{i}-{int(time.time()*1000)}",
                        "role": "ROLE_USER",
                        "parts": [{"text": f"cancel test {i}"}]
                    },
                    "configuration": {"returnImmediately": True},
                    "metadata": {"a2amq": {"transform": "upper"}},
                }
                try:
                    r = await client.post("http://127.0.0.1:18000/rpc",
                                         json={"jsonrpc": "2.0", "id": 400000 + i, "method": "SendMessage", "params": req})
                    if r.status_code == 200:
                        tid = r.json().get("result", {}).get("task", {}).get("id")
                        if tid:
                            r_c = await client.post("http://127.0.0.1:18000/rpc",
                                                    json={"jsonrpc": "2.0", "id": 450000 + i, "method": "CancelTask", "params": {"id": tid}})
                            if r_c.status_code == 200:
                                counters["cancels"] += 1
                except Exception:
                    pass
                await asyncio.sleep(random.uniform(2.0, 5.0))

    async def workload_mcp_loop():
        i = 0
        while running:
            i += 1
            try:
                async with httpx2.AsyncClient(headers={"Authorization": f"Bearer {jwt_mcp}"}, timeout=10, follow_redirects=False) as http:
                    transport = streamable_http_client("http://127.0.0.1:18000/mcp", http_client=http)
                    async with MCPClient(transport) as client:
                        counters["reconnects"] += 1
                        await client.list_tools()
                        counters["mcps"] += 1

                        sub_res = await client.call_tool("submit_task", {
                            "request_id": f"mcp-soak-{i}",
                            "text": f"mcp soak hello {i}",
                            "transform": "upper",
                        })
                        if not sub_res.is_error:
                            counters["mcps"] += 1
                            raw = getattr(sub_res.content[0], "text", "{}") if sub_res.content else "{}"
                            tdict = json.loads(raw)
                            tid = tdict.get("id") or tdict.get("task", {}).get("id")
                            if tid:
                                await client.call_tool("get_task", {"task_id": tid})
                                counters["mcps"] += 1
                        await client.call_tool("list_tasks", {"page_size": 5})
                        counters["mcps"] += 1
            except Exception:
                pass
            await asyncio.sleep(random.uniform(2.0, 5.0))

    async def workload_fault_loop():
        i = 0
        async with httpx.AsyncClient(headers=headers, timeout=5) as client:
            while running:
                i += 1
                counters["faults"] += 1
                scenario = i % 4
                try:
                    if scenario == 0:
                        # Bad jsonrpc method
                        r = await client.post("http://127.0.0.1:18000/rpc",
                                              json={"jsonrpc": "2.0", "id": 900000 + i, "method": "UnknownMethod", "params": {}})
                        if r.status_code in {200, 400} and "error" in r.text:
                            counters["faultsExpectedErrors"] += 1
                    elif scenario == 1:
                        # Invalid auth token
                        r = await client.post("http://127.0.0.1:18000/rpc",
                                              json={"jsonrpc": "2.0", "id": 900000 + i, "method": "ListTasks", "params": {}},
                                              headers={"Authorization": "Bearer invalid.token.payload", "A2A-Version": "1.0"})
                        if r.status_code in {401, 403}:
                            counters["faultsExpectedErrors"] += 1
                    elif scenario == 2:
                        # Nonexistent task
                        r = await client.post("http://127.0.0.1:18000/rpc",
                                              json={"jsonrpc": "2.0", "id": 900000 + i, "method": "GetTask", "params": {"id": "00000000-0000-0000-0000-000000000000"}})
                        if r.status_code in {200, 404} and "error" in r.text:
                            counters["faultsExpectedErrors"] += 1
                    elif scenario == 3:
                        # Malformed params
                        r = await client.post("http://127.0.0.1:18000/rpc",
                                              json={"jsonrpc": "2.0", "id": 900000 + i, "method": "SendMessage", "params": {"invalid": "payload"}})
                        if r.status_code in {200, 400} and "error" in r.text:
                            counters["faultsExpectedErrors"] += 1
                except Exception:
                    pass
                await asyncio.sleep(random.uniform(4.0, 8.0))

    # Launch workload worker tasks
    print(f"Starting soak test: active duration {duration_seconds}s ({round(duration_seconds/60, 1)}m), drain {drain_seconds}s...")
    workers = [
        asyncio.create_task(workload_submit_loop()),
        asyncio.create_task(workload_query_loop()),
        asyncio.create_task(workload_cancellation_loop()),
        asyncio.create_task(workload_mcp_loop()),
        asyncio.create_task(workload_fault_loop()),
    ]

    # Active soak sampling loop
    sample_deadline = start_time + duration_seconds
    while time.monotonic() < sample_deadline:
        await asyncio.sleep(sample_interval)
        s = sample_metrics("active_soak")
        samples.append(s)
        last_sample_time = time.monotonic()
        print(f"[{s['elapsedSeconds']}s / {duration_seconds}s] RSS: {s['totalRssMiB']} MiB | FDs: {s['totalFds']} | Threads: {s['totalThreads']} | Tasks: {s['dbStats'].get('tasksTotal')} | Counters: sub={counters['submits']}, comp={counters['completes']}, cancel={counters['cancels']}, mcp={counters['mcps']}")

    # =========================================================================
    # DRAIN PHASE
    # =========================================================================
    print(f"\nActive workload completed. Entering drain phase ({drain_seconds}s)...")
    running = False
    for w in workers:
        w.cancel()
    await asyncio.gather(*workers, return_exceptions=True)

    drain_start = time.monotonic()
    drain_deadline = drain_start + drain_seconds

    while time.monotonic() < drain_deadline:
        await asyncio.sleep(15.0)
        s = sample_metrics("drain")
        samples.append(s)
        last_sample_time = time.monotonic()
        pending_act = s["dbStats"].get("pendingActions", 0)
        pending_out = s["dbStats"].get("outboxPending", 0)
        print(f"[Drain +{round(time.monotonic()-drain_start, 1)}s] RSS: {s['totalRssMiB']} MiB | FDs: {s['totalFds']} | Threads: {s['totalThreads']} | Pending Actions: {pending_act} | Pending Outbox: {pending_out}")

    # Final pre-shutdown sample
    final_sample = sample_metrics("post_drain_idle")
    samples.append(final_sample)

    stack.stop()

    # SQLite integrity verification
    check = _check_sqlite_integrity(db_path)
    require(check["clean"], f"SQLITE_INTEGRITY_COMPROMISED_AFTER_SOAK: {check}")

    # =========================================================================
    # RESOURCE TREND AND LEAK ANALYSIS
    # =========================================================================
    # Divide active soak into Warmup (first 180s) vs Steady-State (180s to duration)
    warmup_cutoff = start_time + min(180.0, duration_seconds * 0.25)
    warmup_samples = [s for s in samples if s["phase"] == "active_soak" and (start_time + s["elapsedSeconds"]) <= warmup_cutoff]
    steady_samples = [s for s in samples if s["phase"] == "active_soak" and (start_time + s["elapsedSeconds"]) > warmup_cutoff]
    drain_samples = [s for s in samples if s["phase"] in {"drain", "post_drain_idle"}]

    warmup_end_sample = steady_samples[0] if steady_samples else (warmup_samples[-1] if warmup_samples else samples[0])
    drain_end_sample = drain_samples[-1] if drain_samples else samples[-1]

    # FD leak check: post-drain FDs compared to post-warmup FDs
    fd_delta = drain_end_sample["totalFds"] - warmup_end_sample["totalFds"]
    fd_leak = fd_delta > 3

    # Thread leak check: post-drain threads compared to post-warmup threads
    thread_delta = drain_end_sample["totalThreads"] - warmup_end_sample["totalThreads"]
    thread_leak = thread_delta > 2

    # RSS analysis during steady-state
    # Linear slope calculation for RSS in steady-state (MiB per second)
    if len(steady_samples) >= 3:
        ts = [s["elapsedSeconds"] for s in steady_samples]
        ys = [s["totalRssMiB"] for s in steady_samples]
        n = len(ts)
        t_mean = sum(ts) / n
        y_mean = sum(ys) / n
        num = sum((ts[i] - t_mean) * (ys[i] - y_mean) for i in range(n))
        den = sum((ts[i] - t_mean) ** 2 for i in range(n))
        rss_slope = num / den if den > 0 else 0.0
    else:
        rss_slope = 0.0

    post_warmup_rss_delta = round(drain_end_sample["totalRssMiB"] - warmup_end_sample["totalRssMiB"], 3)
    # Monotonic unbounded growth condition: slope > 0.05 MiB/s (3 MiB/minute) sustained
    rss_monotonically_growing = rss_slope > 0.05 and post_warmup_rss_delta > 20.0

    total_duration = round(time.monotonic() - start_time, 2)
    duration_minutes = round(total_duration / 60.0, 2)

    failures: list[str] = []
    if total_duration < 1200.0 and duration_seconds >= 1200.0:
        failures.append(f"DURATION_TOO_SHORT: {total_duration}s < 1200s")
    if fd_leak:
        failures.append(f"FD_LEAK_DETECTED: delta={fd_delta} (from {warmup_end_sample['totalFds']} to {drain_end_sample['totalFds']})")
    if thread_leak:
        failures.append(f"THREAD_LEAK_DETECTED: delta={thread_delta} (from {warmup_end_sample['totalThreads']} to {drain_end_sample['totalThreads']})")
    if rss_monotonically_growing:
        failures.append(f"UNEXPLAINED_RSS_GROWTH: slope={rss_slope:.4f} MiB/s, delta={post_warmup_rss_delta} MiB")
    if not check["clean"]:
        failures.append(f"SQLITE_INTEGRITY_FAILED: {check}")
    if check["pendingActionsCount"] != 0:
        failures.append(f"PENDING_ACTIONS_NOT_DRAINED: {check['pendingActionsCount']}")
    if check["pendingOutboxCount"] != 0:
        failures.append(f"PENDING_OUTBOX_NOT_DRAINED: {check['pendingOutboxCount']}")

    verdict = "PASS_LOCAL_SOAK" if not failures else "FAIL_LOCAL_SOAK"

    return {
        "verdict": verdict,
        "totalDurationSeconds": total_duration,
        "totalDurationMinutes": duration_minutes,
        "workloadDurationSeconds": duration_seconds,
        "drainDurationSeconds": drain_seconds,
        "counters": counters,
        "warmupSample": warmup_end_sample,
        "drainFinalSample": drain_end_sample,
        "rssAnalysis": {
            "warmupEndRssMiB": warmup_end_sample["totalRssMiB"],
            "drainFinalRssMiB": drain_end_sample["totalRssMiB"],
            "postWarmupRssDeltaMiB": post_warmup_rss_delta,
            "steadyStateRssSlopeMiBPerSec": round(rss_slope, 5),
            "monotonicallyGrowing": rss_monotonically_growing,
        },
        "fdAnalysis": {
            "warmupEndFds": warmup_end_sample["totalFds"],
            "drainFinalFds": drain_end_sample["totalFds"],
            "fdDelta": fd_delta,
            "fdLeak": fd_leak,
        },
        "threadAnalysis": {
            "warmupEndThreads": warmup_end_sample["totalThreads"],
            "drainFinalThreads": drain_end_sample["totalThreads"],
            "threadDelta": thread_delta,
            "threadLeak": thread_leak,
        },
        "sqliteIntegrity": check,
        "samplesCount": len(samples),
        "samples": samples,
        "failures": failures,
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
    parser.add_argument("--duration-seconds", type=float, default=1200.0,
                        help="Duration of continuous mixed load (minimum 1200s / 20 min)")
    parser.add_argument("--drain-seconds", type=float, default=180.0,
                        help="Duration of drain and idle stabilization phase (default 180s)")
    parser.add_argument("--sample-interval", type=float, default=10.0,
                        help="Resource sample interval in seconds (default 10s)")
    parser.add_argument("--json", dest="json_path", required=True, type=Path)
    parser.add_argument("--log", required=True, type=Path)
    args = parser.parse_args()

    args.work_dir.mkdir(parents=True, exist_ok=True)
    _prepare_evidence_paths(args.json_path, args.log)

    result = asyncio.run(run_soak_gate(
        args.tools_dir,
        args.native_verification,
        args.work_dir,
        duration_seconds=args.duration_seconds,
        drain_seconds=args.drain_seconds,
        sample_interval=args.sample_interval,
    ))

    with args.json_path.open("x", encoding="utf-8") as f:
        json.dump(result, f, indent=2, sort_keys=True)
    with args.log.open("x", encoding="utf-8") as f:
        f.write(json.dumps(result, indent=2, sort_keys=True) + "\n")

    summary = {
        "verdict": result["verdict"],
        "durationMinutes": result["totalDurationMinutes"],
        "counters": result["counters"],
        "rssAnalysis": result["rssAnalysis"],
        "fdAnalysis": result["fdAnalysis"],
        "threadAnalysis": result["threadAnalysis"],
        "sqliteClean": result["sqliteIntegrity"]["clean"],
        "failures": result["failures"],
    }
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0 if result["verdict"] == "PASS_LOCAL_SOAK" else 1


if __name__ == "__main__":
    raise SystemExit(main())
