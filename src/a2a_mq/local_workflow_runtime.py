"""Optional five-process native local Workflow launcher.

Steady state: Python A2A-MQ + daprd + nats-server + placement + scheduler.
Application state and Dapr Workflow state are separate SQLite files. No Docker,
PostgreSQL or Redis is introduced by this tier.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
from pathlib import Path
import subprocess
import sys
import time

from .domain import require
from .local_runtime import (
    APP_ID,
    _native_binary,
    _read,
    _terminate,
    _wait_port,
    prepare as prepare_local,
)


PLACEMENT_PORT = 50005
SCHEDULER_PORT = 50006
PLACEMENT_RAFT_PORT = 50055
PLACEMENT_HEALTH_PORT = 58080
PLACEMENT_METRICS_PORT = 58090
SCHEDULER_HEALTH_PORT = 58081
SCHEDULER_METRICS_PORT = 58091
SCHEDULER_ETCD_CLIENT_PORT = 52379
SCHEDULER_ETCD_PEER_PORT = 52380


def prepare(root: Path, *, tenant: str = "demo", source: str = "local-worker",
            public_url: str = "http://127.0.0.1:18000", mcp: bool = False) -> dict:
    """Prepare an isolated Workflow profile without rewriting any generated file."""
    return prepare_local(root, tenant=tenant, source=source, public_url=public_url,
                         mcp=mcp, mode="local-workflow")


def _placement_command(binary: str) -> list[str]:
    """Single-node loopback Placement command matching Dapr 1.18 integration flags."""
    node = "a2amq-local-placement"
    return [
        binary,
        "--log-level=warn",
        f"--id={node}",
        f"--port={PLACEMENT_PORT}",
        "--listen-address=127.0.0.1",
        f"--healthz-port={PLACEMENT_HEALTH_PORT}",
        "--healthz-listen-address=127.0.0.1",
        f"--metrics-port={PLACEMENT_METRICS_PORT}",
        "--metrics-listen-address=127.0.0.1",
        f"--initial-cluster={node}=127.0.0.1:{PLACEMENT_RAFT_PORT}",
        "--tls-enabled=false",
        "--metadata-enabled=false",
    ]


def _scheduler_command(binary: str, root: Path) -> list[str]:
    """Single-node loopback Scheduler command matching Dapr 1.18 integration flags."""
    node = "a2amq-local-scheduler"
    data = (root / "state/scheduler").resolve()
    identity = (root / "state/scheduler-identity").resolve()
    return [
        binary,
        "--log-level=warn",
        f"--id={node}",
        f"--port={SCHEDULER_PORT}",
        f"--healthz-port={SCHEDULER_HEALTH_PORT}",
        f"--metrics-port={SCHEDULER_METRICS_PORT}",
        f"--etcd-data-dir={data}",
        f"--etcd-client-port={SCHEDULER_ETCD_CLIENT_PORT}",
        "--listen-address=127.0.0.1",
        f"--identity-directory-write={identity}",
        "--etcd-backend-batch-interval=50ms",
        f"--etcd-initial-cluster={node}=http://127.0.0.1:{SCHEDULER_ETCD_PEER_PORT}/",
        f"--override-broadcast-host-port=127.0.0.1:{SCHEDULER_PORT}",
    ]


def _environment(root: Path, profile: dict) -> dict[str, str]:
    env = os.environ.copy()
    env.update({
        "APP_API_TOKEN": _read(root / "app-api-token"),
        "DAPR_API_TOKEN": _read(root / "dapr-api-token"),
        "APP_API_TOKEN_FILE": str((root / "app-api-token").resolve()),
        "DAPR_API_TOKEN_FILE": str((root / "dapr-api-token").resolve()),
        "A2A_SQLITE_PATH": str((root / "state/a2amq.db").resolve()),
        "A2A_JWT_KEYS_FILE": str((root / "jwt-keys.json").resolve()),
        "A2A_JWT_ISSUER": profile["issuer"],
        "A2A_PUBLIC_URL": profile["publicUrl"],
        "A2A_TENANT": profile["tenant"],
        "A2A_CONSUMER": "local",
        "A2A_SOURCE": profile["source"],
        "A2A_COMPONENT": "bus",
        "A2A_ENABLE_MCP": "1" if profile.get("mcp") else "0",
        "A2A_CONCURRENCY": "2",
        "A2A_DATABASE_CAPACITY": "4",
        "DAPR_GRPC_PORT": "50001",
        "PYTHONNOUSERSITE": "1",
    })
    return env


def run(root: Path) -> int:
    require(root.is_dir(), "PROFILE_NOT_FOUND")
    profile = json.loads((root / "profile.json").read_text(encoding="utf-8"))
    require(profile.get("mode") == "local-workflow", "WORKFLOW_PROFILE_REQUIRED")

    binaries = {name: _native_binary(name) for name in ("nats-server", "daprd", "placement", "scheduler")}
    missing = [name for name, value in binaries.items() if not value]
    if missing:
        raise RuntimeError("missing native prerequisite(s): " + ", ".join(missing))

    env = _environment(root, profile)
    os.environ.update(env)
    nats = placement = scheduler = sidecar = None
    try:
        nats = subprocess.Popen([binaries["nats-server"], "-c", str(root / "nats.conf")],
                                cwd=root, env=env)
        _wait_port("127.0.0.1", 4222, nats)

        subprocess.run([
            sys.executable, "-m", "a2a_mq.provision",
            "--nats-url", "nats://127.0.0.1:4222",
            "--password-file", str(root / "nats-token"),
            "--plan", str(root / "broker-plan.json"),
            "--development",
        ], cwd=root, env=env, check=True, stdout=subprocess.DEVNULL)

        placement = subprocess.Popen(_placement_command(binaries["placement"]), cwd=root, env=env)
        _wait_port("127.0.0.1", PLACEMENT_PORT, placement)

        scheduler = subprocess.Popen(_scheduler_command(binaries["scheduler"], root), cwd=root, env=env)
        _wait_port("127.0.0.1", SCHEDULER_PORT, scheduler)

        sidecar_command = [
            binaries["daprd"], "--mode", "standalone", "--app-id", APP_ID,
            "--app-port", "18000", "--app-protocol", "http",
            "--dapr-http-port", "3500", "--dapr-grpc-port", "50001",
            "--resources-path", str((root / "components").resolve()),
            "--dapr-listen-addresses", "127.0.0.1",
            "--placement-host-address", f"127.0.0.1:{PLACEMENT_PORT}",
            "--scheduler-host-address", f"127.0.0.1:{SCHEDULER_PORT}",
            "--app-max-concurrency", "4", "--max-body-size", "1Mi",
            "--enable-metrics=false", "--log-level", "warn",
        ]
        sidecar = subprocess.Popen(sidecar_command, cwd=root, env=env)
        _wait_port("127.0.0.1", 3500, sidecar)

        import uvicorn
        children = {
            "daprd": sidecar,
            "nats": nats,
            "placement": placement,
            "scheduler": scheduler,
        }
        print(json.dumps({
            "status": "starting",
            "profile": "local-workflow",
            "processes": {"app": os.getpid(), **{name: proc.pid for name, proc in children.items()}},
            "url": profile["publicUrl"],
        }, sort_keys=True), flush=True)

        config = uvicorn.Config(
            "a2a_mq.local_workflow:from_env",
            factory=True,
            host="127.0.0.1",
            port=18000,
            workers=1,
            log_level="warning",
        )
        server = uvicorn.Server(config)

        async def serve() -> int:
            task = asyncio.create_task(server.serve(), name="a2amq-local-workflow-asgi")
            try:
                while not task.done():
                    for name, process in children.items():
                        code = process.poll()
                        if code is not None:
                            print(json.dumps({"status": "failed", "process": name, "exitCode": code}),
                                  file=sys.stderr, flush=True)
                            server.should_exit = True
                            await task
                            return code or 1
                    await asyncio.sleep(.25)
                await task
                return 0
            finally:
                if not task.done():
                    server.should_exit = True
                    await task

        return asyncio.run(serve())
    finally:
        _terminate(sidecar)
        _terminate(scheduler)
        _terminate(placement)
        _terminate(nats)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    prep = commands.add_parser("prepare")
    prep.add_argument("root", type=Path)
    prep.add_argument("--tenant", default="demo")
    prep.add_argument("--source", default="local-worker")
    prep.add_argument("--public-url", default="http://127.0.0.1:18000")
    prep.add_argument("--mcp", action="store_true")
    launch = commands.add_parser("run")
    launch.add_argument("root", type=Path)
    args = parser.parse_args()
    if args.command == "prepare":
        print(json.dumps(prepare(args.root, tenant=args.tenant, source=args.source,
                                 public_url=args.public_url, mcp=args.mcp), sort_keys=True))
        return
    raise SystemExit(run(args.root))


if __name__ == "__main__":
    main()
