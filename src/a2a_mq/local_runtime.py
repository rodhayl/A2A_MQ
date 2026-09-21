"""Prepare and run the lightweight native local profile without Docker/PostgreSQL/Redis."""
from __future__ import annotations

import argparse
import asyncio
from datetime import datetime, timedelta, timezone
import json
import os
from pathlib import Path
import secrets
import shutil
import signal
import socket
import subprocess
import sys
import time
from urllib.parse import urlsplit

from .domain import require, token
from .provision import jetstream

APP_ID = "a2amq-local"


def _private_write(path: Path, value: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(fd, "w", encoding="utf-8") as stream:
        stream.write(value)
        stream.flush()
        os.fsync(stream.fileno())


def _json_write(path: Path, value, *, private: bool = False) -> None:
    raw = json.dumps(value, indent=2, sort_keys=True) + "\n"
    if private:
        _private_write(path, raw)
    else:
        path.parent.mkdir(parents=True, exist_ok=True)
        fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o644)
        with os.fdopen(fd, "w", encoding="utf-8") as stream:
            stream.write(raw)


def _plan(tenant: str, source: str) -> dict:
    tenant, source = token(tenant), token(source)
    return {"stream": {"name": "A2A_MQ", "subjects": [f"a2a.v1.{tenant}.>"],
        "retention": "limits", "storage": "file", "num_replicas": 1,
        "max_age": 604800000000000, "max_bytes": 268435456, "max_msgs": 100000,
        "max_msg_size": 1048576, "discard": "new"}}


def _bus_component(tenant: str, source: str, nats_token: str) -> str:
    command = f"a2a.v1.{tenant}.command.agent.{source}"
    event = f"a2a.v1.{tenant}.event.task"
    dlq = f"a2a.v1.{tenant}.dlq"
    topics = ",".join((command, event, dlq))
    return f'''apiVersion: dapr.io/v1alpha1
kind: Component
metadata:
  name: bus
scopes:
  - {APP_ID}
spec:
  type: pubsub.jetstream
  version: v1
  metadata:
    - name: natsURL
      value: nats://127.0.0.1:4222
    - name: token
      value: {nats_token}
    - name: name
      value: {APP_ID}-bus
    - name: streamName
      value: A2A_MQ
    - name: durableName
      value: a2a-{tenant}-local-command-v1
    - name: queueGroupName
      value: a2a-{tenant}-local-command-v1
    - name: ackPolicy
      value: explicit
    - name: deliverPolicy
      value: all
    - name: ackWait
      value: 60s
    - name: maxDeliver
      value: "20"
    - name: maxAckPending
      value: "8"
    - name: replicas
      value: "1"
    - name: memoryStorage
      value: "false"
    - name: allowedTopics
      value: {topics}
    - name: protectedTopics
      value: {topics}
    - name: publishingScopes
      value: {APP_ID}={topics}
    - name: subscriptionScopes
      value: {APP_ID}={command}
'''


def _dlq_component(tenant: str, nats_token: str) -> str:
    dlq = f"a2a.v1.{tenant}.dlq"
    return f'''apiVersion: dapr.io/v1alpha1
kind: Component
metadata:
  name: bus-dlq
scopes:
  - {APP_ID}
spec:
  type: pubsub.jetstream
  version: v1
  metadata:
    - name: natsURL
      value: nats://127.0.0.1:4222
    - name: token
      value: {nats_token}
    - name: name
      value: {APP_ID}-dlq
    - name: streamName
      value: A2A_MQ
    - name: durableName
      value: a2a-{tenant}-local-dlq-v1
    - name: queueGroupName
      value: a2a-{tenant}-local-dlq-v1
    - name: ackPolicy
      value: explicit
    - name: deliverPolicy
      value: all
    - name: ackWait
      value: 60s
    - name: maxDeliver
      value: "20"
    - name: maxAckPending
      value: "4"
    - name: replicas
      value: "1"
    - name: memoryStorage
      value: "false"
    - name: allowedTopics
      value: {dlq}
    - name: protectedTopics
      value: {dlq}
    - name: publishingScopes
      value: {APP_ID}=
    - name: subscriptionScopes
      value: {APP_ID}={dlq}
'''


def _workflow_state_component(path: Path) -> str:
    return f'''apiVersion: dapr.io/v1alpha1
kind: Component
metadata:
  name: workflow-state
scopes:
  - a2amq-workflow
spec:
  type: state.sqlite
  version: v1
  metadata:
    - name: connectionString
      value: {path}
    - name: actorStateStore
      value: "true"
'''


def _loopback_public_url(value: str) -> str:
    try:
        parsed = urlsplit(value)
        require(parsed.scheme in {"http", "https"}
                and parsed.hostname in {"127.0.0.1", "localhost", "::1"}
                and parsed.path in {"", "/"} and not parsed.username and not parsed.password
                and not parsed.query and not parsed.fragment, "LOCAL_LOOPBACK_URL_REQUIRED")
        if parsed.port is not None:
            require(1 <= parsed.port <= 65535, "LOCAL_LOOPBACK_URL_REQUIRED")
    except (TypeError, ValueError):
        require(False, "LOCAL_LOOPBACK_URL_REQUIRED")
    return value.rstrip("/")


def prepare(root: Path, *, tenant: str = "demo", source: str = "local-worker",
            public_url: str = "http://127.0.0.1:18000", mcp: bool = False) -> dict:
    require(not root.exists(), "OUTPUT_ALREADY_EXISTS")
    tenant, source = token(tenant), token(source)
    public_url = _loopback_public_url(public_url)
    root.mkdir(parents=True, mode=0o700)
    for name in ("components", "examples", "state", "nats"):
        (root / name).mkdir(mode=0o700)

    app_token = secrets.token_hex(32)
    dapr_token = secrets.token_hex(32)
    nats_token = secrets.token_hex(32)
    _private_write(root / "app-api-token", app_token)
    _private_write(root / "dapr-api-token", dapr_token)
    _private_write(root / "nats-token", nats_token)

    nats_store = (root / "nats").resolve()
    _private_write(root / "nats.conf", f'''host: 127.0.0.1
port: 4222
server_name: a2amq-local
jetstream {{
  store_dir: {json.dumps(str(nats_store))}
  max_mem_store: 32MB
  max_file_store: 256MB
}}
authorization {{ token: {json.dumps(nats_token)} }}
''')
    _private_write(root / "components/bus.yaml", _bus_component(tenant, source, nats_token))
    _private_write(root / "components/bus-dlq.yaml", _dlq_component(tenant, nats_token))
    plan = _plan(tenant, source)
    _json_write(root / "broker-plan.json", plan)

    try:
        import jwt
        from cryptography.hazmat.primitives import serialization
        from cryptography.hazmat.primitives.asymmetric import rsa
    except ImportError:
        raise RuntimeError("local runtime extra is required: pip install -e '.[local]'") from None
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    private = key.private_bytes(serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8,
                                serialization.NoEncryption()).decode()
    public = key.public_key().public_bytes(serialization.Encoding.PEM,
                                           serialization.PublicFormat.SubjectPublicKeyInfo).decode()
    _private_write(root / "jwt-private.pem", private)
    _json_write(root / "jwt-keys.json", {"local": public}, private=True)
    now = datetime.now(timezone.utc)
    scopes = "a2a:send a2a:read a2a:cancel a2a:operate a2a:replay a2a:approve"
    issuer = "https://local-a2amq.invalid"
    base_claims = {"iss": issuer, "sub": "local-owner", "tenant": tenant, "scope": scopes,
                   "iat": int(now.timestamp()), "nbf": int(now.timestamp()),
                   "exp": int((now + timedelta(hours=1)).timestamp())}
    token_a2a = jwt.encode({**base_claims, "aud": public_url}, key, algorithm="RS256",
                           headers={"kid": "local", "typ": "JWT"})
    _private_write(root / "client.jwt", token_a2a)
    if mcp:
        token_mcp = jwt.encode({**base_claims, "aud": public_url.rstrip("/") + "/mcp"}, key,
                               algorithm="RS256", headers={"kid": "local", "typ": "JWT"})
        _private_write(root / "client-mcp.jwt", token_mcp)

    # Prepared but not started by local-lite. This removes Redis from the optional
    # local Workflow tier while keeping Scheduler/Placement an explicit separate cost.
    _private_write(root / "examples/workflow-state.yaml",
                   _workflow_state_component((root / "state/dapr-workflow.db").resolve()))

    config = {"tenant": tenant, "source": source, "publicUrl": public_url, "issuer": issuer,
              "mcp": mcp, "appId": APP_ID, "appPort": 18000, "daprHttpPort": 3500,
              "daprGrpcPort": 50001, "processBudget": ["a2a-mq", "daprd", "nats-server"]}
    _json_write(root / "profile.json", config)
    return config


def _wait_port(host: str, port: int, process: subprocess.Popen, timeout: float = 15) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if process.poll() is not None:
            raise RuntimeError(f"process exited before {host}:{port} became ready")
        try:
            with socket.create_connection((host, port), timeout=.25):
                return
        except OSError:
            time.sleep(.1)
    raise RuntimeError(f"timeout waiting for {host}:{port}")


def _read(path: Path) -> str:
    return path.read_text(encoding="utf-8").strip()


def _native_binary(name: str) -> str | None:
    found = shutil.which(name)
    if found:
        return found
    if name == "daprd":
        candidate = Path.home() / ".dapr" / "bin" / "daprd"
        if candidate.is_file() and os.access(candidate, os.X_OK):
            return str(candidate)
    return None


def _terminate(process: subprocess.Popen | None) -> None:
    if process is None or process.poll() is not None:
        return
    process.send_signal(signal.SIGTERM)
    try:
        process.wait(timeout=10)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait(timeout=5)


def run(root: Path) -> int:
    require(root.is_dir(), "PROFILE_NOT_FOUND")
    nats_server = _native_binary("nats-server")
    daprd = _native_binary("daprd")
    if not nats_server or not daprd:
        missing = [name for name, value in (("nats-server", nats_server), ("daprd", daprd)) if not value]
        raise RuntimeError("missing native prerequisite(s): " + ", ".join(missing))

    profile = json.loads((root / "profile.json").read_text(encoding="utf-8"))
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
        "PYTHONNOUSERSITE": "1",
    })

    nats = app = sidecar = None
    try:
        nats = subprocess.Popen([nats_server, "-c", str(root / "nats.conf")], cwd=root, env=env)
        _wait_port("127.0.0.1", 4222, nats)
        asyncio.run(jetstream("nats://127.0.0.1:4222", str(root / "nats-token"),
                              json.loads((root / "broker-plan.json").read_text()), development=True))

        app = subprocess.Popen([sys.executable, "-m", "uvicorn", "a2a_mq.local_lite:from_env",
                                "--factory", "--host", "127.0.0.1", "--port", "18000",
                                "--workers", "1", "--log-level", "warning"],
                               cwd=Path.cwd(), env=env)
        _wait_port("127.0.0.1", 18000, app)

        sidecar_command = [
            daprd, "--mode", "standalone", "--app-id", APP_ID, "--app-port", "18000",
            "--app-protocol", "http", "--dapr-http-port", "3500", "--dapr-grpc-port", "50001",
            "--resources-path", str((root / "components").resolve()),
            "--dapr-listen-addresses", "127.0.0.1",
            "--placement-host-address", " ", "--scheduler-host-address", " ",
            "--app-max-concurrency", "4", "--max-body-size", "1Mi",
            "--enable-metrics=false", "--log-level", "warn",
        ]
        sidecar = subprocess.Popen(sidecar_command, cwd=root, env=env)
        _wait_port("127.0.0.1", 3500, sidecar)
        print(json.dumps({"status": "running", "profile": "local-lite",
                          "processes": {"app": app.pid, "daprd": sidecar.pid, "nats": nats.pid},
                          "url": profile["publicUrl"]}, sort_keys=True), flush=True)

        while True:
            for name, process in (("app", app), ("daprd", sidecar), ("nats", nats)):
                code = process.poll()
                if code is not None:
                    if code != 0:
                        print(json.dumps({"status": "failed", "process": name, "exitCode": code}),
                              file=sys.stderr, flush=True)
                        return code
                    return 0
            time.sleep(.5)
    finally:
        _terminate(sidecar)
        _terminate(app)
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
