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

from .configuration import read_file
from .domain import require, token

APP_ID = "a2amq-local"
CLIENT_TOKEN_LIFETIME_SECONDS = 3600
CLIENT_TOKEN_REFRESH_SECONDS = 1800
SUPERVISOR_INTERVAL_SECONDS = 1.0


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


def _private_replace(path: Path, value: str) -> None:
    """Atomically refresh an owner-private generated credential."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + "." + secrets.token_hex(8) + ".tmp")
    fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as stream:
            stream.write(value)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(tmp, path)
        os.chmod(path, 0o600)
    finally:
        try:
            tmp.unlink()
        except FileNotFoundError:
            pass


def _refresh_client_tokens(root: Path, profile: dict) -> None:
    """Issue short-lived client tokens on every start so persistent profiles do not expire."""
    try:
        import jwt
        from cryptography.hazmat.primitives.serialization import load_pem_private_key
        from cryptography.hazmat.primitives.asymmetric.rsa import RSAPrivateKey
    except ImportError:
        raise RuntimeError("local runtime extra is required: pip install -e '.[local]'") from None

    key = load_pem_private_key(read_file(str(root / "jwt-private.pem"), secret=True, limit=65536).encode(), password=None)
    require(isinstance(key, RSAPrivateKey), "INVALID_LOCAL_IDENTITY_KEY")
    now = datetime.now(timezone.utc)
    claims = {
        "iss": profile["issuer"], "sub": "local-owner", "tenant": token(profile["tenant"]),
        "scope": "a2a:send a2a:read a2a:cancel a2a:operate a2a:replay a2a:approve",
        "iat": int(now.timestamp()), "nbf": int(now.timestamp()),
        "exp": int((now + timedelta(seconds=CLIENT_TOKEN_LIFETIME_SECONDS)).timestamp()),
    }
    public_url = _loopback_public_url(profile["publicUrl"])
    encoded = jwt.encode({**claims, "aud": public_url}, key, algorithm="RS256",
                         headers={"kid": "local", "typ": "JWT"})
    _private_replace(root / "client.jwt", encoded)
    if profile.get("mcp"):
        encoded_mcp = jwt.encode({**claims, "aud": public_url + "/mcp"}, key, algorithm="RS256",
                                headers={"kid": "local", "typ": "JWT"})
        _private_replace(root / "client-mcp.jwt", encoded_mcp)


def _load_profile(root: Path, expected_mode: str) -> dict:
    require(root.is_dir() and not root.is_symlink(), "PROFILE_NOT_FOUND")
    info = root.stat()
    require(info.st_mode & 0o077 == 0, "UNSAFE_LOCAL_PROFILE")
    raw = read_file(str(root / "profile.json"), limit=16384)
    try:
        profile = json.loads(raw)
    except (TypeError, ValueError):
        require(False, "INVALID_LOCAL_PROFILE")
    require(isinstance(profile, dict) and profile.get("mode") == expected_mode,
            "LOCAL_PROFILE_MODE_MISMATCH")
    require(profile.get("appId") == APP_ID and profile.get("appPort") == 18000
            and profile.get("daprHttpPort") == 3500 and profile.get("daprGrpcPort") == 50001,
            "INVALID_LOCAL_PROFILE")
    token(profile.get("tenant"))
    token(profile.get("source"))
    _loopback_public_url(profile.get("publicUrl"))
    require(isinstance(profile.get("issuer"), str) and profile["issuer"] == "https://local-a2amq.invalid",
            "INVALID_LOCAL_PROFILE")
    require(type(profile.get("mcp")) is bool, "INVALID_LOCAL_PROFILE")
    return profile


def _plan(tenant: str, source: str) -> dict:
    tenant, source = token(tenant), token(source)
    return {"stream": {"name": "A2A_MQ", "subjects": [f"a2a.v1.{tenant}.>"],
        "retention": "limits", "storage": "file", "num_replicas": 1,
        "max_age": 604800000000000, "max_bytes": 67108864, "max_msgs": 50000,
        "max_msg_size": 1048576, "discard": "new"}}


def _nats_config(store_dir: Path, nats_token: str) -> str:
    """Bound the single-node JetStream server independently of stream policy."""
    return f'''host: 127.0.0.1
port: 4222
server_name: a2amq-local
jetstream {{
  store_dir: {json.dumps(str(store_dir))}
  max_mem_store: 16MB
  max_file_store: 96MB
}}
authorization {{ token: {json.dumps(nats_token)} }}
'''


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


def _resiliency_resource() -> str:
    """Retry transient app-delivery failures before the configured dead-letter topic."""
    return f'''apiVersion: dapr.io/v1alpha1
kind: Resiliency
metadata:
  name: local-pubsub-retry
scopes:
  - {APP_ID}
spec:
  policies:
    retries:
      localPubsubRetry:
        policy: constant
        duration: 500ms
        maxRetries: 5
  targets:
    components:
      bus:
        inbound:
          retry: localPubsubRetry
'''


def _workflow_state_component(path: Path, *, app_id: str = "a2amq-workflow") -> str:
    token(app_id)
    return f'''apiVersion: dapr.io/v1alpha1
kind: Component
metadata:
  name: workflow-state
scopes:
  - {app_id}
spec:
  type: state.sqlite
  version: v1
  metadata:
    - name: connectionString
      value: {json.dumps(str(path))}
    - name: actorStateStore
      value: "true"
'''


def _loopback_public_url(value: str) -> str:
    """Validate the URL actually served by the built-in native launcher."""
    try:
        parsed = urlsplit(value)
        require(parsed.scheme == "http"
                and parsed.hostname in {"127.0.0.1", "localhost"}
                and parsed.port == 18000
                and parsed.path in {"", "/"} and not parsed.username and not parsed.password
                and not parsed.query and not parsed.fragment, "LOCAL_LOOPBACK_URL_REQUIRED")
    except (TypeError, ValueError):
        require(False, "LOCAL_LOOPBACK_URL_REQUIRED")
    return value.rstrip("/")


def prepare(root: Path, *, tenant: str = "demo", source: str = "local-worker",
            public_url: str = "http://127.0.0.1:18000", mcp: bool = False,
            mode: str = "local-lite") -> dict:
    final_root = root.absolute()
    require(not final_root.exists(), "OUTPUT_ALREADY_EXISTS")
    require(mode in {"local-lite", "local-workflow"}, "INVALID_LOCAL_PROFILE")
    tenant, source = token(tenant), token(source)
    public_url = _loopback_public_url(public_url)
    staging = final_root.with_name(final_root.name + "." + secrets.token_hex(8) + ".tmp")
    require(not staging.exists(), "OUTPUT_ALREADY_EXISTS")
    staging.mkdir(parents=True, mode=0o700)
    root = staging
    try:
        for name in ("components", "examples", "state", "nats"):
            (root / name).mkdir(mode=0o700)
    
        app_token = secrets.token_hex(32)
        dapr_token = secrets.token_hex(32)
        nats_token = secrets.token_hex(32)
        _private_write(root / "app-api-token", app_token)
        _private_write(root / "dapr-api-token", dapr_token)
        _private_write(root / "nats-token", nats_token)
    
        nats_store = (final_root / "nats").resolve()
        _private_write(root / "nats.conf", _nats_config(nats_store, nats_token))
        _private_write(root / "components/bus.yaml", _bus_component(tenant, source, nats_token))
        _private_write(root / "components/bus-dlq.yaml", _dlq_component(tenant, nats_token))
        _private_write(root / "components/resiliency.yaml", _resiliency_resource())
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
                       "exp": int((now + timedelta(seconds=CLIENT_TOKEN_LIFETIME_SECONDS)).timestamp())}
        token_a2a = jwt.encode({**base_claims, "aud": public_url}, key, algorithm="RS256",
                               headers={"kid": "local", "typ": "JWT"})
        _private_write(root / "client.jwt", token_a2a)
        if mcp:
            token_mcp = jwt.encode({**base_claims, "aud": public_url.rstrip("/") + "/mcp"}, key,
                                   algorithm="RS256", headers={"kid": "local", "typ": "JWT"})
            _private_write(root / "client-mcp.jwt", token_mcp)
    
        workflow_state = _workflow_state_component(
            (final_root / "state/dapr-workflow.db").resolve(),
            app_id=APP_ID if mode == "local-workflow" else "a2amq-workflow",
        )
        if mode == "local-workflow":
            _private_write(root / "components/workflow-state.yaml", workflow_state)
            for name in ("scheduler", "scheduler-identity"):
                (root / "state" / name).mkdir(mode=0o700)
            process_budget = ["a2a-mq", "daprd", "nats-server", "placement", "scheduler"]
        else:
            # Documentation/example only; local-lite never loads an actor state component.
            _private_write(root / "examples/workflow-state.yaml", workflow_state)
            process_budget = ["a2a-mq", "daprd", "nats-server"]
    
        config = {"mode": mode, "tenant": tenant, "source": source,
                  "publicUrl": public_url, "issuer": issuer,
                  "mcp": mcp, "appId": APP_ID, "appPort": 18000, "daprHttpPort": 3500,
                  "daprGrpcPort": 50001, "processBudget": process_budget}
        _json_write(root / "profile.json", config)
        os.replace(root, final_root)
        return config
    except BaseException:
        shutil.rmtree(root, ignore_errors=True)
        raise


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
    return read_file(str(path), secret=True, limit=512).strip()


def _runtime_environments(root: Path, profile: dict) -> tuple[dict[str, str], dict[str, str], dict[str, str]]:
    """Return app, sidecar and unprivileged child environments with least-privilege secrets."""
    base = os.environ.copy()
    base.pop("APP_API_TOKEN", None)
    base.pop("DAPR_API_TOKEN", None)
    base["PYTHONNOUSERSITE"] = "1"

    app = base.copy()
    app.update({
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
    })

    sidecar = base.copy()
    sidecar.update({
        "APP_API_TOKEN": _read(root / "app-api-token"),
        "DAPR_API_TOKEN": _read(root / "dapr-api-token"),
    })
    return app, sidecar, base


def _native_binary(name: str) -> str | None:
    found = shutil.which(name)
    if found:
        return found
    if name in {"daprd", "placement", "scheduler"}:
        candidate = Path.home() / ".dapr" / "bin" / name
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
    """Run exactly three product processes: this Python app, daprd and nats-server."""
    profile = _load_profile(root, "local-lite")
    # Profiles are durable, while access tokens are deliberately short-lived.
    # Refresh generated client credentials before checking runtime prerequisites.
    _refresh_client_tokens(root, profile)

    nats_server = _native_binary("nats-server")
    daprd = _native_binary("daprd")
    if not nats_server or not daprd:
        missing = [name for name, value in (("nats-server", nats_server), ("daprd", daprd)) if not value]
        raise RuntimeError("missing native prerequisite(s): " + ", ".join(missing))

    app_env, sidecar_env, child_env = _runtime_environments(root, profile)

    # The current process becomes the ASGI application. Only NATS and daprd are
    # child processes, so the steady-state product process count is exactly three.
    os.environ.pop("APP_API_TOKEN", None)
    os.environ.pop("DAPR_API_TOKEN", None)
    os.environ.update(app_env)
    nats = sidecar = None
    try:
        nats = subprocess.Popen([nats_server, "-c", str(root / "nats.conf")], cwd=root, env=child_env)
        _wait_port("127.0.0.1", 4222, nats)
        # Provisioning is deliberately short-lived so nats-py and its client state
        # are not retained in the steady-state A2A application process.
        subprocess.run([
            sys.executable, "-m", "a2a_mq.provision",
            "--nats-url", "nats://127.0.0.1:4222",
            "--password-file", str(root / "nats-token"),
            "--plan", str(root / "broker-plan.json"),
            "--development",
        ], cwd=root, env=child_env, check=True, stdout=subprocess.DEVNULL)

        sidecar_command = [
            daprd, "--mode", "standalone", "--app-id", APP_ID, "--app-port", "18000",
            "--app-protocol", "http", "--dapr-http-port", "3500", "--dapr-grpc-port", "50001",
            "--resources-path", str((root / "components").resolve()),
            "--dapr-listen-addresses", "127.0.0.1",
            "--placement-host-address", " ", "--scheduler-host-address", " ",
            "--app-max-concurrency", "2", "--max-body-size", "1Mi",
            "--enable-metrics=false", "--log-level", "warn",
        ]
        sidecar = subprocess.Popen(sidecar_command, cwd=root, env=sidecar_env)
        _wait_port("127.0.0.1", 3500, sidecar)

        import uvicorn
        print(json.dumps({"status": "starting", "profile": "local-lite",
                          "processes": {"app": os.getpid(), "daprd": sidecar.pid, "nats": nats.pid},
                          "url": profile["publicUrl"]}, sort_keys=True), flush=True)
        config = uvicorn.Config("a2a_mq.local_lite:from_env", factory=True,
                                host="127.0.0.1", port=18000, workers=1, log_level="warning")
        server = uvicorn.Server(config)

        async def serve() -> int:
            task = asyncio.create_task(server.serve(), name="a2amq-local-asgi")
            next_token_refresh = time.monotonic() + CLIENT_TOKEN_REFRESH_SECONDS
            try:
                while not task.done():
                    for name, process in (("daprd", sidecar), ("nats", nats)):
                        code = process.poll()
                        if code is not None:
                            print(json.dumps({"status": "failed", "process": name, "exitCode": code}),
                                  file=sys.stderr, flush=True)
                            server.should_exit = True
                            await task
                            return code or 1
                    now = time.monotonic()
                    if now >= next_token_refresh:
                        _refresh_client_tokens(root, profile)
                        next_token_refresh = now + CLIENT_TOKEN_REFRESH_SECONDS
                    await asyncio.sleep(SUPERVISOR_INTERVAL_SECONDS)
                await task
                return 0
            finally:
                if not task.done():
                    server.should_exit = True
                    await task

        return asyncio.run(serve())
    finally:
        _terminate(sidecar)
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
