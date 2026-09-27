"""Prepare and run the lightweight native local profile without Docker/PostgreSQL/Redis."""
from __future__ import annotations

import argparse
import asyncio
from datetime import datetime, timedelta, timezone
import importlib.util
import json
import os
from pathlib import Path
import re
import secrets
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import time
from urllib.parse import urlsplit

from .configuration import read_file
from .domain import MODEL_CREDENTIAL, PermanentError, require, token

APP_ID = "a2amq-local"
CLIENT_TOKEN_LIFETIME_SECONDS = 3600
CLIENT_TOKEN_REFRESH_SECONDS = 1800
SUPERVISOR_INTERVAL_SECONDS = 1.0
DAPR_INTERNAL_GRPC_PORT = 50002
LOCAL_DAPR_VERSION = "1.18.3"
LOCAL_NATS_VERSION = "2.15.0"
LOCAL_LITE_PORTS = (18000, 3500, 50001, DAPR_INTERNAL_GRPC_PORT, 4222)


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
    assert isinstance(key, RSAPrivateKey)
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
            and profile.get("daprHttpPort") == 3500 and profile.get("daprGrpcPort") == 50001
            and profile.get("daprInternalGrpcPort", DAPR_INTERNAL_GRPC_PORT) == DAPR_INTERNAL_GRPC_PORT,
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
    parking = f"a2a.v1.{tenant}.parking"
    topics = ",".join((dlq, parking))
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
      value: {topics}
    - name: protectedTopics
      value: {topics}
    - name: publishingScopes
      value: {APP_ID}={parking}
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
      bus-dlq:
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
                and not parsed.query and not parsed.fragment
                and "?" not in value and "#" not in value
                and not any(c.isspace() for c in value), "LOCAL_LOOPBACK_URL_REQUIRED")
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
                  "daprGrpcPort": 50001, "daprInternalGrpcPort": DAPR_INTERNAL_GRPC_PORT,
                  "processBudget": process_budget}
        _json_write(root / "profile.json", config)
        os.replace(root, final_root)
        return config
    except BaseException:
        shutil.rmtree(root, ignore_errors=True)
        raise


def _port_available(port: int) -> bool:
    """Return whether the built-in launcher can bind one loopback TCP port."""
    require(type(port) is int and 1 <= port <= 65535, "INVALID_LOCAL_PORTS")
    probe = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    probe.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    try:
        probe.bind(("127.0.0.1", port))
        return True
    except OSError:
        return False
    finally:
        probe.close()


def _require_ports_free(ports: tuple[int, ...]) -> None:
    """Fail before secrets or child processes are touched when a fixed loopback port is occupied."""
    require(isinstance(ports, tuple) and bool(ports), "INVALID_LOCAL_PORTS")
    seen: set[int] = set()
    for port in ports:
        require(type(port) is int and 1 <= port <= 65535 and port not in seen, "INVALID_LOCAL_PORTS")
        seen.add(port)
        if not _port_available(port):
            raise RuntimeError(f"local loopback port unavailable: {port}") from None


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


def _read(path: Path, limit: int = 512) -> str:
    return read_file(str(path), secret=True, limit=limit).strip()


def _runtime_environments(root: Path, profile: dict) -> tuple[dict[str, str], dict[str, str], dict[str, str]]:
    """Return app, sidecar and helper environments with least-privilege secrets."""
    app = os.environ.copy()
    app.pop("APP_API_TOKEN", None)
    app.pop("DAPR_API_TOKEN", None)
    for name in tuple(app):
        if MODEL_CREDENTIAL.match(name):
            app.pop(name, None)
    app["A2AMQ_NO_LLM_TOKENS"] = "1"
    app["PYTHONNOUSERSITE"] = "1"

    safe_names = {"PATH", "HOME", "TMPDIR", "TMP", "TEMP", "LANG", "TZ",
                  "SSL_CERT_FILE", "SSL_CERT_DIR"}
    child = {name: value for name, value in os.environ.items()
             if name in safe_names or name.startswith("LC_")}
    child["PYTHONNOUSERSITE"] = "1"

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
        "DAPR_HTTP_ENDPOINT": "http://127.0.0.1:3500",
        "DAPR_GRPC_PORT": "50001",
        "DAPR_INTERNAL_GRPC_PORT": str(DAPR_INTERNAL_GRPC_PORT),
    })

    sidecar = child.copy()
    sidecar.update({
        "APP_API_TOKEN": _read(root / "app-api-token"),
        "DAPR_API_TOKEN": _read(root / "dapr-api-token"),
    })
    return app, sidecar, child



def _apply_app_environment(app: dict[str, str]) -> None:
    """Replace inherited application secrets that the local product never needs."""
    for name in tuple(os.environ):
        if name in {"APP_API_TOKEN", "DAPR_API_TOKEN"} or MODEL_CREDENTIAL.match(name):
            os.environ.pop(name, None)
    os.environ.update(app)

def _native_binary(name: str) -> str | None:
    found = shutil.which(name)
    if found:
        return str(Path(found).absolute())
    if name in {"daprd", "placement", "scheduler"}:
        candidate = Path.home() / ".dapr" / "bin" / name
        if candidate.is_file() and os.access(candidate, os.X_OK):
            return str(candidate)
    return None



def _native_version(name: str, path: str) -> str | None:
    """Probe only native binaries with a stable version CLI used by repository gates."""
    arguments = {"daprd": ("--version",), "nats-server": ("-v",)}.get(name)
    if arguments is None:
        return None
    try:
        completed = subprocess.run(
            [path, *arguments],
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=5,
            check=False,
        )
    except (OSError, subprocess.TimeoutExpired):
        return None
    if completed.returncode != 0:
        return None
    match = re.search(r"(?<![0-9])v?(\d+\.\d+\.\d+)(?![0-9])", completed.stdout[:4096])
    return match.group(1) if match else None


def _doctor_item(name: str, ok: bool, detail: str, *, fix: str = "",
                 warning: bool = False) -> dict:
    return {"name": name, "ok": bool(ok), "detail": detail, "fix": fix, "warning": bool(warning)}


def _bootstrap_provenance_check(binaries: dict[str, str], mode: str) -> dict | None:
    """Reuse the repository's exact provenance checker when PATH points at one bootstrap root."""
    parents = {Path(path).resolve().parent for path in binaries.values()}
    if len(parents) != 1:
        return None
    bin_dir = next(iter(parents))
    if bin_dir.name != "bin":
        return None
    tools_root = bin_dir.parent
    manifest = tools_root / "manifest.json"
    if not manifest.is_file():
        return None
    checker = Path(__file__).resolve().parents[2] / "scripts/check_local_native_versions.py"
    if not checker.is_file():
        return _doctor_item(
            "native provenance",
            True,
            f"bootstrap manifest found at {manifest}; exact checker unavailable in this installation",
            warning=True,
        )
    with tempfile.TemporaryDirectory(prefix="a2amq-doctor-") as tmp:
        report = Path(tmp) / "native.json"
        command = [
            sys.executable,
            str(checker),
            "--daprd", binaries["daprd"],
            "--nats-server", binaries["nats-server"],
            "--bootstrap-manifest", str(manifest),
            "--json", str(report),
        ]
        if mode == "local-workflow":
            command += [
                "--placement", binaries["placement"],
                "--scheduler", binaries["scheduler"],
            ]
        try:
            completed = subprocess.run(
                command,
                stdin=subprocess.DEVNULL,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                encoding="utf-8",
                errors="replace",
                timeout=30,
                check=False,
            )
            value = json.loads(completed.stdout.strip().splitlines()[-1])
        except (OSError, subprocess.TimeoutExpired, ValueError, IndexError):
            return _doctor_item(
                "native provenance",
                False,
                "bootstrap provenance checker could not complete",
                fix="rerun scripts/check_local_native_versions.py for the selected tools directory",
            )
    verdict = value.get("verdict")
    return _doctor_item(
        "native provenance",
        completed.returncode == 0 and verdict == "PASS_NATIVE_VERSION",
        str(verdict or "INCOMPLETE_NATIVE_VERSION"),
        fix="re-bootstrap the pinned native tools; do not reuse a modified tools directory",
    )


def _client_token_check(root: Path, profile: dict) -> dict:
    """Verify local token signature/identity; expiry is a warning because run refreshes it."""
    try:
        import jwt
        keys = json.loads(read_file(str(root / "jwt-keys.json"), secret=True, limit=65536))
        encoded = _read(root / "client.jwt", limit=65536)
        header = jwt.get_unverified_header(encoded)
        kid = header.get("kid")
        require(isinstance(keys, dict) and isinstance(kid, str) and kid in keys, "UNAUTHENTICATED")
        claims = jwt.decode(
            encoded,
            key=keys[kid],
            algorithms=["RS256"],
            issuer=profile["issuer"],
            audience=profile["publicUrl"],
            options={"require": ["exp", "iat", "nbf", "iss", "aud", "sub", "tenant", "scope"],
                     "verify_exp": False},
        )
        exp = claims.get("exp")
        require(type(exp) is int, "UNAUTHENTICATED")
        if exp <= int(time.time()):
            return _doctor_item(
                "client JWT",
                True,
                "signature/identity valid; token expired and will be refreshed by run",
                warning=True,
            )
        return _doctor_item("client JWT", True, "signature, issuer and audience valid")
    except Exception:
        return _doctor_item(
            "client JWT",
            False,
            "generated client token or public key is invalid",
            fix="recreate the local profile; do not hand-edit generated identity files",
        )


def doctor(root: Path | None = None, *, mode: str = "local-lite",
           native_names: tuple[str, ...] = ("daprd", "nats-server"),
           ports: tuple[int, ...] = LOCAL_LITE_PORTS) -> dict:
    """Non-mutating local preflight. It never installs tools, changes profiles or kills processes."""
    require(mode in {"local-lite", "local-workflow"}, "INVALID_LOCAL_PROFILE")
    checks: list[dict] = []
    checks.append(_doctor_item(
        "Python",
        sys.version_info >= (3, 12),
        f"{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}",
        fix="use Python 3.12 or newer",
    ))

    modules = ["a2a", "jwt", "cryptography", "nats"]
    if mode == "local-workflow":
        modules.append("dapr")
    missing_modules = [name for name in modules if importlib.util.find_spec(name) is None]
    checks.append(_doctor_item(
        "Python extras",
        not missing_modules,
        "installed" if not missing_modules else "missing: " + ", ".join(missing_modules),
        fix=("python -m pip install -e '.[local,local-workflow]'"
             if mode == "local-workflow" else "python -m pip install -e '.[local]'"),
    ))

    profile = None
    if root is not None:
        root = root.absolute()
        try:
            profile = _load_profile(root, mode)
            checks.append(_doctor_item(
                "profile",
                True,
                f"{mode}; tenant={profile['tenant']}; source={profile['source']}",
            ))
        except Exception as exc:
            code = getattr(exc, "code", type(exc).__name__)
            checks.append(_doctor_item(
                "profile",
                False,
                str(code),
                fix="prepare a fresh profile with the matching local launcher",
            ))

    binaries: dict[str, str] = {}
    for name in native_names:
        path = _native_binary(name)
        if path is None:
            checks.append(_doctor_item(
                name,
                False,
                "not found",
                fix="bootstrap the repository-pinned native tools and prepend its bin directory to PATH",
            ))
            continue
        binaries[name] = path
        expected = {"daprd": LOCAL_DAPR_VERSION, "nats-server": LOCAL_NATS_VERSION}.get(name)
        observed = _native_version(name, path)
        if expected is not None:
            checks.append(_doctor_item(
                name,
                observed == expected,
                f"{observed or 'version unavailable'} at {path}",
                fix=f"use repository-pinned {name} {expected}",
            ))
        else:
            checks.append(_doctor_item(name, True, path))

    if all(name in binaries for name in native_names):
        provenance = _bootstrap_provenance_check(binaries, mode)
        if provenance is not None:
            checks.append(provenance)
        else:
            checks.append(_doctor_item(
                "native provenance",
                True,
                "host binaries; exact bootstrap archive provenance not available",
                warning=True,
            ))

    occupied = [str(port) for port in ports if not _port_available(port)]
    checks.append(_doctor_item(
        "startup ports",
        not occupied,
        "available" if not occupied else "occupied: " + ", ".join(occupied),
        fix="stop the conflicting service or choose a different host before starting A2A-MQ",
    ))

    if root is not None and profile is not None:
        state = root / "state"
        writable = state.is_dir() and not state.is_symlink() and os.access(state, os.W_OK)
        checks.append(_doctor_item(
            "state directory",
            writable,
            str(state),
            fix="restore the generated profile directory and owner write permissions",
        ))
        checks.append(_client_token_check(root, profile))

    failed = [item for item in checks if not item["ok"]]
    return {
        "status": "ready" if not failed else "not-ready",
        "mode": mode,
        "profile": str(root) if root is not None else None,
        "checks": checks,
    }


def _print_doctor(report: dict) -> None:
    print(f"A2A-MQ doctor ({report['mode']})")
    for item in report["checks"]:
        label = "FAIL" if not item["ok"] else "WARN" if item.get("warning") else "PASS"
        print(f"{item['name']:<22} {label:<4}  {item['detail']}")
        if not item["ok"] and item.get("fix"):
            print(f"{'':22}       Fix: {item['fix']}")
    print("READY" if report["status"] == "ready" else "NOT_READY")


_CLI_CODE_HINTS = {
    "OUTPUT_ALREADY_EXISTS": "choose a different profile directory or remove the old profile after stopping it",
    "PROFILE_NOT_FOUND": "prepare the profile first and pass its directory to the launcher",
    "UNSAFE_LOCAL_PROFILE": "make the profile owner-private (chmod 700 on Linux) or prepare a fresh profile",
    "LOCAL_PROFILE_MODE_MISMATCH": "use a2amq-local for Local-lite or a2amq-local-workflow for Workflow",
    "INVALID_LOCAL_PROFILE": "prepare a fresh profile; generated profile files are not user-editable configuration",
    "LOCAL_LOOPBACK_URL_REQUIRED": "use the built-in loopback URL http://127.0.0.1:18000",
    "INVALID_ROUTE": "use lowercase token-safe tenant/source names",
    "CONFIG_FILE_UNAVAILABLE": "restore the generated file as a regular owner-controlled file",
    "INVALID_CONFIG_FILE": "recreate the profile instead of modifying generated files",
}


def _format_cli_error(exc: BaseException) -> str | None:
    """Render expected operator mistakes without hiding unexpected programming errors."""
    if isinstance(exc, PermanentError):
        code = exc.code
        hint = _CLI_CODE_HINTS.get(code, "see docs/TROUBLESHOOTING.md for this validation error")
        return f"ERROR: {code}\nFix: {hint}"
    if isinstance(exc, subprocess.TimeoutExpired):
        return "ERROR: local dependency startup/provisioning timed out\nFix: run a2amq-local doctor and inspect the failing native process"
    if isinstance(exc, RuntimeError):
        message = str(exc)
        if message.startswith("missing native prerequisite(s):"):
            return (
                f"ERROR: {message}\n"
                "Fix: bootstrap the pinned native tools and prepend their bin directory to PATH"
            )
        if message.startswith("local loopback port unavailable:"):
            port = message.rsplit(":", 1)[-1].strip()
            return (
                f"ERROR: local port {port} is already in use\n"
                f"Fix: inspect the owner with: ss -tulpn | grep ':{port}'"
            )
        if message.startswith("local runtime extra is required:"):
            return f"ERROR: {message}"
        if message.startswith("process exited before ") or message.startswith("timeout waiting for "):
            return f"ERROR: {message}\nFix: run a2amq-local doctor and inspect native process output"
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


def _server_exit_code(server) -> int:
    """Never report success when ASGI startup failed before becoming ready to serve."""
    return 0 if bool(getattr(server, "started", False)) else 1


def _lite_sidecar_command(daprd: str, root: Path) -> list[str]:
    """Run daprd without Actor/Workflow control-plane services in local-lite."""
    return [
        daprd, "--mode", "standalone", "--app-id", APP_ID, "--app-port", "18000",
        "--app-protocol", "http", "--dapr-http-port", "3500", "--dapr-grpc-port", "50001",
        "--dapr-internal-grpc-port", str(DAPR_INTERNAL_GRPC_PORT),
        "--dapr-internal-grpc-listen-address", "127.0.0.1",
        "--resources-path", str((root / "components").resolve()),
        "--dapr-listen-addresses", "127.0.0.1",
        "--placement-host-address=", "--scheduler-host-address=",
        "--app-max-concurrency", "2", "--max-body-size", "1Mi",
        "--enable-metrics=false", "--log-level", "warn",
    ]


def run(root: Path) -> int:
    """Run exactly three product processes: this Python app, daprd and nats-server."""
    root = root.absolute()
    profile = _load_profile(root, "local-lite")
    _require_ports_free(LOCAL_LITE_PORTS)
    nats_server = _native_binary("nats-server")
    daprd = _native_binary("daprd")
    if not nats_server or not daprd:
        missing = [name for name, value in (("nats-server", nats_server), ("daprd", daprd)) if not value]
        raise RuntimeError("missing native prerequisite(s): " + ", ".join(missing))

    # Profiles are durable, while access tokens are deliberately short-lived.
    # Refresh only after deterministic native prerequisites have passed.
    _refresh_client_tokens(root, profile)
    app_env, sidecar_env, child_env = _runtime_environments(root, profile)

    # The current process becomes the ASGI application. Only NATS and daprd are
    # child processes, so the steady-state product process count is exactly three.
    _apply_app_environment(app_env)
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
        ], cwd=root, env=child_env, check=True, stdout=subprocess.DEVNULL, timeout=30)

        sidecar_command = _lite_sidecar_command(daprd, root)
        sidecar = subprocess.Popen(sidecar_command, cwd=root, env=sidecar_env)
        _wait_port("127.0.0.1", 3500, sidecar)

        import uvicorn
        print(json.dumps({"status": "starting", "profile": "local-lite",
                          "processes": {"app": os.getpid(), "daprd": sidecar.pid, "nats": nats.pid},
                          "url": profile["publicUrl"],
                          "labUrl": profile["publicUrl"].rstrip("/") + "/lab/"}, sort_keys=True), flush=True)
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
                return _server_exit_code(server)
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
    diagnose = commands.add_parser("doctor", help="non-mutating local prerequisite/profile preflight")
    diagnose.add_argument("root", type=Path, nargs="?")
    args = parser.parse_args()
    try:
        if args.command == "prepare":
            print(json.dumps(prepare(args.root, tenant=args.tenant, source=args.source,
                                     public_url=args.public_url, mcp=args.mcp), sort_keys=True))
            return
        if args.command == "doctor":
            report = doctor(args.root)
            _print_doctor(report)
            raise SystemExit(0 if report["status"] == "ready" else 2)
        raise SystemExit(run(args.root))
    except (PermanentError, RuntimeError, subprocess.SubprocessError) as exc:
        message = _format_cli_error(exc)
        if message is None:
            raise
        print(message, file=sys.stderr)
        raise SystemExit(2) from None


if __name__ == "__main__":
    main()
