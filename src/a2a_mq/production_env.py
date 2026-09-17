"""Fail-closed environment factories for the deployable production roles."""
from __future__ import annotations

import asyncio
import os
from pathlib import Path
from typing import Mapping

from .central_dlq import CentralDLQ
from .dlq_service import create_dlq_collector
from .domain import PermanentError, require, token
from .identity import JWTVerifier
from .postgres import open_store
from .production import create_api_platform, create_workflow_worker
from .transport import DaprPublisher
from .vault import FileToken


def _value(env: Mapping[str, str], name: str, *, default: str | None = None,
           maximum: int = 8192) -> str:
    value = env.get(name, default)
    require(isinstance(value, str) and 0 < len(value) <= maximum and "\x00" not in value,
            "MISSING_OR_INVALID_CONFIG")
    return value


def _secret(env: Mapping[str, str], name: str, file_name: str, *, required: bool) -> str:
    direct, path = env.get(name), env.get(file_name)
    require(not (direct and path), "AMBIGUOUS_SECRET_CONFIG")
    if path:
        return FileToken(path)()
    if direct:
        require(0 < len(direct) <= 8192 and all(33 <= ord(c) <= 126 for c in direct),
                "INVALID_SECRET")
        return direct
    require(not required, "MISSING_OR_INVALID_CONFIG")
    return ""


def _bool(env: Mapping[str, str], name: str, default: bool = False) -> bool:
    raw = env.get(name)
    if raw is None:
        return default
    require(raw in {"true", "false"}, "INVALID_CONFIG")
    return raw == "true"


def _public_key(path: str) -> bytes:
    item = Path(path).absolute()
    try:
        require(item.is_file() and item.stat().st_size <= 65536, "INVALID_IDENTITY_KEY")
        return item.read_bytes()
    except OSError:
        raise PermanentError("INVALID_IDENTITY_KEY") from None


def _storage_config(env: Mapping[str, str]) -> tuple[str | None, str | None, bool]:
    sqlite_path = env.get("A2A_SQLITE_PATH")
    postgres_dsn = _secret(env, "A2A_POSTGRES_DSN", "A2A_POSTGRES_DSN_FILE", required=False)
    require(bool(sqlite_path) != bool(postgres_dsn), "EXACTLY_ONE_STORAGE_BACKEND_REQUIRED")
    if sqlite_path:
        require(0 < len(sqlite_path) <= 4096 and "\x00" not in sqlite_path, "INVALID_STORAGE_PATH")
    return sqlite_path, postgres_dsn or None, _bool(env, "A2A_POSTGRES_ALLOW_INSECURE", False)


def _stores(env: Mapping[str, str], *, tenant: str, task_consumer: str,
            dlq_consumer: str | None = None):
    sqlite_path, postgres_dsn, allow_insecure = _storage_config(env)

    def one(consumer: str):
        return open_store(tenant=tenant, consumer=consumer,
                          path=sqlite_path if sqlite_path else None,
                          dsn=postgres_dsn if postgres_dsn else None,
                          allow_insecure=allow_insecure)

    task = one(task_consumer)
    if dlq_consumer is None or dlq_consumer == task_consumer:
        return task, task
    try:
        return task, one(dlq_consumer)
    except BaseException:
        close = getattr(task, "close", None)
        if callable(close):
            close()
        raise


def _close_bus(bus) -> None:
    """Best-effort cleanup only for factory failures before lifespan ownership."""
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        asyncio.run(bus.close())
    else:
        loop.create_task(bus.close(), name="a2amq-failed-factory-bus-close")


def _bus(env: Mapping[str, str]) -> DaprPublisher:
    return DaprPublisher(
        _value(env, "DAPR_HTTP_ENDPOINT", default="http://127.0.0.1:3500"),
        component=token(_value(env, "A2A_PUBSUB_COMPONENT", default="bus")),
        api_token=_secret(env, "DAPR_API_TOKEN", "DAPR_API_TOKEN_FILE", required=False),
    )


def _identity(env: Mapping[str, str], audience: str) -> JWTVerifier:
    kid = token(_value(env, "A2A_JWT_KID"))
    pem = _public_key(_value(env, "A2A_JWT_PUBLIC_KEY_FILE"))
    return JWTVerifier({kid: pem}, issuer=_value(env, "A2A_JWT_ISSUER"), audience=audience)


def _close_stores(*stores) -> None:
    seen: set[int] = set()
    for store in stores:
        if id(store) in seen:
            continue
        seen.add(id(store))
        close = getattr(store, "close", None)
        if callable(close):
            close()


def create_api_from_env(env: Mapping[str, str] | None = None):
    env = os.environ if env is None else env
    tenant = token(_value(env, "A2A_TENANT", default="demo"))
    consumer = token(_value(env, "A2A_CONSUMER", default="workflow-worker"))
    dlq_consumer = token(_value(env, "A2A_DLQ_CONSUMER", default="operations"))
    public_url = _value(env, "A2A_PUBLIC_URL").rstrip("/")
    mcp_url = env.get("A2A_MCP_RESOURCE_URL", public_url + "/mcp").rstrip("/")
    # Validate identities and storage selection before allocating clients/connections.
    a2a_identity = _identity(env, public_url)
    mcp_identity = _identity(env, mcp_url)
    _storage_config(env)
    task_store, dlq_store = _stores(env, tenant=tenant, task_consumer=consumer,
                                    dlq_consumer=dlq_consumer)
    bus = _bus(env)
    try:
        return create_api_platform(
            task_store, dlq_store, bus,
            a2a_verifier=a2a_identity, mcp_verifier=mcp_identity,
            public_url=public_url, mcp_resource_url=mcp_url,
            backup_dir=env.get("A2A_BACKUP_DIR"),
        )
    except BaseException:
        _close_bus(bus)
        _close_stores(task_store, dlq_store)
        raise


def create_worker_from_env(env: Mapping[str, str] | None = None):
    env = os.environ if env is None else env
    tenant = token(_value(env, "A2A_TENANT", default="demo"))
    consumer = token(_value(env, "A2A_CONSUMER", default="workflow-worker"))
    app_token = _secret(env, "APP_API_TOKEN", "APP_API_TOKEN_FILE", required=True)
    source = token(_value(env, "A2A_SOURCE", default="workflow-worker"))
    _storage_config(env)
    store, _ = _stores(env, tenant=tenant, task_consumer=consumer)
    bus = _bus(env)
    try:
        return create_workflow_worker(store, bus, app_token=app_token, source=source)
    except BaseException:
        _close_bus(bus)
        _close_stores(store)
        raise


def create_dlq_from_env(env: Mapping[str, str] | None = None):
    env = os.environ if env is None else env
    tenant = token(_value(env, "A2A_TENANT", default="demo"))
    consumer = token(_value(env, "A2A_DLQ_CONSUMER", default="operations"))
    app_token = _secret(env, "APP_API_TOKEN", "APP_API_TOKEN_FILE", required=True)
    _storage_config(env)
    store, _ = _stores(env, tenant=tenant, task_consumer=consumer)
    bus = _bus(env)
    try:
        return create_dlq_collector(store, bus, CentralDLQ(store), app_token=app_token)
    except BaseException:
        _close_bus(bus)
        _close_stores(store)
        raise
