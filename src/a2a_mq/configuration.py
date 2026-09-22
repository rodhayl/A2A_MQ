"""Explicit role configuration; secret values are never part of configuration repr."""
from __future__ import annotations

from dataclasses import dataclass
import os
import stat
from urllib.parse import urlsplit

from .domain import PermanentError, load_json, require, token


def read_file(path: str, *, secret: bool = False, limit: int = 65536) -> str:
    """Read a regular non-symlink file; private files require owner-only permissions."""
    try:
        fd = os.open(path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_NONBLOCK", 0))
        with os.fdopen(fd, "rb") as stream:
            info = os.fstat(stream.fileno())
            require(stat.S_ISREG(info.st_mode) and (not secret or info.st_mode & 0o077 == 0),
                    "UNSAFE_CONFIG_FILE")
            raw = stream.read(limit + 1)
        require(0 < len(raw) <= limit, "INVALID_CONFIG_FILE")
        return raw.decode("utf-8", errors="strict")
    except (OSError, UnicodeError):
        raise PermanentError("CONFIG_FILE_UNAVAILABLE") from None


def https_origin(value: str, code: str = "PUBLIC_HTTPS_URL_REQUIRED") -> str:
    try:
        url = urlsplit(value)
        require(url.scheme == "https" and bool(url.hostname) and url.path in {"", "/"}
                and not url.username and not url.password and not url.query and not url.fragment
                and "?" not in value and "#" not in value
                and not any(c.isspace() for c in value), code)
        if url.port is not None:
            require(1 <= url.port <= 65535, code)
    except (ValueError, TypeError):
        raise PermanentError(code) from None
    return value.rstrip("/")


def https_issuer(value: str) -> str:
    try:
        url = urlsplit(value)
        require(url.scheme == "https" and bool(url.hostname) and not url.username
                and not url.password and not url.query and not url.fragment
                and "?" not in value and "#" not in value
                and not any(c.isspace() for c in value), "HTTPS_ISSUER_REQUIRED")
        if url.port is not None:
            require(1 <= url.port <= 65535, "HTTPS_ISSUER_REQUIRED")
    except (ValueError, TypeError):
        raise PermanentError("HTTPS_ISSUER_REQUIRED") from None
    return value


@dataclass(frozen=True)
class Settings:
    role: str
    tenant: str
    consumer: str
    app_token_file: str
    dapr_token_file: str
    sqlite_path: str = ""
    postgres_dsn_file: str = ""
    source: str = "workflow-worker"
    component: str = "bus"
    dapr_http: str = "http://127.0.0.1:3500"
    dapr_grpc_port: int = 50001
    public_url: str = ""
    issuer: str = ""
    jwt_keys_file: str = ""
    mcp_enabled: bool = False
    development: bool = False
    crypto_file: str = ""
    otlp_endpoint: str = ""
    otlp_ca_file: str = ""
    max_active: int = 1000
    max_records: int = 100000
    callback_timeout: float = 30
    rpc_timeout: float = 5
    relay_interval: float = 1
    database_capacity: int = 16
    concurrency: int = 8
    subscription_queue: str = ""

    def __post_init__(self):
        require(self.role in {"api", "workflow", "observer", "operator"}, "INVALID_ROLE")
        for value in (self.tenant, self.consumer, self.source, self.component):
            token(value)
        require(bool(self.sqlite_path) != bool(self.postgres_dsn_file), "EXACTLY_ONE_STORAGE_BACKEND_REQUIRED")
        require(type(self.development) is bool and type(self.mcp_enabled) is bool, "INVALID_CONFIGURATION")
        require(self.development or bool(self.postgres_dsn_file), "PRODUCTION_POSTGRES_REQUIRED")
        require(self.app_token_file and self.dapr_token_file, "TOKEN_FILES_REQUIRED")
        require(type(self.dapr_grpc_port) is int and 1 <= self.dapr_grpc_port <= 65535, "INVALID_DAPR_PORT")
        try:
            url = urlsplit(self.dapr_http)
            require(url.scheme in {"http", "https"} and url.hostname in {"127.0.0.1", "localhost", "::1"}
                    and url.path in {"", "/"} and not url.username and not url.password
                    and not url.query and not url.fragment, "COLOCATED_DAPR_REQUIRED")
            if url.port is not None:
                require(1 <= url.port <= 65535, "INVALID_DAPR_PORT")
        except ValueError:
            raise PermanentError("COLOCATED_DAPR_REQUIRED") from None
        if self.role in {"api", "operator"}:
            https_origin(self.public_url)
            https_issuer(self.issuer)
            require(bool(self.jwt_keys_file), "IDENTITY_KEYS_REQUIRED")
        require(not self.mcp_enabled or self.role == "api", "MCP_API_ROLE_REQUIRED")
        require(type(self.max_active) is int and 1 <= self.max_active <= 1000000, "INVALID_QUOTA")
        require(type(self.max_records) is int and self.max_active <= self.max_records <= 10000000, "INVALID_QUOTA")
        for value, low, high in ((self.callback_timeout, .01, 300), (self.rpc_timeout, .01, 60),
                                  (self.relay_interval, .01, 60)):
            require(type(value) in {int, float} and low <= value <= high, "INVALID_TIMEOUT")
        require(type(self.database_capacity) is int and 1 <= self.database_capacity <= 128,
                "INVALID_STORAGE_CAPACITY")
        require(type(self.concurrency) is int and 1 <= self.concurrency <= 128, "INVALID_CONCURRENCY")
        require(not self.subscription_queue or (isinstance(self.subscription_queue, str)
                and len(self.subscription_queue) <= 200
                and all(c in "abcdefghijklmnopqrstuvwxyz0123456789.-" for c in self.subscription_queue)),
                "INVALID_SUBSCRIPTION_QUEUE")
        if self.otlp_endpoint:
            https_origin(self.otlp_endpoint, "OTLP_HTTPS_REQUIRED")

    @property
    def command_route(self):
        return f"a2a.v1.{self.tenant}.command.agent.{self.source}"

    @property
    def event_route(self):
        return f"a2a.v1.{self.tenant}.event.task"

    @classmethod
    def from_env(cls, env=None):
        env = os.environ if env is None else env

        def integer(name, default):
            value = env.get(name, str(default))
            require(isinstance(value, str) and value.isascii() and value.isdigit()
                    and len(value) <= 9, "INVALID_CONFIGURATION")
            return int(value)

        def boolean(name):
            value = env.get(name, "0")
            require(value in {"0", "1"}, "INVALID_CONFIGURATION")
            return value == "1"

        return cls(role=env.get("A2A_ROLE", ""), tenant=env.get("A2A_TENANT", "demo"),
            consumer=env.get("A2A_CONSUMER", "tasks"), source=env.get("A2A_SOURCE", "workflow-worker"),
            component=env.get("A2A_COMPONENT", "bus"), sqlite_path=env.get("A2A_SQLITE_PATH", ""),
            postgres_dsn_file=env.get("A2A_POSTGRES_DSN_FILE", ""),
            app_token_file=env.get("APP_API_TOKEN_FILE", ""), dapr_token_file=env.get("DAPR_API_TOKEN_FILE", ""),
            dapr_http=env.get("DAPR_HTTP_ENDPOINT", "http://127.0.0.1:3500"),
            dapr_grpc_port=integer("DAPR_GRPC_PORT", 50001),
            public_url=env.get("A2A_PUBLIC_URL", ""), issuer=env.get("A2A_JWT_ISSUER", ""),
            jwt_keys_file=env.get("A2A_JWT_KEYS_FILE", ""), mcp_enabled=boolean("A2A_ENABLE_MCP"),
            development=boolean("A2A_DEVELOPMENT"), crypto_file=env.get("A2A_CRYPTO_CONFIG", ""),
            otlp_endpoint=env.get("A2A_OTLP_ENDPOINT", ""), otlp_ca_file=env.get("A2A_OTLP_CA_FILE", ""),
            max_active=integer("A2A_MAX_ACTIVE", 1000), max_records=integer("A2A_MAX_RECORDS", 100000),
            concurrency=integer("A2A_CONCURRENCY", 8), database_capacity=integer("A2A_DATABASE_CAPACITY", 16),
            subscription_queue=env.get("A2A_SUBSCRIPTION_QUEUE", ""))


def verifier(settings, *, audience=None):
    from .identity import JWTVerifier
    keys = load_json(read_file(settings.jwt_keys_file))
    require(isinstance(keys, dict) and all(isinstance(k, str) and isinstance(v, str)
            for k, v in keys.items()), "INVALID_IDENTITY_CONFIG")
    return JWTVerifier(keys, issuer=settings.issuer,
                       audience=audience or settings.public_url.rstrip("/"))


def store(settings):
    from .postgres import open_store
    dsn = read_file(settings.postgres_dsn_file, secret=True, limit=8192).strip() if settings.postgres_dsn_file else None
    require(dsn is None or not any(c in dsn for c in "\x00\r\n"), "INVALID_POSTGRES_DSN")
    return open_store(tenant=settings.tenant, consumer=settings.consumer,
                      path=settings.sqlite_path or None, dsn=dsn, allow_insecure=settings.development)
