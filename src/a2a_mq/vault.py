"""Bounded Vault Transit CEK wrapping and file-token secret provider."""
from __future__ import annotations

import asyncio
from dataclasses import dataclass
import os
from pathlib import Path
import re
import stat
from typing import Callable

import httpx

from .configuration import https_origin
from .domain import PermanentError, RetryableError, load_json, require, token
from .protection import _b64, _decode


class FileToken:
    """Read a locally mounted secret afresh so rotation does not require a restart."""

    def __init__(self, path: str | Path):
        self.path = Path(path).absolute()

    def __call__(self) -> str:
        try:
            fd = os.open(self.path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
                         | getattr(os, "O_NONBLOCK", 0))
            with os.fdopen(fd, "rb") as stream:
                info = os.fstat(stream.fileno())
                require(stat.S_ISREG(info.st_mode) and info.st_mode & 0o077 == 0, "UNSAFE_SECRET_FILE")
                raw = stream.read(4097)
            require(0 < len(raw) <= 4096, "INVALID_SECRET")
            value = raw.decode("ascii").strip()
            require(value and all(33 <= ord(c) <= 126 for c in value), "INVALID_SECRET")
            return value
        except (OSError, UnicodeError):
            raise PermanentError("SECRET_UNAVAILABLE") from None


@dataclass(frozen=True)
class TransitKey:
    key_id: str
    name: str
    tenant: str
    receiver: str
    write_version: int
    min_read_version: int = 1

    def __post_init__(self):
        for value in (self.key_id, self.name, self.tenant, self.receiver):
            token(value)
        require(type(self.write_version) is int and type(self.min_read_version) is int
                and 1 <= self.min_read_version <= self.write_version, "INVALID_KEY_VERSION")


class VaultTransit:
    """Pinned Transit key names/scopes. Authentication and policy remain Vault-owned."""

    def __init__(self, url: str, token_provider: Callable[[], str], keys: list[TransitKey],
                 active: str, *, mount: str = "transit", namespace: str = "",
                 timeout: float = 5, client: httpx.AsyncClient | None = None):
        url = https_origin(url, "VAULT_HTTPS_REQUIRED")
        token(mount)
        require(type(timeout) in {int, float} and 0 < timeout <= 60, "INVALID_TIMEOUT")
        require(isinstance(namespace, str) and len(namespace) <= 256
                and all(33 <= ord(c) <= 126 for c in namespace), "INVALID_NAMESPACE")
        require(1 <= len(keys) <= 32 and len({k.key_id for k in keys}) == len(keys), "INVALID_KEYRING")
        self.keys = {k.key_id: k for k in keys}
        require(active in self.keys, "UNKNOWN_KEY")
        self.url, self.mount, self.namespace = url.rstrip("/"), mount, namespace
        self.active, self.token_provider, self.timeout = active, token_provider, timeout
        self.owned = client is None
        self.client = client or httpx.AsyncClient(trust_env=False, follow_redirects=False)

    def _key(self, key_id, tenant, receiver):
        item = self.keys.get(key_id)
        require(item is not None and (item.tenant, item.receiver) == (tenant, receiver), "KEY_NOT_AUTHORIZED")
        return item

    async def _call(self, operation, item, payload):
        secret = self.token_provider()
        require(isinstance(secret, str) and 0 < len(secret) <= 4096
                and all(33 <= ord(c) <= 126 for c in secret), "INVALID_SECRET")
        headers = {"X-Vault-Token": secret}
        if self.namespace:
            headers["X-Vault-Namespace"] = self.namespace
        try:
            async with asyncio.timeout(self.timeout):
                async with self.client.stream("POST", f"{self.url}/v1/{self.mount}/{operation}/{item.name}",
                                              json=payload, headers=headers, timeout=self.timeout,
                                              follow_redirects=False) as response:
                    if response.status_code in {408, 429} or response.status_code >= 500:
                        raise RetryableError("KMS_UNAVAILABLE")
                    require(response.status_code == 200, "KMS_REJECTED")
                    body = bytearray()
                    async for chunk in response.aiter_bytes():
                        require(len(body) + len(chunk) <= 32768, "KMS_RESPONSE_TOO_LARGE")
                        body.extend(chunk)
            result = load_json(bytes(body), 32768)
            require(isinstance(result, dict) and isinstance(result.get("data"), dict), "INVALID_KMS_RESPONSE")
            return result["data"]
        except (httpx.RequestError, TimeoutError):
            raise RetryableError("KMS_UNAVAILABLE") from None

    async def wrap(self, key: bytes, tenant: str, receiver: str) -> tuple[str, str]:
        item = self._key(self.active, tenant, receiver)
        require(isinstance(key, bytes) and len(key) == 32, "INVALID_DATA_KEY")
        data = await self._call("encrypt", item, {"plaintext": _b64(key), "key_version": item.write_version})
        wrapped = data.get("ciphertext")
        require(isinstance(wrapped, str) and len(wrapped) <= 8192
                and wrapped.startswith(f"vault:v{item.write_version}:"), "INVALID_KMS_RESPONSE")
        return item.key_id, wrapped

    async def unwrap(self, key_id: str, wrapped: str, tenant: str, receiver: str) -> bytes:
        item = self._key(key_id, tenant, receiver)
        require(isinstance(wrapped, str) and len(wrapped) <= 8192, "INVALID_WRAPPED_KEY")
        match = re.fullmatch(r"vault:v([1-9][0-9]{0,8}):[A-Za-z0-9+/=]+", wrapped)
        require(match is not None and item.min_read_version <= int(match[1]) <= item.write_version,
                "RETIRED_OR_UNKNOWN_KEY_VERSION")
        data = await self._call("decrypt", item, {"ciphertext": wrapped})
        key = _decode(data.get("plaintext"), 32)
        require(len(key) == 32, "INVALID_KMS_RESPONSE")
        return key

    async def close(self):
        if self.owned:
            await self.client.aclose()
