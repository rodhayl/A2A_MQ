"""Optional transport wrapper: library AES-256-GCM with per-message wrapped keys.

The plaintext Envelope contract is unchanged. Persist sealed bytes before sending
when ciphertext identity must also survive publication retries. This is not an
at-rest database encryption feature or an upstream standardized A2A binding.
"""
from __future__ import annotations

import base64
import os
from dataclasses import dataclass, field
from typing import Protocol

from .domain import MAX_BYTES, Envelope, PermanentError, canonical, load_json, require, token

SEALED_LIMIT = MAX_BYTES * 2
FIELDS = {"version", "algorithm", "keyId", "wrappedKey", "nonce", "tenant", "receiver", "route", "messageId"}


def _b64(raw: bytes) -> str:
    return base64.b64encode(raw).decode("ascii")


def _decode(value: str, limit: int) -> bytes:
    require(isinstance(value, str) and len(value) <= limit * 2 + 8, "INVALID_SEALED_ENVELOPE")
    try:
        data = base64.b64decode(value, validate=True)
    except (ValueError, TypeError):
        raise PermanentError("INVALID_SEALED_ENVELOPE") from None
    require(len(data) <= limit and _b64(data) == value, "INVALID_SEALED_ENVELOPE")
    return data


class KeyProvider(Protocol):
    async def wrap(self, key: bytes, tenant: str, receiver: str) -> tuple[str, str]: ...
    async def unwrap(self, key_id: str, wrapped: str, tenant: str, receiver: str) -> bytes: ...


@dataclass(frozen=True)
class LocalKey:
    key_id: str
    tenant: str
    receiver: str
    material: bytes = field(repr=False)

    def __post_init__(self):
        token(self.key_id)
        token(self.tenant)
        token(self.receiver)
        require(isinstance(self.material, bytes) and len(self.material) == 32, "INVALID_WRAPPING_KEY")


class LocalKeyRing:
    """Explicit development/test keyring; production keys belong in an authorized KMS.

    The active ID is used for new writes; retained IDs allow the rotation read
    window. Removing an ID retires it immediately. There is no fallback key.
    """

    def __init__(self, keys: list[LocalKey], active: str):
        require(1 <= len(keys) <= 32 and len({k.key_id for k in keys}) == len(keys), "INVALID_KEYRING")
        self.keys = {k.key_id: k for k in keys}
        require(active in self.keys, "UNKNOWN_KEY")
        self.active = active

    def _authorized(self, key_id, tenant, receiver):
        key = self.keys.get(key_id)
        require(key is not None and (key.tenant, key.receiver) == (tenant, receiver), "KEY_NOT_AUTHORIZED")
        return key

    async def wrap(self, key, tenant, receiver):
        from cryptography.hazmat.primitives.keywrap import aes_key_wrap
        item = self._authorized(self.active, tenant, receiver)
        return item.key_id, _b64(aes_key_wrap(item.material, key))

    async def unwrap(self, key_id, wrapped, tenant, receiver):
        from cryptography.hazmat.primitives.keywrap import InvalidUnwrap, aes_key_unwrap
        item = self._authorized(key_id, tenant, receiver)
        try:
            return aes_key_unwrap(item.material, _decode(wrapped, 64))
        except (InvalidUnwrap, ValueError):
            raise PermanentError("DECRYPTION_FAILED") from None


class EnvelopeProtector:
    def __init__(self, provider: KeyProvider, *, tenant: str, receiver: str):
        self.provider, self.tenant, self.receiver = provider, token(tenant), token(receiver)

    async def seal(self, envelope: Envelope) -> bytes:
        from cryptography.hazmat.primitives.ciphers.aead import AESGCM
        e = envelope.as_dict()
        require(e["tenantId"] == self.tenant, "SEALED_SCOPE_MISMATCH")
        cek, nonce = AESGCM.generate_key(bit_length=256), os.urandom(12)
        key_id, wrapped = await self.provider.wrap(cek, self.tenant, self.receiver)
        require(isinstance(key_id, str) and 0 < len(key_id) <= 256
                and isinstance(wrapped, str) and 0 < len(wrapped) <= 8192, "INVALID_WRAPPED_KEY")
        header = {"version": "a2amq-sealed/1", "algorithm": "A256GCM", "keyId": key_id,
                  "wrappedKey": wrapped, "nonce": _b64(nonce), "tenant": self.tenant,
                  "receiver": self.receiver, "route": envelope.route, "messageId": e["messageId"]}
        encrypted = AESGCM(cek).encrypt(nonce, envelope.wire.encode(), canonical(header).encode())
        raw = canonical({"protected": header, "ciphertext": _b64(encrypted)}).encode()
        require(len(raw) <= SEALED_LIMIT, "MESSAGE_TOO_LARGE")
        return raw

    async def open(self, raw: bytes, *, expected_route: str) -> Envelope:
        from cryptography.exceptions import InvalidTag
        from cryptography.hazmat.primitives.ciphers.aead import AESGCM
        value = load_json(raw, SEALED_LIMIT)
        require(isinstance(value, dict) and set(value) == {"protected", "ciphertext"}, "INVALID_SEALED_ENVELOPE")
        h = value["protected"]
        require(isinstance(h, dict) and set(h) == FIELDS
                and all(isinstance(v, str) for v in h.values()), "INVALID_SEALED_ENVELOPE")
        require(h["version"] == "a2amq-sealed/1" and h["algorithm"] == "A256GCM", "UNSUPPORTED_ENCRYPTION")
        require((h["tenant"], h["receiver"], h["route"]) == (self.tenant, self.receiver, expected_route),
                "SEALED_SCOPE_MISMATCH")
        require(0 < len(h["keyId"]) <= 256 and 0 < len(h["wrappedKey"]) <= 8192, "INVALID_WRAPPED_KEY")
        nonce, ciphertext = _decode(h["nonce"], 12), _decode(value["ciphertext"], MAX_BYTES + 16)
        require(len(nonce) == 12 and len(ciphertext) >= 16, "INVALID_SEALED_ENVELOPE")
        cek = await self.provider.unwrap(h["keyId"], h["wrappedKey"], self.tenant, self.receiver)
        require(isinstance(cek, bytes) and len(cek) == 32, "INVALID_DATA_KEY")
        try:
            plaintext = AESGCM(cek).decrypt(nonce, ciphertext, canonical(h).encode())
        except (InvalidTag, ValueError):
            raise PermanentError("DECRYPTION_FAILED") from None
        envelope = Envelope.parse(plaintext)
        e = envelope.as_dict()
        require(e["messageId"] == h["messageId"] and e["tenantId"] == self.tenant
                and envelope.route == expected_route, "SEALED_IDENTITY_MISMATCH")
        return envelope
