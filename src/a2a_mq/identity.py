"""Verified workload principals for HTTP bridges. Payload fields never grant authority."""
from __future__ import annotations

from contextvars import ContextVar
from dataclasses import dataclass
from types import MappingProxyType
from typing import Mapping

from starlette.responses import JSONResponse

from .domain import PermanentError, require, token


@dataclass(frozen=True)
class Principal:
    subject: str
    tenant: str
    scopes: frozenset[str]

    def __post_init__(self) -> None:
        require(isinstance(self.subject, str) and 0 < len(self.subject) <= 256
                and all(ord(c) >= 32 for c in self.subject), "INVALID_PRINCIPAL")
        token(self.tenant)
        require(isinstance(self.scopes, frozenset) and len(self.scopes) <= 32
                and all(isinstance(s, str) and 0 < len(s) <= 64 for s in self.scopes),
                "INVALID_PRINCIPAL")

    def authorize(self, operation: str, tenant: str, owner: str | None = None) -> None:
        require(self.tenant == tenant, "FORBIDDEN")
        require("a2a:admin" in self.scopes or "a2a:" + operation in self.scopes, "FORBIDDEN")
        require(owner is None or owner == self.subject or "a2a:admin" in self.scopes, "FORBIDDEN")


_CURRENT: ContextVar[Principal | None] = ContextVar("a2amq_principal", default=None)


def current_principal() -> Principal:
    principal = _CURRENT.get()
    require(principal is not None, "UNAUTHENTICATED")
    assert principal is not None
    return principal


class JWTVerifier:
    """RS256 only, pinned public keys, issuer/audience and mandatory bounded lifetime.

    Update the configured key map atomically when rotating. Untrusted JWT URLs,
    keys and algorithms are never used to fetch or select verification material.
    """

    def __init__(self, keys: Mapping[str, str | bytes], *, issuer: str, audience: str,
                 max_lifetime: int = 3600, leeway: int = 30):
        require(isinstance(issuer, str) and bool(issuer) and isinstance(audience, str)
                and bool(audience), "INVALID_IDENTITY_CONFIG")
        require(type(max_lifetime) is int and 1 <= max_lifetime <= 86400
                and type(leeway) is int and 0 <= leeway <= 60, "INVALID_IDENTITY_CONFIG")
        require(1 <= len(keys) <= 32, "INVALID_IDENTITY_CONFIG")
        try:
            from cryptography.hazmat.primitives.serialization import load_pem_public_key
            from cryptography.hazmat.primitives.asymmetric.rsa import RSAPublicKey
        except ImportError:
            raise PermanentError("SECURITY_EXTRA_REQUIRED") from None
        verified: dict[str, RSAPublicKey] = {}
        for kid, pem in keys.items():
            token(kid)
            try:
                key = load_pem_public_key(pem.encode() if isinstance(pem, str) else pem)
            except (ValueError, TypeError):
                raise PermanentError("INVALID_IDENTITY_KEY") from None
            require(isinstance(key, RSAPublicKey) and key.key_size >= 2048, "INVALID_IDENTITY_KEY")
            assert isinstance(key, RSAPublicKey)
            verified[kid] = key
        self.keys = MappingProxyType(verified)
        self.issuer, self.audience = issuer, audience
        self.max_lifetime, self.leeway = max_lifetime, leeway

    def verify(self, encoded: str) -> Principal:
        try:
            import jwt
        except ImportError:
            raise PermanentError("SECURITY_EXTRA_REQUIRED") from None
        try:
            require(isinstance(encoded, str) and 0 < len(encoded) <= 16384, "UNAUTHENTICATED")
            header = jwt.get_unverified_header(encoded)
            require(header.get("alg") == "RS256" and header.get("typ") in {None, "JWT", "at+jwt"}
                    and not any(k in header for k in ("crit", "jku", "jwk", "x5u")), "UNAUTHENTICATED")
            kid = header.get("kid")
            require(isinstance(kid, str), "UNAUTHENTICATED")
            assert isinstance(kid, str)
            key = self.keys.get(kid)
            require(key is not None, "UNAUTHENTICATED")
            assert key is not None
            claims = jwt.decode(encoded, key=key, algorithms=["RS256"], issuer=self.issuer,
                                audience=self.audience, leeway=self.leeway,
                                options={"require": ["exp", "iat", "nbf", "iss", "aud", "sub", "tenant", "scope"]})
            require(all(type(claims[k]) is int for k in ("exp", "iat", "nbf")), "UNAUTHENTICATED")
            require(0 < claims["exp"] - claims["iat"] <= self.max_lifetime
                    and claims["iat"] <= claims["nbf"] < claims["exp"], "UNAUTHENTICATED")
            require(isinstance(claims["scope"], str) and len(claims["scope"]) <= 2048, "UNAUTHENTICATED")
            return Principal(claims["sub"], claims["tenant"], frozenset(claims["scope"].split()))
        except (jwt.PyJWTError, PermanentError, TypeError, ValueError, KeyError):
            raise PermanentError("UNAUTHENTICATED") from None


class BearerIdentity:
    """ASGI middleware; context is reset even during streaming/cancellation."""

    def __init__(self, app, verifier: JWTVerifier, *, public_paths: frozenset[str] = frozenset()):
        self.app, self.verifier, self.public_paths = app, verifier, public_paths

    async def __call__(self, scope, receive, send):
        if scope["type"] == "websocket":
            await send({"type": "websocket.close", "code": 1008})
            return
        if scope["type"] != "http" or (scope["method"] == "GET" and scope["path"] in self.public_paths):
            await self.app(scope, receive, send)
            return
        values = [v for k, v in scope["headers"] if k.lower() == b"authorization"]
        try:
            require(len(values) == 1 and values[0].startswith(b"Bearer "), "UNAUTHENTICATED")
            principal = self.verifier.verify(values[0][7:].decode("ascii"))
        except (PermanentError, UnicodeError):
            response = JSONResponse({"error": "UNAUTHENTICATED"}, status_code=401,
                                    headers={"WWW-Authenticate": "Bearer", "Cache-Control": "no-store"})
            await response(scope, receive, send)
            return
        marker = _CURRENT.set(principal)
        try:
            scope.setdefault("state", {})["principal"] = principal
            await self.app(scope, receive, send)
        finally:
            _CURRENT.reset(marker)
