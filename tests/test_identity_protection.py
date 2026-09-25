"""Real RSA/JWT and AES-GCM/key-wrap tests; Vault responses are HTTP fixtures."""
import base64
import json
import os
import time

import httpx
import jwt
import pytest
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.primitives import serialization

from a2a_mq.domain import PermanentError, RetryableError, command, canonical
from a2a_mq.identity import BearerIdentity, JWTVerifier, Principal, current_principal
from a2a_mq.protection import EnvelopeProtector, LocalKey, LocalKeyRing
from a2a_mq.vault import FileToken, TransitKey, VaultTransit


@pytest.fixture(scope="module")
def rsa_keys():
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    return key, key.public_key().public_bytes(serialization.Encoding.PEM,
                                             serialization.PublicFormat.SubjectPublicKeyInfo)


def bearer(keys, changes=None, header=None):
    now = int(time.time())
    claims = dict(iss="issuer", aud="a2amq", sub="alice", tenant="demo", scope="a2a:send a2a:read",
                  iat=now, nbf=now, exp=now+300)
    claims.update(changes or {})
    return jwt.encode(claims, keys[0], algorithm="RS256", headers=header or {"kid": "one"})


def verifier(keys):
    return JWTVerifier({"one": keys[1]}, issuer="issuer", audience="a2amq", leeway=0)


def test_verified_identity(rsa_keys):
    p = verifier(rsa_keys).verify(bearer(rsa_keys))
    assert p == Principal("alice", "demo", frozenset({"a2a:send", "a2a:read"}))
    p.authorize("read", "demo", "alice")


@pytest.mark.parametrize("changes", [
    {"iss":"other"}, {"aud":"other"}, {"exp":1}, {"nbf":2**40}, {"iat":2**40},
    {"exp":2**40}, {"sub":None}, {"tenant":"other.dot"}, {"scope":[]}, {"nbf":True},
    {"exp":"9999999999"}, {"iat":None}, {"scope":"x" * 2049},
])
def test_bad_claims(rsa_keys, changes):
    with pytest.raises(PermanentError, match="^UNAUTHENTICATED$"):
        verifier(rsa_keys).verify(bearer(rsa_keys, changes))


@pytest.mark.parametrize("header", [{"kid":"unknown"}, {"kid":"one", "jku":"https://invalid"},
                                    {"kid":"one", "crit":[]}, {"kid":"one", "typ":"other"}])
def test_untrusted_headers(rsa_keys, header):
    with pytest.raises(PermanentError, match="UNAUTHENTICATED"):
        verifier(rsa_keys).verify(bearer(rsa_keys, header=header))


@pytest.mark.parametrize("op,tenant,owner", [("cancel","demo","alice"), ("read","other","alice"),
                                            ("read","demo","bob")])
def test_policy_negative(op, tenant, owner):
    p = Principal("alice", "demo", frozenset({"a2a:read"}))
    with pytest.raises(PermanentError, match="FORBIDDEN"):
        p.authorize(op, tenant, owner)


async def test_middleware_isolation_and_duplicate_header(rsa_keys):
    from starlette.responses import JSONResponse
    async def app(scope, receive, send):
        await JSONResponse({"subject":current_principal().subject})(scope, receive, send)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(BearerIdentity(app, verifier(rsa_keys))),
                                 base_url="http://fixture") as client:
        value = "Bearer " + bearer(rsa_keys)
        assert (await client.get("/", headers={"Authorization":value})).json() == {"subject":"alice"}
        assert (await client.get("/", headers=[("Authorization",value), ("Authorization",value)])).status_code == 401
        assert (await client.get("/")).status_code == 401
    with pytest.raises(PermanentError, match="UNAUTHENTICATED"):
        current_principal()


@pytest.fixture
def protector():
    return EnvelopeProtector(LocalKeyRing([LocalKey("v1","demo","workers",os.urandom(32))], "v1"),
                             tenant="demo", receiver="workers")


async def test_seal_roundtrip_and_randomized_data_keys(protector):
    e = command("private content")
    first, second = await protector.seal(e), await protector.seal(e)
    assert first != second and b"private content" not in first
    assert json.loads(first)["protected"]["wrappedKey"] != json.loads(second)["protected"]["wrappedKey"]
    assert await protector.open(first, expected_route=e.route) == e


@pytest.mark.parametrize("field", ["version","algorithm","keyId","wrappedKey","nonce","tenant",
                                   "receiver","route","messageId"])
async def test_protected_metadata_mutation_rejected(protector, field):
    e = command("private")
    value = json.loads(await protector.seal(e))
    value["protected"][field] = "changed"
    with pytest.raises(PermanentError):
        await protector.open(canonical(value).encode(), expected_route=e.route)


async def test_ciphertext_mutation_rejected(protector):
    e = command("private")
    value = json.loads(await protector.seal(e))
    data = bytearray(base64.b64decode(value["ciphertext"]))
    data[0] ^= 1
    value["ciphertext"] = base64.b64encode(data).decode()
    with pytest.raises(PermanentError, match="DECRYPTION_FAILED"):
        await protector.open(canonical(value).encode(), expected_route=e.route)


async def test_rotation_read_window():
    one, two = LocalKey("v1","demo","workers",os.urandom(32)), LocalKey("v2","demo","workers",os.urandom(32))
    e = command("private")
    old = EnvelopeProtector(LocalKeyRing([one],"v1"),tenant="demo",receiver="workers")
    data = await old.seal(e)
    rotated = EnvelopeProtector(LocalKeyRing([one,two],"v2"),tenant="demo",receiver="workers")
    assert await rotated.open(data,expected_route=e.route) == e
    assert json.loads(await rotated.seal(e))["protected"]["keyId"] == "v2"
    retired = EnvelopeProtector(LocalKeyRing([two],"v2"),tenant="demo",receiver="workers")
    with pytest.raises(PermanentError,match="KEY_NOT_AUTHORIZED"):
        await retired.open(data,expected_route=e.route)


@pytest.mark.parametrize("raw", [b"null",b"{}",b'{"protected":null,"ciphertext":""}',b'"text"'])
async def test_malformed_sealed(protector, raw):
    with pytest.raises(PermanentError):
        await protector.open(raw,expected_route=command("x").route)


def test_file_token_rotation_and_permissions(tmp_path):
    path = tmp_path/"secret"
    path.write_text("first\n")
    path.chmod(0o600)
    provider = FileToken(path)
    assert provider() == "first"
    path.write_text("second\n")
    assert provider() == "second"
    path.chmod(0o644)
    with pytest.raises(PermanentError,match="UNSAFE_SECRET_FILE"):
        provider()


async def test_vault_wrap_unwrap_and_scope():
    key = b"x"*32
    requests = []
    def respond(request):
        requests.append(request)
        return httpx.Response(200,json={"data":{"ciphertext":"vault:v2:eA=="} if "/encrypt/" in request.url.path
                                       else {"plaintext":base64.b64encode(key).decode()}})
    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
        vault=VaultTransit("https://vault.example",lambda:"test-token",[TransitKey("k","name","demo","workers",2)],"k",client=client)
        assert await vault.wrap(key,"demo","workers") == ("k","vault:v2:eA==")
        assert await vault.unwrap("k","vault:v2:eA==","demo","workers") == key
        with pytest.raises(PermanentError,match="KEY_NOT_AUTHORIZED"):
            await vault.unwrap("k","vault:v2:eA==","other","workers")
        with pytest.raises(PermanentError,match="RETIRED_OR_UNKNOWN_KEY_VERSION"):
            await vault.unwrap("k","vault:v3:eA==","demo","workers")
    assert len(requests)==2 and requests[0].headers["X-Vault-Token"] == "test-token"


@pytest.mark.parametrize("status", [301,403,500])
async def test_vault_fails_closed(status):
    async with httpx.AsyncClient(transport=httpx.MockTransport(lambda r:httpx.Response(status))) as client:
        vault=VaultTransit("https://vault.example",lambda:"test",[TransitKey("k","name","demo","workers",1)],"k",client=client)
        with pytest.raises(RetryableError if status==500 else PermanentError):
            await vault.wrap(b"x"*32,"demo","workers")


async def test_encrypted_publication_is_stable_after_restart(tmp_path, protector):
    from a2a_mq.runtime import AsyncSQLite
    from a2a_mq.storage import SQLiteStore
    from a2a_mq.secure_transport import ProtectedPublisher
    from a2a_mq.transport import DaprPublisher
    e = command("private content")
    captured = []
    def respond(request):
        captured.append(request.content)
        return httpx.Response(204)
    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
        bus = DaprPublisher(client=client)
        for _ in range(2):
            lane = AsyncSQLite(SQLiteStore(tmp_path/"db",tenant="demo",consumer="producer"))
            try:
                publisher=ProtectedPublisher(bus,lane,{e.route:protector})
                await publisher.publish(e)
            finally:
                await lane.aclose()
    assert captured[0] == captured[1] and b"private content" not in captured[0]
    assert await protector.open(canonical(json.loads(captured[0])["data"]).encode(), expected_route=e.route) == e


async def test_encrypted_callback_and_plaintext_downgrade(protector):
    from a2a_mq.transport import DaprPublisher, cloud_event, subscriber
    e = command("private content")
    calls=[]
    dlq=[]
    async def handle(envelope):
        calls.append(envelope)
    def respond(request):
        dlq.append(request.content)
        return httpx.Response(204)
    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as outbound:
        bus = DaprPublisher(client=outbound)
        app = subscriber(handle,bus,tenant="demo",consumer="workers",topic=e.route,protector=protector)
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app),base_url="http://app") as client:
            event = {**cloud_event(e),"topic":e.route,"pubsubname":"bus"}
            assert (await client.post("/events",json=event)).json()=={"status":"SUCCESS"}
            assert not calls and len(dlq)==1
            event.update(data=json.loads(await protector.seal(e)),datacontenttype="application/vnd.a2amq.sealed+json")
            assert (await client.post("/events",json=event)).json()=={"status":"SUCCESS"}
            assert calls==[e] and len(dlq)==1
