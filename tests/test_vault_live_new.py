"""Live Vault Transit wrapping/read-window/tamper gate using preprovisioned test keys.

The supplied test key must already have versions 1 and 2. This test never rotates
or changes Vault policies, never uses a root token, and never deletes a key.
"""
import os
import ssl
import httpx
import pytest
from a2a_mq.configuration import read_file
from a2a_mq.domain import PermanentError, command, load_json, canonical
from a2a_mq.protection import EnvelopeProtector
from a2a_mq.vault import VaultTransit, FileToken, TransitKey

pytestmark=[pytest.mark.live,pytest.mark.vault]


async def test_real_vault_rotation_read_window_and_tamper():
    path=os.environ.get('A2AMQ_VAULT_TEST_CONFIG_FILE')
    assert path, 'A2AMQ_VAULT_TEST_CONFIG_FILE is required; unavailable Vault is not a passing gate'
    cfg=load_json(read_file(path))
    context=ssl.create_default_context(cafile=cfg['caFile'])
    common=dict(tenant=cfg['tenant'],receiver=cfg['receiver'])
    old=TransitKey('test-old',cfg['keyName'],**common,write_version=1)
    new=TransitKey('test-new',cfg['keyName'],**common,write_version=2)
    async with httpx.AsyncClient(verify=context,timeout=5,follow_redirects=False,trust_env=False) as http:
        def provider(keys,active):
            return VaultTransit(cfg['url'],FileToken(cfg['tokenFile']),keys,active,
                mount=cfg.get('mount','transit'),namespace=cfg.get('namespace',''),client=http)
        writer=EnvelopeProtector(provider([old],'test-old'),**common)
        reader=EnvelopeProtector(provider([old,new],'test-new'),**common)
        retired=EnvelopeProtector(provider([new],'test-new'),**common)
        request=command('disposable vault acceptance',tenant=cfg['tenant'])
        sealed=await writer.seal(request)
        assert await reader.open(sealed,expected_route=request.route)==request
        rotated=await reader.seal(request)
        assert await retired.open(rotated,expected_route=request.route)==request
        with pytest.raises(PermanentError,match='KEY_NOT_AUTHORIZED'):
            await retired.open(sealed,expected_route=request.route)
        corrupt=load_json(rotated)
        text=corrupt['ciphertext']
        corrupt['ciphertext']=('A' if text[0]!='A' else 'B')+text[1:]
        with pytest.raises(PermanentError,match='DECRYPTION_FAILED'):
            await reader.open(canonical(corrupt).encode(),expected_route=request.route)
        with pytest.raises(PermanentError):
            await reader.open(rotated,expected_route='a2a.v1.foreign.event.task')
