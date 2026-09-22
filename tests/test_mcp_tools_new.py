"""Actual SQL/facade/RS256 authorization; separate SDK tests cover the wire."""
import asyncio
import time

import jwt
import pytest
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.primitives import serialization

from a2a_mq.domain import PermanentError
from a2a_mq.identity import JWTVerifier
from a2a_mq.mcp_server import MCPTools, MCPToolFailure, create_mcp
from a2a_mq.runtime import AsyncSQLite
from a2a_mq.storage import SQLiteStore
from a2a_mq.tasks import TaskLedger
from a2a_mq.task_api import TaskAPI


@pytest.fixture
async def setup(tmp_path):
    key = rsa.generate_private_key(public_exponent=65537,key_size=2048)
    pub = key.public_key().public_bytes(serialization.Encoding.PEM, serialization.PublicFormat.SubjectPublicKeyInfo)
    verifier = JWTVerifier({'k':pub},issuer='https://issuer.test',audience='https://tasks.test/mcp',leeway=0)
    def signed(subject='alice', tenant='demo', scopes='a2a:send a2a:read a2a:cancel a2a:approve',exp=None):
        now=int(time.time())
        return jwt.encode(dict(sub=subject,tenant=tenant,scope=scopes,iat=now,nbf=now,
            exp=exp or now+60,iss=verifier.issuer,aud=verifier.audience),key,algorithm='RS256',headers={'kid':'k'})
    token=[signed()]
    store=SQLiteStore(tmp_path/'db',tenant='demo',consumer='workflow')
    ledger=TaskLedger(store)
    db=AsyncSQLite(store)
    api=TaskAPI(ledger,db,wait_timeout=5.0,poll_interval=.005)
    tools=MCPTools(api,verifier,lambda:token[0])
    try:
        yield tools,ledger,token,signed,api,verifier
    finally:
        await db.aclose()


async def test_submit_read_list_idempotent(setup):
    tools,ledger,_,_,_,_=setup
    first=await tools.submit_task('r1','hello')
    assert first['status']['state']=='TASK_STATE_SUBMITTED'
    assert await tools.submit_task('r1','hello')==first
    assert (await tools.get_task(first['id']))['id']==first['id']
    assert 'history' not in await tools.get_task(first['id'])
    assert (await tools.list_tasks())['totalSize']==1
    with pytest.raises(MCPToolFailure,match='MESSAGE_ID_CONFLICT'):
        await tools.submit_task('r1','different')


async def test_token_is_reverified_on_every_call(setup):
    tools,_,token,signed,_,_=setup
    task=await tools.submit_task('r1','private')
    token[0]=signed(subject='bob')
    assert (await tools.list_tasks())['tasks']==[]
    with pytest.raises(MCPToolFailure,match='TASK_NOT_FOUND'):
        await tools.get_task(task['id'])
    token[0]=signed(tenant='other')
    with pytest.raises(MCPToolFailure,match='FORBIDDEN'):
        await tools.list_tasks()
    token[0]=signed(exp=int(time.time())-1)
    with pytest.raises(MCPToolFailure,match='UNAUTHENTICATED'):
        await tools.list_tasks()
    token[0]=None
    with pytest.raises(MCPToolFailure,match='UNAUTHENTICATED'):
        await tools.submit_task('r2','x')


async def test_continue_and_approval_are_durable_and_authorized(setup):
    tools,ledger,token,signed,_,_=setup
    task=await tools.submit_task('r1','x',input_required=True,approval_required=True)
    tid=task['id']
    ledger.step(tid,'work','WORKING')
    ledger.step(tid,'input','INPUT_REQUIRED')
    await tools.continue_task(tid,'c1','more')
    action=ledger.pending(('input',))[0]
    assert 'more' in action.body
    await tools.continue_task(tid,'c1','more')
    assert len(ledger.pending(('input',)))==1
    ledger.step(tid,'resume','WORKING')
    ledger.step(tid,'auth','AUTH_REQUIRED')
    token[0]=signed(scopes='a2a:read a2a:send')
    with pytest.raises(MCPToolFailure,match='FORBIDDEN'):
        await tools.approve_task(tid,'yes',True)
    token[0]=signed()
    await tools.approve_task(tid,'yes',True)
    assert len(ledger.pending(('auth',)))==1


async def test_cancel_requires_owner_confirmation(setup):
    tools,ledger,_,_,_,_=setup
    task=await tools.submit_task('r1','x')
    async def owner():
        while not ledger.pending(('cancel',)):
            await asyncio.sleep(.005)
        ledger.step(task['id'],'canceled','CANCELED')
    worker=asyncio.create_task(owner())
    try:
        result=await tools.cancel_task(task['id'])
        assert result['status']['state']=='TASK_STATE_CANCELED'
    finally:
        await worker


@pytest.mark.parametrize('args',[{'request_id':'','text':'x'}, {'request_id':'r','text':3},
    {'request_id':'r','text':'x','transform':'shell'}, {'request_id':'r','text':'x','delay_seconds':-1},
    {'request_id':'r','text':'x'*17000}, {'request_id':'r','text':'x','input_required':'yes'}])
async def test_input_bounds_do_not_admit_work(setup,args):
    tools,_,_,_,_,_=setup
    with pytest.raises(MCPToolFailure):
        await tools.submit_task(**args)
    assert (await tools.list_tasks())['totalSize']==0


async def test_unexpected_failure_sanitized(setup,monkeypatch):
    tools,_,_,_,api,_=setup
    async def fail(*a,**k):
        raise RuntimeError('sensitive-token-value')
    monkeypatch.setattr(api,'get',fail)
    with pytest.raises(MCPToolFailure,match='^INTERNAL_ERROR$'):
        await tools.get_task('t')


@pytest.mark.parametrize('url',['http://tasks.test/mcp','https://x/api','https://u:p@x/mcp',
                                'https://tasks.test/mcp?q=1','https://tasks.test/mcp#',
                                'https://tasks.test:70000/mcp','https://tasks.test/mcp\n',
                                'https://tasks.test/mcp\t'])
async def test_bad_public_resource_rejected_before_sdk_import(setup,url):
    _,_,_,_,api,verifier=setup
    with pytest.raises(PermanentError,match='MCP_HTTPS_RESOURCE_REQUIRED'):
        create_mcp(api,verifier,resource_url=url)


async def test_resource_specific_audience(setup):
    _,_,_,_,api,verifier=setup
    with pytest.raises(PermanentError,match='MCP_RESOURCE_AUDIENCE_REQUIRED'):
        create_mcp(api,verifier,resource_url='https://other.test/mcp')
