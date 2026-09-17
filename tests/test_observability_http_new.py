"""Real OTel SDK, RS256/ASGI boundary and SQLite operational integration."""
import asyncio
import time

import httpx
import jwt
import pytest
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.primitives import serialization

from a2a_mq.deadletters import DeadLetters,ReplayService
from a2a_mq.dlq_callback import create_dlq_callback
from a2a_mq.domain import Envelope,PermanentError,RetryableError,command
from a2a_mq.identity import JWTVerifier
from a2a_mq.maintenance import Operations
from a2a_mq.operations_http import create_operations
from a2a_mq.runtime import AsyncSQLite
from a2a_mq.storage import SQLiteStore
from a2a_mq.task_api import TaskAPI
from a2a_mq.tasks import TaskLedger
from a2a_mq.telemetry import Telemetry,OTLP
from a2a_mq.transport import cloud_event


@pytest.fixture
def telemetry():
    from opentelemetry.sdk.metrics import MeterProvider
    from opentelemetry.sdk.metrics.export import InMemoryMetricReader
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import SimpleSpanProcessor
    from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
    metrics=InMemoryMetricReader()
    meter=MeterProvider(metric_readers=[metrics])
    spans=InMemorySpanExporter()
    tracer=TracerProvider()
    tracer.add_span_processor(SimpleSpanProcessor(spans))
    try:
        yield Telemetry(meter,tracer),metrics,spans
    finally:
        meter.shutdown()
        tracer.shutdown()


@pytest.mark.parametrize('error',[None,PermanentError('PRIVATE'),RetryableError('PRIVATE'),RuntimeError('PRIVATE'),asyncio.CancelledError()])
async def test_metrics_outcome_and_payload_free_traces(telemetry,error):
    t,reader,export=telemetry
    d=command('PRIVATE-TASK-CONTENT').as_dict()
    d['traceparent']='00-'+'1'*32+'-'+'2'*16+'-01'
    event=Envelope.from_dict(d)
    class Bus:
        async def publish(self,e):
            if error is not None:
                raise error
    bus=t.publisher(Bus())
    if error is None:
        await bus.publish(event)
    else:
        with pytest.raises(type(error)):
            await bus.publish(event)
    spans=export.get_finished_spans()
    assert len(spans)==1 and spans[0].context.trace_id==int('1'*32,16)
    assert spans[0].parent.span_id==int('2'*16,16)
    assert spans[0].events==()
    metrics=reader.get_metrics_data()
    assert 'PRIVATE' not in repr(metrics) and 'PRIVATE' not in repr(dict(spans[0].attributes))
    names={m.name for r in metrics.resource_metrics for s in r.scope_metrics for m in s.metrics}
    assert names=={'a2amq.operations','a2amq.operation.duration'}


def test_gauges_are_fixed_and_do_not_query_database(telemetry):
    t,reader,_=telemetry
    t.update_backlog({'activeTasks':3,'pendingActions':4,'taskId':'secret','tenant':'private',
                      'oldestPublicationAgeSeconds':float('nan')})
    data=reader.get_metrics_data()
    metrics={m.name:m for r in data.resource_metrics for s in r.scope_metrics for m in s.metrics}
    assert set(metrics)=={'a2amq.tasks.active','a2amq.actions.pending'}
    assert metrics['a2amq.tasks.active'].data.data_points[0].value==3
    assert 'secret' not in repr(data)


@pytest.mark.parametrize('url',['http://collector:4318','https://user:secret@x','https://x?token=secret','https://x/path'])
def test_otlp_secure_endpoint_required(url):
    with pytest.raises(PermanentError,match='OTLP_HTTPS_REQUIRED'):
        OTLP(url,service_name='test')


@pytest.fixture
async def apps(tmp_path):
    key=rsa.generate_private_key(public_exponent=65537,key_size=2048)
    pub=key.public_key().public_bytes(serialization.Encoding.PEM,serialization.PublicFormat.SubjectPublicKeyInfo)
    verify=JWTVerifier({'k':pub},issuer='https://issuer.test',audience='https://api.test')
    def headers(scope='a2a:admin',tenant='demo'):
        now=int(time.time())
        raw=jwt.encode(dict(iss=verify.issuer,aud=verify.audience,sub='admin',tenant=tenant,scope=scope,
                            iat=now,nbf=now,exp=now+300),key,algorithm='RS256',headers={'kid':'k'})
        return {'authorization':'Bearer '+raw}
    store=SQLiteStore(tmp_path/'db',tenant='demo',consumer='workflow')
    ledger=TaskLedger(store)
    db=AsyncSQLite(store)
    archive=DeadLetters(store)
    class Bus:
        async def _post(self,*args):
            pass
    replay=ReplayService(archive,db,Bus(),allowed_routes=frozenset({'a2a.v1.demo.command.skill.echo'}))
    api=TaskAPI(ledger,db)
    app=create_operations(api,verify,Operations(ledger),archive,replay,db)
    callback=create_dlq_callback(archive,db,app_token='safe-test-token-value')
    try:
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app),base_url='https://api.test') as client:
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app=callback),base_url='http://app') as internal:
                yield client,internal,headers,archive
    finally:
        await db.aclose()


async def test_ops_requires_permission_and_correct_tenant(apps):
    client,_,headers,_=apps
    assert (await client.get('/status')).status_code==401
    assert (await client.get('/status',headers=headers('a2a:read'))).status_code==403
    assert (await client.get('/status',headers=headers(tenant='other'))).status_code==403
    r=await client.get('/status',headers=headers())
    assert r.status_code==200 and r.json()['activeTasks']==0
    assert r.headers['cache-control']=='no-store'
    r=await client.post('/retention',json={},headers=headers())
    assert r.status_code==200 and r.json()['dryRun'] is True


async def test_dlq_callback_archive_and_authorized_replay(apps):
    client,internal,headers,archive=apps
    req=command('content')
    event={**cloud_event(req),'topic':archive.topic,'pubsubname':'bus'}
    assert (await internal.post('/events',json=event)).status_code==401
    r=await internal.post('/events',json=event,headers={'dapr-api-token':'safe-test-token-value'})
    assert r.json()=={'status':'SUCCESS'}
    listing=(await client.get('/deadletters',headers=headers())).json()
    rid=listing['records'][0]['id']
    assert 'content' not in repr(listing)
    assert (await client.get('/deadletters/'+rid,headers=headers('a2a:operate'))).status_code==403
    assert (await client.get('/deadletters/'+rid,headers=headers())).status_code==200
    r=await client.post('/deadletters/'+rid+'/replay',headers=headers(),json={'requestId':'replay-1'})
    assert r.status_code==202 and r.json()['status']=='queued'
    assert len(archive.pending())==1


async def test_operator_never_loops_failed_archives_to_dlq(apps):
    _,internal,_,_=apps
    r=await internal.post('/events',json={'bad':'delivery'},headers={'dapr-api-token':'safe-test-token-value'})
    assert r.json()=={'status':'RETRY'}
    definitions=(await internal.get('/dapr/subscribe',headers={'dapr-api-token':'safe-test-token-value'})).json()
    assert 'deadLetterTopic' not in definitions[0]


async def test_ops_rejects_unknown_filters_and_duplicate_json(apps):
    client,_,headers,_=apps
    assert (await client.get('/status?tenant=other',headers=headers())).status_code==422
    assert (await client.get('/deadletters?limit=nan',headers=headers())).status_code==422
    h={**headers(),'content-type':'application/json'}
    r=await client.post('/retention',content='{"dryRun":true,"dryRun":false}',headers=h)
    assert r.status_code==400 and r.json()['error']=='DUPLICATE_JSON_KEY'
