"""Static deployment contracts and administrative HTTP/API adapter tests."""
import json
from pathlib import Path
from types import SimpleNamespace

import httpx
import pytest

from a2a_mq.deployment import IMAGES, broker_plan, component, compose, kubernetes, topology
from a2a_mq.domain import PermanentError
from a2a_mq.provision import jetstream, rabbit


def metadata(doc):
    return {item['name']: item for item in doc['spec']['metadata']}


@pytest.mark.parametrize('broker', ['jetstream','rabbitmq'])
@pytest.mark.parametrize('name',list(topology()))
def test_components_are_role_scoped_and_fixed_topics(broker,name):
    doc = component(broker,name)
    m = metadata(doc)
    assert doc['scopes']==['a2amq-'+name]
    assert m['publishingScopes']['value'].startswith('a2amq-'+name+'=')
    assert m['protectedTopics']['value']==m['allowedTopics']['value']
    if name=='api':
        assert m['subscriptionScopes']['value']=='a2amq-api='
    if broker=='jetstream':
        assert 'consumerID' not in m
        if name!='api':
            assert m['durableName']['value']==m['queueGroupName']['value']
    else:
        assert m['autoAck']['value']=='false' and m['publisherConfirm']['value']=='true'
        assert m['durable']['value']=='true' and m['deletedWhenUnused']['value']=='false'


@pytest.mark.parametrize('broker', ['jetstream','rabbitmq'])
def test_real_local_generation_private_secrets_no_runtime_start(tmp_path,broker):
    root=tmp_path/broker
    path=compose(root,broker=broker,source_root=Path(__file__).resolve().parents[1])
    value=json.loads(path.read_text())
    services=value['services']
    for name in ('api','workflow-a','workflow-b','observer-a','observer-b','operator'):
        assert services[name]['environment']['A2A_ROLE'] != 'worker'
        assert services[name]['depends_on']['broker-init']['condition']=='service_completed_successfully'
        assert services[name+'-dapr']['depends_on'][name]['condition']=='service_started'
        assert services[name+'-dapr']['network_mode']=='service:'+name
        assert all(p.startswith('127.0.0.1:') for p in services[name]['ports'])
    for path in (root/'secrets').iterdir():
        if path.name!='tls.crt':
            assert path.stat().st_mode & 0o077==0
    assert services['workflow-a']['environment']['A2A_CONSUMER']==services['workflow-b']['environment']['A2A_CONSUMER']
    assert services['observer-a']['environment']['A2A_CONSUMER']!=services['observer-b']['environment']['A2A_CONSUMER']
    assert services['nats' if broker=='jetstream' else 'redis']['depends_on']['volume-init']['condition']=='service_completed_successfully'
    with pytest.raises(PermanentError,match='OUTPUT_ALREADY_EXISTS'):
        compose(root,broker=broker,source_root=Path.cwd())


def config(broker='jetstream'):
    return {'namespace':'tenant','image':'registry.example/a2a@sha256:'+'1'*64,'broker':broker,
        'brokerEndpoint':'tls://nats.internal:4222' if broker=='jetstream' else 'amqps://rabbit.internal:5671',
        'publicUrl':'https://api.example','issuer':'https://identity.example','redisHost':'redis.internal:6379',
        'egressCIDRs':['10.0.0.0/16']}


@pytest.mark.parametrize('broker', ['jetstream','rabbitmq'])
def test_production_secrets_tls_ha_and_workload_scopes(broker):
    docs=kubernetes(config(broker))
    for item in docs:
        if item['kind']=='Deployment':
            spec=item['spec']['template']['spec']
            annotations=item['spec']['template']['metadata']['annotations']
            assert spec['containers'][0]['securityContext']['runAsNonRoot'] is True
            assert '@sha256:' in spec['containers'][0]['image']
            assert 'dapr.io/app-token-secret' in annotations and 'dapr.io/api-token-secret' in annotations
            assert 'copy-credentials'==spec['initContainers'][0]['name']
            if item['metadata']['name']=='a2amq-workflow':
                assert item['spec']['replicas']==2
        if item['kind']=='Component' and item['spec']['type'].startswith('pubsub.'):
            m=metadata(item)
            assert len(item['scopes'])==1
            if broker=='jetstream':
                assert 'seedKey' in m and m['natsURL']['value'].startswith('tls:')
            else:
                assert m['protocol']['value']=='amqps' and all('secretKeyRef' in m[k] for k in ['caCert','clientCert','clientKey'])
    assert any(d['kind']=='PodDisruptionBudget' for d in docs)
    assert any(d['kind']=='NetworkPolicy' for d in docs)
    assert all(d['kind']!='Secret' for d in docs)


@pytest.mark.parametrize('broker', ['jetstream','rabbitmq'])
def test_integrated_kubernetes_needs_no_runtime_service_account_credentials(broker):
    docs=kubernetes(config(broker))
    deployments=[item for item in docs if item['kind']=='Deployment']
    assert deployments
    for item in deployments:
        template=item['spec']['template']
        assert template['spec']['automountServiceAccountToken'] is False
        assert template['metadata']['annotations']['dapr.io/disable-builtin-k8s-secret-store']=='true'


def test_integrated_service_accounts_have_no_runtime_secret_rbac():
    docs=kubernetes(config())
    accounts={item['metadata']['name']:item for item in docs if item['kind']=='ServiceAccount'}
    assert set(accounts)=={'a2amq-api','a2amq-workflow','a2amq-observer-a','a2amq-observer-b','a2amq-operator'}
    assert all(item['automountServiceAccountToken'] is False for item in accounts.values())
    assert not [item for item in docs if item['kind'] in {'Role','RoleBinding'}]


@pytest.mark.parametrize('change,code',[({'image':'a2amq:latest'},'DIGEST_PINNED_IMAGE_REQUIRED'),
    ({'image':None},'DIGEST_PINNED_IMAGE_REQUIRED'),
    ({'brokerEndpoint':'nats://plain:4222'},'BROKER_TLS_REQUIRED'),
    ({'egressCIDRs':['0.0.0.0/0']},'EXPLICIT_EGRESS_REQUIRED'),
    ({'egressCIDRs':['not-a-cidr']},'EXPLICIT_EGRESS_REQUIRED'),
    ({'egressCIDRs':[123]},'EXPLICIT_EGRESS_REQUIRED'),
    ({'redisHost':'redis.internal'},'INVALID_REDIS_HOST'),
    ({'redisHost':'redis://redis.internal:6379'},'INVALID_REDIS_HOST'),
    ({'redisHost':'user:pass@redis.internal:6379'},'INVALID_REDIS_HOST'),
    ({'redisHost':'redis.internal:70000'},'INVALID_REDIS_HOST'),
    ({'publicUrl':'http://api'},'PUBLIC_HTTPS_URL_REQUIRED'),
    ({'namespace':'tenant-'},'INVALID_KUBERNETES_NAME'),
    ({'namespace':'x'*64},'INVALID_KUBERNETES_NAME'),
    ({'publicUrl':'https://127.0.0.1'},'INVALID_INGRESS_HOST'),
    ({'publicUrl':'https://bad_host.example'},'INVALID_INGRESS_HOST'),
    ({'cryptoConfigMap':'bad-'},'INVALID_KUBERNETES_NAME')])
def test_production_rejects_unsafe_defaults(change,code):
    with pytest.raises(PermanentError,match=code):
        kubernetes({**config(),**change})


def test_production_redis_host_accepts_bounded_host_port_list():
    docs=kubernetes({**config(),'redisHost':'redis-a.internal:6379,redis-b.internal:6380'})
    state=next(item for item in docs if item['kind']=='Component' and item['metadata']['name']=='workflow-state')
    values={item['name']:item.get('value') for item in state['spec']['metadata']}
    assert values['redisHost']=='redis-a.internal:6379,redis-b.internal:6380'
    assert values['enableTLS']=='true'


def test_kubernetes_names_and_ingress_host_accept_dns_safe_values():
    docs=kubernetes({**config(),'namespace':'tenant-1','cryptoConfigMap':'crypto-1'})
    namespace=next(item for item in docs if item['kind']=='Namespace')
    ingress=next(item for item in docs if item['kind']=='Ingress')
    assert namespace['metadata']['name']=='tenant-1'
    assert ingress['spec']['rules'][0]['host']=='api.example'


def test_optional_vault_otlp_wiring():
    docs=kubernetes({**config(),'cryptoConfigMap':'a2amq-crypto','otlpEndpoint':'https://collector.internal'})
    app=next(d for d in docs if d['kind']=='Deployment' and d['metadata']['name']=='a2amq-api')
    spec=app['spec']['template']['spec']
    env={e['name']:e['value'] for e in spec['containers'][0]['env']}
    assert env['A2A_CRYPTO_CONFIG']=='/credentials/crypto.json'
    assert env['A2A_OTLP_ENDPOINT']=='https://collector.internal'
    assert 'vault-token' in spec['initContainers'][0]['command'][-1]


def test_rabbit_plan_retains_dlq_without_recursion():
    plan=broker_plan()
    p={p['name']:p['definition'] for p in plan['policies']}
    assert p['a2amq-workflow-v1']['dead-letter-exchange']=='a2a.v1.demo.dlq'
    assert p['a2amq-operator-v1']['dead-letter-exchange']=='a2a.v1.demo.parking'
    assert 'dead-letter-exchange' not in p['a2amq-parking-v1']
    assert all(v['overflow']=='reject-publish' for v in p.values())
    assert all(v.get('dead-letter-strategy')=='at-least-once' for k,v in p.items() if k!='a2amq-parking-v1')


def test_real_httpx_rabbit_bootstrap_provisions_before_consumers(tmp_path):
    password=tmp_path/'secret'
    password.write_text('x'*32)
    password.chmod(0o600)
    stored={}
    order=[]
    def handler(request):
        path=request.url.path
        order.append((request.method,path))
        if request.method=='PUT':
            stored[path]=json.loads(request.content)
            return httpx.Response(204)
        if request.method=='POST':
            return httpx.Response(201)
        return httpx.Response(200,json=stored.get(path,{}))
    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        result=rabbit('http://rabbit',password,broker_plan(),development=True,client=client)
    assert result['queues']==5 and result['verified']
    assert any('/policies/' in p for _,p in order)
    assert next(i for i,(m,p) in enumerate(order) if '/queues/' in p)>next(i for i,(m,p) in enumerate(order) if '/exchanges/' in p)


@pytest.mark.parametrize('exists',[False,True])
async def test_native_jetstream_api_bootstrap_and_existing_verification(tmp_path,exists):
    password=tmp_path/'secret'
    password.write_text('x'*32)
    password.chmod(0o600)
    plan=broker_plan()
    calls=[]
    closed=[]
    class Client:
        async def request(self,subject,payload,timeout):
            calls.append(subject)
            if subject.startswith('$JS.API.STREAM.INFO') and not exists:
                return SimpleNamespace(data=b'{"error":{"code":404}}')
            return SimpleNamespace(data=json.dumps({'config':plan['stream']}).encode())
        async def close(self):
            closed.append(True)
    async def connect(**kw):
        assert kw['allow_reconnect'] is False
        return Client()
    result=await jetstream('nats://localhost:4222',password,plan,development=True,connect=connect)
    assert result['verified'] and closed
    assert len(calls)==(1 if exists else 2)


@pytest.mark.parametrize("broker,scheme", [("jetstream","tls"),("rabbitmq","amqps")])
@pytest.mark.parametrize("suffix", ["//user:secret@broker:4222","//broker:4222?secret=value",
                                    "///missing-host","//broker:70000","//broker:bad","//bro\nker:4222"])
def test_component_urls_reject_credentials_and_ambiguous_endpoints(broker,scheme,suffix):
    from a2a_mq.deployment import component
    from a2a_mq.domain import PermanentError
    with pytest.raises(PermanentError,match="BROKER_TLS_REQUIRED"):
        component(broker,"workflow",endpoint=scheme+":"+suffix,production=True)


def test_disposable_live_fixture_scrubs_generated_secrets(tmp_path):
    from a2a_mq.live_probe import ComposeRun

    fixture = object.__new__(ComposeRun)
    fixture.root = tmp_path
    secrets = tmp_path / "secrets"
    secrets.mkdir(mode=0o700)
    for name in ("jwt-private.pem", "postgres-password", "tls.key", "api-app"):
        path = secrets / name
        path.write_text("disposable-secret")
        path.chmod(0o600)
    public = tmp_path / "live-verdict.json"
    public.write_text("{}")

    fixture.scrub_secrets()

    assert not secrets.exists()
    assert public.exists()


def test_integrated_deployment_dapr_patch_matches_python_sdk_pin():
    assert IMAGES["dapr"] == "daprio/daprd:1.18.3"
    assert IMAGES["control"] == "daprio/dapr:1.18.3"
    dockerfile = (Path(__file__).resolve().parents[1] / "deploy" / "Dockerfile.sidecar").read_text()
    assert "ARG DAPR_IMAGE=daprio/daprd:1.18.3" in dockerfile
