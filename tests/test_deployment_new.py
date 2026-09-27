"""Static deployment contracts and administrative HTTP/API adapter tests."""
import json
from pathlib import Path
from types import SimpleNamespace

import httpx
import pytest

import a2a_mq.deployment as deployment
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


def test_compose_preserves_dangling_output_symlink(tmp_path):
    root=tmp_path/'stack'
    target=tmp_path/'missing-target'
    root.symlink_to(target)
    with pytest.raises(PermanentError,match='OUTPUT_ALREADY_EXISTS'):
        compose(root,broker='jetstream',source_root=Path(__file__).resolve().parents[1])
    assert root.is_symlink() and root.readlink()==target
    assert not target.exists()


def test_failed_compose_generation_leaves_no_partial_secret_tree(tmp_path,monkeypatch):
    root=tmp_path/'failed'
    source=Path(__file__).resolve().parents[1]
    original=deployment._write
    calls=0

    def fail_after_first_write(*args,**kwargs):
        nonlocal calls
        calls+=1
        if calls==2:
            raise RuntimeError('fixture generation failed')
        return original(*args,**kwargs)

    monkeypatch.setattr(deployment,'_write',fail_after_first_write)
    with pytest.raises(RuntimeError,match='fixture generation failed'):
        deployment.compose(root,broker='jetstream',source_root=source)
    assert not root.exists()
    assert not list(tmp_path.glob('failed.*.tmp'))



def test_deployment_json_config_is_bounded_regular_and_duplicate_safe(tmp_path):
    path=tmp_path/'config.json'
    path.write_text('{"broker":"jetstream"}')
    assert deployment._json_config(path)=={"broker":"jetstream"}

    duplicate=tmp_path/'duplicate.json'
    duplicate.write_text('{"broker":"jetstream","broker":"rabbitmq"}')
    with pytest.raises(PermanentError,match='DUPLICATE_JSON_KEY'):
        deployment._json_config(duplicate)

    target=tmp_path/'target.json'
    target.write_text('{}')
    link=tmp_path/'config-link.json'
    link.symlink_to(target)
    with pytest.raises(PermanentError,match='CONFIG_FILE_UNAVAILABLE'):
        deployment._json_config(link)

    oversized=tmp_path/'oversized.json'
    oversized.write_text('x'*(deployment.MAX_BYTES+1))
    with pytest.raises(PermanentError,match='INVALID_CONFIG_FILE'):
        deployment._json_config(oversized)

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


def test_network_policy_includes_configured_service_ports():
    docs=kubernetes({**config(),
        'brokerEndpoint':'tls://nats.internal:7443',
        'redisHost':'redis-a.internal:6379,redis-b.internal:6380',
        'otlpEndpoint':'https://collector.internal:4318'})
    policy=next(item for item in docs if item['kind']=='NetworkPolicy')
    ports={entry['port'] for rule in policy['spec']['egress']
           for entry in rule.get('ports',[]) if entry.get('protocol')=='TCP'}
    assert {7443,6380,4318} <= ports
    assert {5432,50001,50002,50005,50006} <= ports


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


@pytest.mark.parametrize("url", ["http://rabbit?", "http://rabbit#", "http://:15672",
                                 "http://rabbit:70000", "http://rabbit:bad",
                                 "http://rabbit\n", "http://rabbit/path"])
def test_rabbit_provisioning_rejects_empty_query_fragment_delimiters(url):
    with pytest.raises(PermanentError, match="PROVISION_TLS_REQUIRED"):
        rabbit(url, "unused", {}, development=True, client=object())


@pytest.mark.parametrize("url", ["nats://localhost:4222?", "nats://localhost:4222#",
                                 "nats://:4222", "nats://localhost:70000",
                                 "nats://localhost:bad", "nats://localhost:4222\n",
                                 "nats://localhost:4222/path"])
async def test_jetstream_provisioning_rejects_empty_query_fragment_delimiters(url):
    with pytest.raises(PermanentError, match="PROVISION_TLS_REQUIRED"):
        await jetstream(url, "unused", {}, development=True, connect=object())


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


def test_deployment_cli_kubernetes_rejects_existing_and_symlink_output(tmp_path, monkeypatch):
    import json
    import sys
    from a2a_mq.deployment import main

    cfg = tmp_path / "config.json"
    cfg.write_text(json.dumps(config()))

    out = tmp_path / "manifest.json"
    out.write_text("{}")
    monkeypatch.setattr(sys, "argv", ["deployment.py", "kubernetes", "--config", str(cfg), "--output", str(out)])
    with pytest.raises(PermanentError, match="OUTPUT_ALREADY_EXISTS"):
        main()

    target = tmp_path / "target.json"
    dangling = tmp_path / "dangling.json"
    dangling.symlink_to(target)
    monkeypatch.setattr(sys, "argv", ["deployment.py", "kubernetes", "--config", str(cfg), "--output", str(dangling)])
    with pytest.raises(PermanentError, match="OUTPUT_ALREADY_EXISTS"):
        main()
