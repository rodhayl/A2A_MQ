"""Acceptance evidence must fail closed, independently of optional service availability."""
import pytest

from a2a_mq.acceptance import FULL_REQUIRED, GROUPS, full_status, junit, test_gate as execute_gate
from a2a_mq.live_probe import wait_until


@pytest.mark.parametrize('xml',[
 '<testsuite tests="1"><testcase name="a"/></testsuite>',
 '<testsuites tests="2"><testsuite tests="2"><testcase name="a"/><testcase name="b"/></testsuite></testsuites>'
])
def test_actual_nonempty_junit_accepted(tmp_path,xml):
    path=tmp_path/'r.xml'
    path.write_text(xml)
    assert junit(path)['tests'] in {1,2}


@pytest.mark.parametrize('xml',[
 '<testsuite/>','<testsuite tests="2"><testcase name="a"/></testsuite>',
 '<testsuite><testcase name="a"><skipped/></testcase></testsuite>',
 '<testsuite><testcase name="a"><failure/></testcase></testsuite>',
 '<testsuite><testcase name="a"><error/></testcase></testsuite>',
 '<!DOCTYPE x><testsuite><testcase name="a"/></testsuite>'
])
def test_bad_or_incomplete_evidence_rejected(tmp_path,xml):
    path=tmp_path/'r.xml'
    path.write_text(xml)
    with pytest.raises(ValueError):
        junit(path)


def test_missing_native_sdk_never_launches_fake_test(tmp_path,monkeypatch):
    monkeypatch.setattr('a2a_mq.acceptance.present',lambda name:False)
    def nope(*args):
        pytest.fail('Missing native SDK must not be replaced with fixtures')
    monkeypatch.setattr('a2a_mq.acceptance.run_command',nope)
    result=execute_gate('sdk',tmp_path,tmp_path,{},30)
    assert result['status']=='UNAVAILABLE' and result['missing']


@pytest.mark.parametrize('statuses,expected',[
 (['PASSED'],'FULL_PASS'),(['PASSED','UNAVAILABLE'],'INCOMPLETE'),
 (['FAILED','UNAVAILABLE'],'FAILED')])
def test_aggregate_keeps_all_missing_gates(statuses,expected):
    results=[{'name':name,'status':'PASSED'} for name in FULL_REQUIRED]
    results.extend({'name':'extra-'+str(i),'status':status} for i,status in enumerate(statuses))
    assert full_status(results,'full')==expected
    assert full_status([{'name':'local','status':'PASSED'}],'local')=='LOCAL_PASS'


async def test_live_deadline_does_not_accept_false_condition():
    async def condition():
        return False
    with pytest.raises(TimeoutError):
        await wait_until(condition,timeout=.01)


@pytest.mark.parametrize('xml',[
 '<other><testsuite><testcase name="a"/></testsuite></other>',
 '<testsuite><testcase name="a"/><testcase name="a"/></testsuite>',
 '<testsuite><properties><testcase name="a"/></properties></testsuite>',
 '<testsuite><error/><testcase name="a"/></testsuite>',
 '<testsuite><testcase name="a"><failure/><error/></testcase></testsuite>'
])
def test_forged_junit_hierarchy_or_duplicates_fail(tmp_path,xml):
    path=tmp_path/'r.xml'
    path.write_text(xml)
    with pytest.raises(ValueError):
        junit(path)


def test_percentile_nearest_rank_and_invalid_measurements():
    from a2a_mq.performance import distribution
    assert distribution(list(range(1,101)))=={'count':100,'medianMs':50.5,'p95Ms':95,'maxMs':100}
    for sample in ([],[float('nan')],[-1],[True]):
        with pytest.raises(ValueError):
            distribution(sample)


@pytest.mark.parametrize("results", [[], [{"name":"local","status":"PASSED"}],
    [{"name":name,"status":"PASSED"} for name in FULL_REQUIRED]+[{"name":"local","status":"PASSED"}]])
def test_full_acceptance_requires_every_named_gate_once(results):
    assert full_status(results,"full")=="INCOMPLETE"


def test_topic_denial_requires_explicit_403_not_unavailability():
    from a2a_mq.live_probe import checked_publish_status
    assert checked_publish_status(204)==204
    assert checked_publish_status(403,403)==403
    for value in (401,404,500,503,True):
        with pytest.raises(ValueError):
            checked_publish_status(value,403)


def test_connection_failures_cannot_establish_owner_denial():
    import httpx
    from a2a_mq.live_probe import require_access_denial
    class NotFound(Exception):
        pass
    require_access_denial(NotFound(), NotFound)
    req=httpx.Request("GET","https://fixture.example/tasks/id")
    require_access_denial(httpx.HTTPStatusError("denied",request=req,response=httpx.Response(403)),NotFound)
    for error in (httpx.ConnectError("unavailable"),httpx.ReadTimeout("timeout"),RuntimeError("SDK broken")):
        with pytest.raises(type(error)):
            require_access_denial(error,NotFound)


@pytest.mark.parametrize("conflict", [False,True])
def test_legacy_postgres_gate_uses_same_private_dsn_without_logging(tmp_path,monkeypatch,conflict):
    import json
    (tmp_path/"tests").mkdir()
    (tmp_path/"tests/test_postgres_live.py").write_text("# concurrent native gate")
    private=tmp_path/"dsn"
    private.write_text("host=fixture dbname=acceptance password=private-value sslmode=verify-full")
    private.chmod(0o600)
    env={"A2AMQ_POSTGRES_DSN_FILE":str(private)}
    if conflict:
        env["A2A_TEST_POSTGRES_DSN"]="different-db"
    monkeypatch.setattr("a2a_mq.acceptance.present",lambda name:True)
    captured=[]
    def run(args,root,log,timeout,child_env):
        captured.append(child_env)
        (tmp_path/"postgres.junit.xml").write_text('<testsuite><testcase name="real-gate-fixture"/></testsuite>')
        return 0
    monkeypatch.setattr("a2a_mq.acceptance.run_command",run)
    result=execute_gate("postgres",tmp_path,tmp_path,env,30)
    assert "private-value" not in json.dumps(result)
    if conflict:
        assert result["status"]=="FAILED" and not captured
    else:
        assert result["status"]=="PASSED"
        assert captured[0]["A2A_TEST_POSTGRES_DSN"]==private.read_text()


def test_sdk_gate_requires_mcp_v2_http_client_dependency():
    assert GROUPS["sdk"]["modules"] == ["a2a", "dapr.ext.workflow", "mcp", "httpx2"]


def test_sdk_owned_mcp_contract_is_excluded_from_local_gate():
    from pathlib import Path

    source = Path(__file__).with_name("test_mcp_adapter.py").read_text(encoding="utf-8")
    assert "pytestmark = pytest.mark.mcp" in source


def test_postgres_marker_suites_share_acceptance_dsn_name():
    from pathlib import Path

    adapter = Path(__file__).with_name("test_postgres_adapter.py").read_text(encoding="utf-8")
    live = Path(__file__).with_name("test_postgres_live.py").read_text(encoding="utf-8")
    assert "A2AMQ_TEST_POSTGRES_DSN" not in adapter
    assert "A2A_TEST_POSTGRES_DSN" in adapter
    assert "A2A_TEST_POSTGRES_DSN" in live
