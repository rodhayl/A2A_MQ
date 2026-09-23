"""Acceptance evidence must fail closed, independently of optional service availability."""
import pytest

from a2a_mq.acceptance import (FULL_REQUIRED, GIT_OVERRIDES, GROUPS, MODEL_CREDENTIAL, _file_sha256, evidence_hashes, full_status,
                               source_hashes,
                               git_snapshot, junit, package_gate, provenance_status,
                               test_gate as execute_gate)
from a2a_mq.domain import PermanentError
from a2a_mq.live_probe import LIVE_REQUIRED_CHECKS, broker_outage_hold_seconds, broker_service, wait_until


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


@pytest.mark.parametrize("name", [
    "OPENAI_API_KEY", "ANTHROPIC_API_KEY", "AZURE_OPENAI_API_KEY", "GEMINI_API_KEY",
    "GOOGLE_API_KEY", "MISTRAL_API_KEY", "COHERE_API_KEY", "GROQ_API_KEY",
    "DEEPSEEK_API_KEY", "TOGETHER_API_KEY", "OPENROUTER_API_KEY", "HF_TOKEN",
    "HUGGING_FACE_TOKEN", "XAI_API_KEY", "CEREBRAS_API_KEY", "REPLICATE_API_TOKEN",
])
def test_acceptance_model_credential_guard_matches_independent_review_scope(name):
    assert MODEL_CREDENTIAL.match(name)


@pytest.mark.parametrize("name", ["DAPR_API_TOKEN", "APP_API_TOKEN", "VAULT_TOKEN", "A2A_JWT_KEYS_FILE"])
def test_acceptance_model_credential_guard_does_not_block_product_credentials(name):
    assert MODEL_CREDENTIAL.match(name) is None


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


@pytest.mark.parametrize("initial,final", [
    (None, {"head":"a" * 40,"tree":"b" * 40}),
    ({"head":"a" * 40,"tree":"b" * 40}, None),
    ({"head":"a" * 40,"tree":"b" * 40}, {"head":"c" * 40,"tree":"d" * 40}),
    ({"head":"a" * 40,"tree":"b" * 40}, {"head":"a" * 40,"tree":"d" * 40}),
])
def test_acceptance_git_override_guard_covers_repository_redirection():
    assert {"GIT_DIR", "GIT_WORK_TREE", "GIT_INDEX_FILE",
            "GIT_OBJECT_DIRECTORY", "GIT_ALTERNATE_OBJECT_DIRECTORIES"} <= GIT_OVERRIDES


def test_passing_acceptance_requires_stable_git_provenance(initial,final):
    assert provenance_status("LOCAL_PASS", initial, final) == "INCOMPLETE"
    assert provenance_status("FULL_PASS", initial, final) == "INCOMPLETE"


def test_stable_git_provenance_preserves_pass_and_failure():
    snapshot={"head":"a" * 40,"tree":"b" * 40}
    assert provenance_status("LOCAL_PASS", snapshot, snapshot.copy()) == "LOCAL_PASS"
    assert provenance_status("FULL_PASS", snapshot, snapshot.copy()) == "FULL_PASS"
    assert provenance_status("FAILED", None, None) == "FAILED"


def test_git_snapshot_requires_clean_exact_head_and_tree(tmp_path,monkeypatch):
    values=iter([str(tmp_path), "a" * 40, "b" * 40, ""])
    monkeypatch.setattr("a2a_mq.acceptance.subprocess.check_output",
                        lambda *a,**k: next(values))
    assert git_snapshot(tmp_path)=={"head":"a" * 40,"tree":"b" * 40}
    values=iter([str(tmp_path), "a" * 40, "b" * 40, "?? untracked"])
    monkeypatch.setattr("a2a_mq.acceptance.subprocess.check_output",
                        lambda *a,**k: next(values))
    assert git_snapshot(tmp_path) is None
    values=iter([str(tmp_path / "other"), "a" * 40, "b" * 40, ""])
    monkeypatch.setattr("a2a_mq.acceptance.subprocess.check_output",
                        lambda *a,**k: next(values))
    assert git_snapshot(tmp_path) is None


@pytest.mark.parametrize("broker,service", [("jetstream","nats"),("rabbitmq","rabbitmq")])
def test_live_broker_recovery_targets_real_broker_service(broker,service):
    assert broker_service(broker)==service
    with pytest.raises(PermanentError, match="INVALID_BROKER"):
        broker_service("fixture")


def test_live_broker_matrix_pins_all_production_checks():
    assert "broker-outage-recovery-without-sidecar-restart" in LIVE_REQUIRED_CHECKS
    assert "live-dlq-archive-exact-replay-and-publish-scopes" in LIVE_REQUIRED_CHECKS
    assert "official-mcp-http-authenticated-query" in LIVE_REQUIRED_CHECKS
    assert len(LIVE_REQUIRED_CHECKS)==12


def test_jetstream_outage_exceeds_default_reconnect_budget():
    assert broker_outage_hold_seconds("jetstream") > 60 * 2
    assert 1 <= broker_outage_hold_seconds("rabbitmq") < 60


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
    env={"A2AMQ_POSTGRES_DSN_FILE":str(private),
         "A2AMQ_POSTGRES_TEST_ALLOW_INSECURE":"1"}
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
        assert "A2AMQ_POSTGRES_TEST_ALLOW_INSECURE" not in captured[0]


def test_postgres_gate_fails_closed_when_private_dsn_cannot_be_read(tmp_path,monkeypatch):
    from a2a_mq.domain import PermanentError

    (tmp_path/"tests").mkdir()
    (tmp_path/"tests/test_postgres_live.py").write_text("# concurrent native gate")
    monkeypatch.setattr("a2a_mq.acceptance.present",lambda name:True)
    monkeypatch.setattr("a2a_mq.configuration.read_file",
                        lambda *a,**k: (_ for _ in ()).throw(PermanentError("UNSAFE_CONFIG_FILE")))
    result=execute_gate("postgres",tmp_path,tmp_path,
                        {"A2AMQ_POSTGRES_DSN_FILE":str(tmp_path/"dsn")},30)
    assert result == {"name":"postgres","status":"FAILED",
                      "configurationError":"INVALID_POSTGRES_GATE_FILE"}


def test_full_acceptance_requires_repository_contract_checker():
    assert "repository-check" in FULL_REQUIRED


def test_full_acceptance_requires_exact_commit_package_smoke():
    assert "package" in FULL_REQUIRED


def test_package_gate_uses_external_git_archive_and_isolated_import(tmp_path,monkeypatch):
    import io
    from pathlib import Path
    import tarfile

    root=tmp_path/"repo"
    root.mkdir()
    output=tmp_path/"evidence"
    output.mkdir()
    calls=[]

    def run(args,cwd,log,timeout,env):
        calls.append((args,cwd,env))
        if args[:2] == ["git","archive"]:
            archive=Path(args[-1])
            data=b"[build-system]\nrequires=[]\nbuild-backend='fixture'\n"
            with tarfile.open(archive,"w") as bundle:
                info=tarfile.TarInfo("pyproject.toml")
                info.size=len(data)
                bundle.addfile(info,io.BytesIO(data))
        elif args[1:4] == ["-m","pip","wheel"]:
            wheel_dir=Path(args[args.index("--wheel-dir")+1])
            (wheel_dir/"a2a_mq-0.1.0-py3-none-any.whl").write_bytes(b"fixture-wheel")
        return 0

    monkeypatch.setattr("a2a_mq.acceptance.run_command",run)
    result=package_gate(root,output,{},30)
    assert result["status"]=="PASSED"
    assert calls[0][0][:3] == ["git","archive","--format=tar"]
    assert calls[0][1] == root
    assert calls[1][1] == output and str(root) not in calls[1][0][-1]
    assert "--no-build-isolation" not in calls[1][0]
    assert calls[-1][0][1:3] == ["-I","-c"]
    assert calls[-1][2]["A2AMQ_WHEEL_TARGET"].startswith(str(output))



def test_evidence_file_hashing_streams_without_path_read_bytes(tmp_path, monkeypatch):
    import hashlib
    from pathlib import Path

    payload=tmp_path/"large.log"
    payload.write_bytes((b"0123456789abcdef"*131072)+b"tail")
    expected=hashlib.sha256(payload.read_bytes()).hexdigest()

    monkeypatch.setattr(Path,"read_bytes",lambda self: (_ for _ in ()).throw(AssertionError("read_bytes forbidden")))
    assert _file_sha256(payload)==expected

def test_streaming_evidence_hash_rejects_symlink_target(tmp_path):
    target=tmp_path/"target.log"
    target.write_text("outside")
    link=tmp_path/"evidence.log"
    link.symlink_to(target)
    with pytest.raises(OSError):
        _file_sha256(link)


def test_source_hashes_stream_and_reject_symlinked_python(tmp_path):
    source = tmp_path / "src" / "a2a_mq"
    source.mkdir(parents=True)
    module = source / "safe.py"
    module.write_text("VALUE = 1\n")
    hashes, unsafe = source_hashes(tmp_path)
    assert unsafe == []
    assert hashes["src/a2a_mq/safe.py"] == _file_sha256(module)

    outside = tmp_path / "outside.py"
    outside.write_text("VALUE = 2\n")
    link = source / "unsafe.py"
    link.symlink_to(outside)
    hashes, unsafe = source_hashes(tmp_path)
    assert "src/a2a_mq/unsafe.py" in unsafe
    assert "src/a2a_mq/unsafe.py" not in hashes


def test_source_hashes_rejects_symlinked_source_directory(tmp_path):
    source = tmp_path / "src" / "a2a_mq"
    source.mkdir(parents=True)
    (source / "safe.py").write_text("VALUE = 1\n")
    outside = tmp_path / "outside-package"
    outside.mkdir()
    (outside / "evil.py").write_text("VALUE = 2\n")
    link = source / "linked"
    link.symlink_to(outside, target_is_directory=True)

    hashes, unsafe = source_hashes(tmp_path)

    assert "src/a2a_mq/linked" in unsafe
    assert "src/a2a_mq/safe.py" in hashes
    assert not any("evil.py" in name for name in hashes)


def test_evidence_manifest_hashes_nested_files_and_rejects_symlinks(tmp_path):
    import hashlib

    nested=tmp_path/"broker"/"logs"
    nested.mkdir(parents=True)
    payload=nested/"run.log"
    payload.write_bytes(b"evidence")
    (tmp_path/"verdict.json").write_text("{}")
    (tmp_path/"sha256.json").write_text("stale")
    hashes,unsafe=evidence_hashes(tmp_path)
    assert unsafe == []
    assert hashes["broker/logs/run.log"] == hashlib.sha256(b"evidence").hexdigest()
    assert "verdict.json" in hashes
    assert "sha256.json" not in hashes

    target=tmp_path/"outside"
    target.write_text("secret")
    link=tmp_path/"broker"/"unsafe-link"
    link.symlink_to(target)
    hashes,unsafe=evidence_hashes(tmp_path)
    assert "broker/unsafe-link" in unsafe
    assert "broker/unsafe-link" not in hashes


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
