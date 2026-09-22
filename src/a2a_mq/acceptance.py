"""Run explicit implementation gates; unavailable prerequisites never become passes.

LOCAL_PASS is limited to broker-independent repository tests. FULL_PASS requires
local, native SDK, PostgreSQL, Vault and both disposable real-broker runs. Neither verdict
is a production certificate or an upstream binding-conformance certification.
"""
from __future__ import annotations

import argparse
import hashlib
import importlib.metadata
import importlib.util
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import time
import tarfile
import xml.etree.ElementTree as ET

from .domain import PermanentError


MODEL_CREDENTIAL = re.compile(
    r"^(?:OPENAI|ANTHROPIC|AZURE_OPENAI|GEMINI|GOOGLE|MISTRAL|COHERE|GROQ|"
    r"DEEPSEEK|TOGETHER|OPENROUTER|HUGGING_FACE|HF_|XAI|CEREBRAS|REPLICATE).*(?:KEY|TOKEN|CREDENTIAL)",
    re.I,
)


GROUPS = {
    "local": {"selection":"not live and not postgres and not upstream and not mcp and not vault",
              "modules":["pytest", "pytest_asyncio", "cryptography", "jwt", "opentelemetry.sdk"]},
    # MCP 2.x Streamable HTTP intentionally uses the transitive httpx2 package.
    "sdk": {"selection":"upstream or mcp", "modules":["a2a", "dapr.ext.workflow", "mcp", "httpx2"]},
    "postgres": {"selection":"postgres", "modules":["psycopg", "psycopg_pool"]},
    "vault": {"selection":"vault", "modules":["cryptography"]},
}


def present(name):
    try:
        return importlib.util.find_spec(name) is not None
    except (ModuleNotFoundError, ValueError):
        return False


def junit(path):
    """Count actual outcomes, validate summaries and reject empty/skipped reports."""
    raw=path.read_bytes()
    if len(raw)>8*1024*1024 or b'<!DOCTYPE' in raw.upper() or b'<!ENTITY' in raw.upper():
        raise ValueError('INVALID_JUNIT')
    text=raw.decode("utf-8-sig", errors="strict")
    if "\x00" in text:
        raise ValueError("INVALID_JUNIT")
    root=ET.fromstring(text)
    if root.tag not in {"testsuite", "testsuites"}:
        raise ValueError("INVALID_JUNIT_ROOT")
    parents={child:parent for parent in root.iter() for child in parent}
    cases=list(root.iter('testcase'))
    identities=set()
    for case in cases:
        identity=(case.get("classname", ""),case.get("name", ""))
        if not identity[1] or identity in identities or parents[case].tag!="testsuite":
            raise ValueError("INVALID_JUNIT_CASE")
        identities.add(identity)
        if sum(child.tag in {"failure","error","skipped"} for child in case)>1:
            raise ValueError("INVALID_JUNIT_OUTCOME")
    for node in root.iter():
        if node.tag in {"failure","error","skipped"} and (node not in parents or parents[node].tag!="testcase"):
            raise ValueError("INVALID_JUNIT_OUTCOME")
    actual={"tests":len(cases),**{k:sum(c.find(tag) is not None for c in cases)
                                for k,tag in [('failures','failure'),('errors','error'),('skipped','skipped')]}}
    if not cases or any(actual[k] for k in ('failures','errors','skipped')):
        raise ValueError('INCOMPLETE_JUNIT')
    for suite in root.iter():
        if suite.tag not in {'testsuite','testsuites'}:
            continue
        nested=list(suite.iter('testcase'))
        for key,tag in [('tests',None),('failures','failure'),('errors','error'),('skipped','skipped')]:
            expected=len(nested) if tag is None else sum(c.find(tag) is not None for c in nested)
            if key in suite.attrib and suite.attrib[key]!=str(expected):
                raise ValueError('INCONSISTENT_JUNIT')
    return actual


def run_command(args,root,log,timeout,env):
    with log.open('wb') as stream:
        try:
            process=subprocess.run(args,cwd=root,env=env,stdout=stream,stderr=subprocess.STDOUT,
                                   timeout=timeout,check=False)
            return process.returncode
        except (OSError,subprocess.TimeoutExpired) as error:
            stream.write(('\n'+type(error).__name__+'\n').encode())
            return 124


def test_gate(name,root,output,env,timeout):
    group=GROUPS[name]
    missing=[m for m in group['modules'] if not present(m)]
    if name == 'postgres':
        # The acceptance gate is always verified-TLS. A direct pytest CI fixture may
        # opt into local sslmode=disable, but that override cannot satisfy acceptance.
        env = {**env}
        env.pop('A2AMQ_POSTGRES_TEST_ALLOW_INSECURE', None)
        if not env.get('A2AMQ_POSTGRES_DSN_FILE'):
            missing.append('A2AMQ_POSTGRES_DSN_FILE')
    if name=='vault' and not env.get('A2AMQ_VAULT_TEST_CONFIG_FILE'):
        missing.append('A2AMQ_VAULT_TEST_CONFIG_FILE')
    if missing:
        return {"name":name,"status":"UNAVAILABLE","missing":missing}
    if name == "postgres" and (root / "tests/test_postgres_live.py").exists():
        # The concurrently added gate uses an environment DSN. Keep both suites
        # on the same explicitly supplied disposable database; do not log it.
        try:
            from .configuration import read_file
            dsn = read_file(env["A2AMQ_POSTGRES_DSN_FILE"], secret=True, limit=8192).strip()
            prior = env.get("A2A_TEST_POSTGRES_DSN")
            if not dsn or "\x00" in dsn or (prior and prior != dsn):
                return {"name":name,"status":"FAILED","configurationError":"POSTGRES_GATE_DSN_CONFLICT"}
            env = {**env, "A2A_TEST_POSTGRES_DSN":dsn}
        except (OSError, ValueError, PermanentError):
            return {"name":name,"status":"FAILED","configurationError":"INVALID_POSTGRES_GATE_FILE"}
    report=output/(name+'.junit.xml')
    args=[sys.executable,'-m','pytest','-p','pytest_asyncio.plugin','-q','-W','error',
          '-m',group['selection'],'--junitxml='+str(report)]
    result={"name":name,"command":args,"status":"FAILED"}
    result['returnCode']=run_command(args,root,output/(name+'.log'),timeout,env)
    try:
        result['junit']=junit(report)
        if result['returnCode']==0:
            result['status']='PASSED'
    except (OSError,ValueError,ET.ParseError):
        result['evidenceError']='MISSING_OR_INCOMPLETE_JUNIT'
    return result


FULL_REQUIRED = frozenset({*GROUPS, "repository-check", "dependency-check", "lint", "types", "package", "jetstream", "rabbitmq"})



def package_gate(root, output, env, timeout):
    """Build and import the exact committed wheel entirely outside the checkout."""
    archive=output/"package-source.tar"
    source=output/"package-source"
    wheels=output/"package-wheel"
    installed=output/"package-installed"
    source.mkdir(mode=0o700)
    wheels.mkdir(mode=0o700)
    installed.mkdir(mode=0o700)
    result={"name":"package","status":"FAILED"}
    archive_cmd=["git","archive","--format=tar","HEAD","-o",str(archive)]
    result["archiveReturnCode"]=run_command(archive_cmd,root,output/"package-archive.log",timeout,env)
    if result["archiveReturnCode"] != 0:
        return result
    try:
        with tarfile.open(archive,"r:") as bundle:
            bundle.extractall(source,filter="data")
    except (OSError,tarfile.TarError):
        result["evidenceError"]="INVALID_GIT_ARCHIVE"
        return result
    wheel_cmd=[sys.executable,"-m","pip","wheel","--no-deps",
               "--wheel-dir",str(wheels),str(source)]
    result["wheelReturnCode"]=run_command(wheel_cmd,output,output/"package-wheel.log",timeout,env)
    candidates=list(wheels.glob("a2a_mq-*.whl"))
    if result["wheelReturnCode"] != 0 or len(candidates) != 1:
        result["evidenceError"]="WHEEL_BUILD_FAILED"
        return result
    wheel=candidates[0]
    install_cmd=[sys.executable,"-m","pip","install","--no-deps","--target",str(installed),str(wheel)]
    result["installReturnCode"]=run_command(install_cmd,output,output/"package-install.log",timeout,env)
    if result["installReturnCode"] != 0:
        return result
    smoke=(
        "import os,sys; from pathlib import Path; "
        "target=Path(os.environ['A2AMQ_WHEEL_TARGET']).resolve(); "
        "sys.path.insert(0,str(target)); import a2a_mq; "
        "from importlib.resources import files; import a2a_mq.binding as b; "
        "assert Path(a2a_mq.__file__).resolve().is_relative_to(target); "
        "assert b.load_binding()['protocolVersion']=='1.0'; "
        "assert files('a2a_mq').joinpath('schema.json').is_file()"
    )
    child={**env,"A2AMQ_WHEEL_TARGET":str(installed)}
    smoke_cmd=[sys.executable,"-I","-c",smoke]
    result["smokeReturnCode"]=run_command(smoke_cmd,output,output/"package-smoke.log",timeout,child)
    if result["smokeReturnCode"] == 0:
        result.update(status="PASSED",wheel=wheel.name,
                      wheelSha256=hashlib.sha256(wheel.read_bytes()).hexdigest(),
                      wheelBytes=wheel.stat().st_size)
    return result


def evidence_hashes(root):
    """Hash every regular evidence file recursively and report unsafe path types."""
    hashes={}
    unsafe=[]
    for path in sorted(root.rglob("*")):
        relative=path.relative_to(root).as_posix()
        if relative == "sha256.json":
            continue
        if path.is_symlink():
            unsafe.append(relative)
        elif path.is_file():
            hashes[relative]=hashlib.sha256(path.read_bytes()).hexdigest()
        elif not path.is_dir():
            unsafe.append(relative)
    return hashes, unsafe


def full_status(results,requested):
    required = FULL_REQUIRED if requested == "full" else frozenset({requested})
    names = [r.get("name") for r in results]
    if any(r['status']=='FAILED' for r in results):
        return 'FAILED'
    if not required.issubset(names) or len(names) != len(set(names)):
        return 'INCOMPLETE'
    if any(r['status']!='PASSED' for r in results):
        return 'INCOMPLETE'
    return 'FULL_PASS' if requested=='full' else requested.upper()+'_PASS'



def git_snapshot(root):
    """Return one clean exact Git HEAD/tree snapshot, or None when provenance is unusable."""
    try:
        head = subprocess.check_output(["git","rev-parse","HEAD"],cwd=root,text=True,timeout=5).strip()
        tree = subprocess.check_output(["git","rev-parse","HEAD^{tree}"],cwd=root,text=True,timeout=5).strip()
        status = subprocess.check_output(["git","status","--porcelain=v1","--untracked-files=all"],
                                         cwd=root,text=True,timeout=5).strip()
    except (OSError, subprocess.SubprocessError):
        return None
    if (status or re.fullmatch(r"[0-9a-f]{40}", head) is None
            or re.fullmatch(r"[0-9a-f]{40}", tree) is None):
        return None
    return {"head":head,"tree":tree}


def provenance_status(status, initial, final):
    """Bind a passing verdict to the same clean exact Git tree before and after execution."""
    if status == "FAILED":
        return status
    return status if initial is not None and final == initial else "INCOMPLETE"


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--mode',choices=['local','sdk','postgres','vault','full'],default='full')
    p.add_argument('--root',type=Path,default=Path.cwd())
    p.add_argument('--output',type=Path,required=True)
    p.add_argument('--timeout',type=int,default=300)
    p.add_argument('--allow-disposable-containers',action='store_true')
    args=p.parse_args()
    root=args.root.resolve()
    output=args.output.resolve()
    if output.exists() or output==root or root in output.parents or not 1<=args.timeout<=7200:
        p.error('Use a new external output directory and a bounded timeout')
    output.mkdir(parents=True,mode=0o700)
    initial_git=git_snapshot(root)
    env={**os.environ,'PYTEST_DISABLE_PLUGIN_AUTOLOAD':'1','PYTHONDONTWRITEBYTECODE':'1',
         'A2AMQ_NO_LLM_TOKENS':'1'}
    env["PYTHONPATH"]=str(root/"src")
    env.pop('PYTEST_ADDOPTS',None)
    env.pop('PYTEST_PLUGINS',None)
    # An environment guard is not network isolation. All implemented fixtures
    # are model-free; externally restrict egress for an independent attestation.
    secret_names=[k for k,v in env.items() if v and MODEL_CREDENTIAL.match(k)]
    results=[]
    if initial_git is None:
        results.append({'name':'provenance-guard','status':'UNAVAILABLE',
                        'missing':['clean exact git checkout']})
    elif secret_names:
        results.append({'name':'credential-guard','status':'FAILED','presentNames':sorted(secret_names)})
    else:
        for name in (GROUPS if args.mode=='full' else [args.mode]):
            results.append(test_gate(name,root,output,env,args.timeout))
        if args.mode=='full':
            for name,command in [('repository-check',[sys.executable,'tools/check.py']),
                                 ('dependency-check',[sys.executable,'-m','pip','check']),
                                 ('lint',[sys.executable,'-m','ruff','check','.']),
                                 ('types',[sys.executable,'-m','mypy'])]:
                rc=run_command(command,root,output/(name+'.log'),args.timeout,env)
                results.append({'name':name,'status':'PASSED' if rc==0 else 'FAILED','returnCode':rc})
            results.append(package_gate(root,output,env,args.timeout))
            for broker in ('jetstream','rabbitmq'):
                if not args.allow_disposable_containers:
                    results.append({'name':broker,'status':'UNAVAILABLE','missing':['explicit container opt-in']})
                    continue
                command=[sys.executable,'-m','a2a_mq.live_probe','--broker',broker,'--source',str(root),
                         '--output',str(output/broker),'--allow-disposable-containers']
                rc=run_command(command,root,output/(broker+'.log'),2400,env)
                record={'name':broker,'status':'FAILED','returnCode':rc}
                try:
                    verdict=json.loads((output/broker/'live-verdict.json').read_text())
                    if rc==0 and verdict['status']=='PASSED':
                        record.update(status='PASSED',checks=verdict['checks'])
                except (OSError,KeyError,ValueError):
                    record['evidenceError']='MISSING_LIVE_VERDICT'
                results.append(record)
    source={str(path.relative_to(root)):hashlib.sha256(path.read_bytes()).hexdigest()
            for path in sorted((root/'src').rglob('*.py'))}
    report={'status':full_status(results,args.mode),'requestedMode':args.mode,'results':results,
            'python':sys.version,'productionCertified':False,'upstreamBindingCertified':False,
            'timestamp':time.time(),'sourceSha256':source,
            'dependencies':{d.metadata['Name']:d.version for d in importlib.metadata.distributions()
                            if d.metadata.get('Name')}}
    final_git=git_snapshot(root)
    report['gitHead']=None if final_git is None else final_git['head']
    report['gitTree']=None if final_git is None else final_git['tree']
    report['initialGitHead']=None if initial_git is None else initial_git['head']
    report['initialGitTree']=None if initial_git is None else initial_git['tree']
    report['gitStatus']="" if final_git is not None else None
    report['status']=provenance_status(report['status'],initial_git,final_git)
    verdict_path=output/'verdict.json'
    verdict_path.write_text(json.dumps(report,indent=2)+'\n')
    hashes,unsafe=evidence_hashes(output)
    if unsafe:
        if report['status'] != 'FAILED':
            report['status']='INCOMPLETE'
        report['evidenceError']='UNSAFE_EVIDENCE_PATHS'
        report['unsafeEvidencePaths']=unsafe
        verdict_path.write_text(json.dumps(report,indent=2)+'\n')
        hashes,_=evidence_hashes(output)
    (output/'sha256.json').write_text(json.dumps(hashes,indent=2)+'\n')
    print(json.dumps({'status':report['status'],'results':results},indent=2))
    raise SystemExit(0 if report['status'].endswith('_PASS') else 2 if report['status']=='INCOMPLETE' else 1)


if __name__=='__main__':
    main()
