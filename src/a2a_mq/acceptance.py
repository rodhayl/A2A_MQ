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
import subprocess
import sys
import time
import xml.etree.ElementTree as ET


GROUPS = {
    "local": {"selection":"not live and not postgres and not upstream and not mcp and not vault",
              "modules":["pytest", "pytest_asyncio", "cryptography", "jwt", "opentelemetry.sdk"]},
    "sdk": {"selection":"upstream or mcp", "modules":["a2a", "dapr.ext.workflow", "mcp", "httpx"]},
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
    if name=='postgres' and not env.get('A2AMQ_POSTGRES_DSN_FILE'):
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
        except (OSError, ValueError):
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


FULL_REQUIRED = frozenset({*GROUPS, "dependency-check", "lint", "types", "jetstream", "rabbitmq"})


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
    env={**os.environ,'PYTEST_DISABLE_PLUGIN_AUTOLOAD':'1','PYTHONDONTWRITEBYTECODE':'1',
         'A2AMQ_NO_LLM_TOKENS':'1'}
    env["PYTHONPATH"]=str(root/"src")
    env.pop('PYTEST_ADDOPTS',None)
    env.pop('PYTEST_PLUGINS',None)
    # An environment guard is not network isolation. All implemented fixtures
    # are model-free; externally restrict egress for an independent attestation.
    secret_names=[k for k,v in env.items() if v and k.upper() in {
        'OPENAI_API_KEY','ANTHROPIC_API_KEY','GEMINI_API_KEY','GOOGLE_API_KEY','AZURE_OPENAI_API_KEY'}]
    results=[]
    if secret_names:
        results.append({'name':'credential-guard','status':'FAILED','presentNames':sorted(secret_names)})
    else:
        for name in (GROUPS if args.mode=='full' else [args.mode]):
            results.append(test_gate(name,root,output,env,args.timeout))
        if args.mode=='full':
            for name,command in [('dependency-check',[sys.executable,'-m','pip','check']),
                                 ('lint',[sys.executable,'-m','ruff','check','.']),
                                 ('types',[sys.executable,'-m','mypy'])]:
                rc=run_command(command,root,output/(name+'.log'),args.timeout,env)
                results.append({'name':name,'status':'PASSED' if rc==0 else 'FAILED','returnCode':rc})
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
    for key,command in [('gitHead',['git','rev-parse','HEAD']),('gitStatus',['git','status','--porcelain'])]:
        try:
            report[key]=subprocess.check_output(command,cwd=root,text=True,timeout=5).strip()
        except (OSError,subprocess.SubprocessError):
            report[key]=None
    (output/'verdict.json').write_text(json.dumps(report,indent=2)+'\n')
    hashes={path.name:hashlib.sha256(path.read_bytes()).hexdigest() for path in output.iterdir() if path.is_file()}
    (output/'sha256.json').write_text(json.dumps(hashes,indent=2)+'\n')
    print(json.dumps({'status':report['status'],'results':results},indent=2))
    raise SystemExit(0 if report['status'].endswith('_PASS') else 2 if report['status']=='INCOMPLETE' else 1)


if __name__=='__main__':
    main()
