#!/usr/bin/env python3
"""Measure the real local-lite Python app without starting daprd or nats-server."""
from __future__ import annotations
import argparse, asyncio, gc, json, os, resource, sys, time
from pathlib import Path

def _mib(value): return round(float(value)/(1024*1024),3)

def _rss_bytes():
    try:
        for line in Path("/proc/self/status").read_text().splitlines():
            if line.startswith("VmRSS:"):
                return int(line.split()[1])*1024
    except (OSError, ValueError, IndexError):
        pass
    return _peak_rss_bytes()

def _peak_rss_bytes():
    try:
        value=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        return int(value if sys.platform=="darwin" else value*1024)
    except (ValueError,OSError):
        return 0

def _tree_bytes(path):
    path=Path(path)
    if not path.exists(): return 0
    total=0
    for candidate in path.rglob("*"):
        try:
            if candidate.is_file() and not candidate.is_symlink(): total+=candidate.stat().st_size
        except OSError: pass
    return total

async def _measure(app,idle_seconds):
    async with app.router.lifespan_context(app):
        cpu=time.process_time(); wall=time.perf_counter()
        await asyncio.sleep(idle_seconds)
        elapsed=max(time.perf_counter()-wall,.001)
        percent=max(0.0,(time.process_time()-cpu)/elapsed*100.0)
        rss=max(_rss_bytes(),_peak_rss_bytes())
    return percent,rss

def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument("--state-dir",type=Path,required=True); p.add_argument("--json",dest="json_path",type=Path,required=True)
    p.add_argument("--idle-seconds",type=float,default=2.0); p.add_argument("--max-rss-mib",type=float,default=192)
    p.add_argument("--max-idle-cpu-percent",type=float,default=3); p.add_argument("--max-build-seconds",type=float,default=5)
    p.add_argument("--max-state-mib",type=float,default=4)
    a=p.parse_args()
    if not .5<=a.idle_seconds<=30: p.error("--idle-seconds must be between 0.5 and 30")
    root=Path(__file__).resolve().parents[1]; state=a.state_dir.resolve(); report=a.json_path.resolve()
    if state.exists() or report.exists(): raise SystemExit("state/evidence output already exists; refusing to overwrite")
    state.mkdir(parents=True,mode=0o700); report.parent.mkdir(parents=True,exist_ok=True)
    sys.path.insert(0,str(root/"src"))
    started=time.perf_counter()
    from a2a_mq.local_runtime import _runtime_environments,prepare
    from a2a_mq.local_lite import LocalLiteSettings,create_app
    profile_root=state/"profile"; profile=prepare(profile_root)
    app_env,_,_=_runtime_environments(profile_root,profile)
    app=create_app(LocalLiteSettings.from_env(app_env))
    build=time.perf_counter()-started
    gc.collect(); idle_cpu,rss=asyncio.run(_measure(app,a.idle_seconds))
    state_bytes=_tree_bytes(profile_root/"state")
    checks={"rss":_mib(rss)<=a.max_rss_mib,"idleCpu":idle_cpu<=a.max_idle_cpu_percent,
            "build":build<=a.max_build_seconds,"state":_mib(state_bytes)<=a.max_state_mib}
    failures=[k for k,v in checks.items() if not v]
    result={"verdict":"PASS_LOCAL_PYTHON" if not failures else "FAIL_LOCAL_PYTHON",
      "thresholds":{"maxRssMiB":a.max_rss_mib,"maxIdleCpuPercentOneCore":a.max_idle_cpu_percent,
                    "maxBuildSeconds":a.max_build_seconds,"maxStateMiB":a.max_state_mib},
      "observed":{"rssMiB":_mib(rss),"idleCpuPercentOneCore":round(idle_cpu,3),
                  "buildSeconds":round(build,4),"stateMiB":_mib(state_bytes)},
      "failedChecks":failures,
      "scope":"local-lite Python app + SQLite/background loops; no daprd or nats-server"}
    with report.open("x",encoding="utf-8") as stream: stream.write(json.dumps(result,indent=2,sort_keys=True)+"\n")
    print(json.dumps(result,sort_keys=True)); return 0 if not failures else 3

if __name__=="__main__": raise SystemExit(main())
