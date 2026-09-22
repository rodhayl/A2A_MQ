#!/usr/bin/env python3
"""Measure the real local-lite Python app without starting daprd or nats-server."""
from __future__ import annotations

import argparse
import asyncio
import gc
import json
from pathlib import Path
import resource
import sys
import time


def _mib(value: int | float) -> float:
    return round(float(value) / (1024 * 1024), 3)


def _rss_bytes() -> int:
    try:
        for line in Path("/proc/self/status").read_text().splitlines():
            if line.startswith("VmRSS:"):
                return int(line.split()[1]) * 1024
    except (OSError, ValueError, IndexError):
        pass
    return _peak_rss_bytes()


def _peak_rss_bytes() -> int:
    try:
        value = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        return int(value if sys.platform == "darwin" else value * 1024)
    except (ValueError, OSError):
        return 0


def _tree_bytes(path: Path) -> int:
    if not path.exists():
        return 0
    total = 0
    for candidate in path.rglob("*"):
        try:
            if candidate.is_file() and not candidate.is_symlink():
                total += candidate.stat().st_size
        except OSError:
            pass
    return total


async def _measure(app, idle_seconds: float) -> tuple[float, int]:
    async with app.router.lifespan_context(app):
        cpu_started = time.process_time()
        wall_started = time.perf_counter()
        await asyncio.sleep(idle_seconds)
        elapsed = max(time.perf_counter() - wall_started, 0.001)
        cpu_percent = max(0.0, (time.process_time() - cpu_started) / elapsed * 100.0)
        rss = max(_rss_bytes(), _peak_rss_bytes())
    return cpu_percent, rss


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state-dir", type=Path, required=True)
    parser.add_argument("--json", dest="json_path", type=Path, required=True)
    parser.add_argument("--idle-seconds", type=float, default=2.0)
    parser.add_argument("--max-rss-mib", type=float, default=192)
    parser.add_argument("--max-idle-cpu-percent", type=float, default=3)
    parser.add_argument("--max-build-seconds", type=float, default=5)
    parser.add_argument("--max-state-mib", type=float, default=4)
    args = parser.parse_args()
    if not 0.5 <= args.idle_seconds <= 30:
        parser.error("--idle-seconds must be between 0.5 and 30")

    root = Path(__file__).resolve().parents[1]
    state = args.state_dir.resolve()
    report = args.json_path.resolve()
    if state.exists() or report.exists():
        raise SystemExit("state/evidence output already exists; refusing to overwrite")
    state.mkdir(parents=True, mode=0o700)
    report.parent.mkdir(parents=True, exist_ok=True)
    sys.path.insert(0, str(root / "src"))

    started = time.perf_counter()
    from a2a_mq.local_lite import LocalLiteSettings, create_app
    from a2a_mq.local_runtime import _runtime_environments, prepare

    profile_root = state / "profile"
    profile = prepare(profile_root)
    app_env, _, _ = _runtime_environments(profile_root, profile)
    app = create_app(LocalLiteSettings.from_env(app_env))
    build_seconds = time.perf_counter() - started

    gc.collect()
    idle_cpu, rss = asyncio.run(_measure(app, args.idle_seconds))
    state_bytes = _tree_bytes(profile_root / "state")
    checks = {
        "rss": _mib(rss) <= args.max_rss_mib,
        "idleCpu": idle_cpu <= args.max_idle_cpu_percent,
        "build": build_seconds <= args.max_build_seconds,
        "state": _mib(state_bytes) <= args.max_state_mib,
    }
    failures = [name for name, passed in checks.items() if not passed]
    result = {
        "verdict": "PASS_LOCAL_PYTHON" if not failures else "FAIL_LOCAL_PYTHON",
        "thresholds": {
            "maxRssMiB": args.max_rss_mib,
            "maxIdleCpuPercentOneCore": args.max_idle_cpu_percent,
            "maxBuildSeconds": args.max_build_seconds,
            "maxStateMiB": args.max_state_mib,
        },
        "observed": {
            "rssMiB": _mib(rss),
            "idleCpuPercentOneCore": round(idle_cpu, 3),
            "buildSeconds": round(build_seconds, 4),
            "stateMiB": _mib(state_bytes),
        },
        "failedChecks": failures,
        "scope": "local-lite Python app + SQLite/background loops; no daprd or nats-server",
    }
    with report.open("x", encoding="utf-8") as stream:
        stream.write(json.dumps(result, indent=2, sort_keys=True) + "\n")
    print(json.dumps(result, sort_keys=True))
    return 0 if not failures else 3


if __name__ == "__main__":
    raise SystemExit(main())
