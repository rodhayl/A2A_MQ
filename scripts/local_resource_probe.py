#!/usr/bin/env python3
"""Measure the local A2A-MQ core before any broad or infrastructure tests."""
from __future__ import annotations

import argparse
import gc
import json
import os
from pathlib import Path
import shutil
import sys
import time


def _mib(value: int | float) -> float:
    return round(float(value) / (1024 * 1024), 3)


def _meminfo() -> dict[str, int]:
    values: dict[str, int] = {}
    try:
        for line in Path("/proc/meminfo").read_text().splitlines():
            name, raw = line.split(":", 1)
            first = raw.strip().split()[0]
            values[name] = int(first) * 1024
    except (OSError, ValueError, IndexError):
        pass
    return values


def _rss_bytes() -> int:
    try:
        for line in Path("/proc/self/status").read_text().splitlines():
            if line.startswith("VmRSS:"):
                return int(line.split()[1]) * 1024
    except (OSError, ValueError, IndexError):
        pass
    try:
        import resource
        value = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        return int(value * 1024 if sys.platform != "darwin" else value)
    except (ImportError, ValueError):
        return 0


def _state_bytes(path: Path) -> int:
    total = 0
    for candidate in (path, Path(str(path) + "-wal"), Path(str(path) + "-shm")):
        try:
            total += candidate.stat().st_size
        except OSError:
            pass
    return total


def _disk_free(path: Path) -> int:
    try:
        return shutil.disk_usage(path).free
    except OSError:
        return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state-dir", required=True)
    parser.add_argument("--json", dest="json_path", required=True)
    parser.add_argument("--tasks", type=int, default=100)
    parser.add_argument("--min-available-mib", type=float, default=512)
    parser.add_argument("--max-rss-mib", type=float, default=192)
    parser.add_argument("--max-idle-cpu-percent", type=float, default=3)
    parser.add_argument("--max-import-seconds", type=float, default=3)
    parser.add_argument("--max-smoke-seconds", type=float, default=8)
    parser.add_argument("--max-state-mib", type=float, default=16)
    args = parser.parse_args()

    if not 1 <= args.tasks <= 1000:
        parser.error("--tasks must be between 1 and 1000")

    root = Path(__file__).resolve().parents[1]
    state_dir = Path(args.state_dir).resolve()
    state_dir.mkdir(parents=True, exist_ok=True)
    report_path = Path(args.json_path).resolve()
    report_path.parent.mkdir(parents=True, exist_ok=True)

    before_mem = _meminfo()
    available_before = before_mem.get("MemAvailable", 0)
    swap_before = before_mem.get("SwapTotal", 0) - before_mem.get("SwapFree", 0)
    disk_before = _disk_free(state_dir)
    rss_start = _rss_bytes()
    failures: list[str] = []

    if available_before and _mib(available_before) < args.min_available_mib:
        result = {
            "verdict": "INCOMPLETE_HOST_HEADROOM",
            "reason": "available memory is below the conservative safety reserve",
            "availableMiB": _mib(available_before),
            "minimumAvailableMiB": args.min_available_mib,
            "diskFreeMiB": _mib(disk_before),
            "rssMiB": _mib(rss_start),
            "tasksExecuted": 0,
        }
        report_path.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n")
        print(json.dumps(result, sort_keys=True))
        return 2

    sys.path.insert(0, str(root / "src"))
    import_started = time.perf_counter()
    from a2a_mq.domain import command
    from a2a_mq.reference import echo_events
    from a2a_mq.storage import SQLiteStore
    import_seconds = time.perf_counter() - import_started
    rss_after_import = _rss_bytes()

    db_path = state_dir / "probe.sqlite"
    if db_path.exists():
        print("probe database already exists; refusing to overwrite", file=sys.stderr)
        return 2

    smoke_started = time.perf_counter()
    store = SQLiteStore(db_path, tenant="probe", consumer="probe")
    try:
        for index in range(args.tasks):
            request = command(f"probe-{index}", tenant="probe", skill="echo")
            claim = store.claim(request, "probe-worker")
            if claim.status != "acquired":
                raise RuntimeError(f"unexpected claim status: {claim.status}")
            result = echo_events(request)[-1]
            store.commit_claim(
                claim,
                effect_key=f"effect-{index}",
                effect={"index": index},
                result=result,
            )
        if len(store.list_tasks(limit=min(args.tasks, 200))) != min(args.tasks, 200):
            raise RuntimeError("task projection count mismatch")
    finally:
        pass
    smoke_seconds = time.perf_counter() - smoke_started

    gc.collect()
    rss_after_smoke = _rss_bytes()
    cpu_before = time.process_time()
    wall_before = time.perf_counter()
    time.sleep(2.0)
    idle_wall = max(time.perf_counter() - wall_before, 0.001)
    idle_cpu_percent = max(0.0, (time.process_time() - cpu_before) / idle_wall * 100.0)

    after_mem = _meminfo()
    available_after = after_mem.get("MemAvailable", 0)
    swap_after = after_mem.get("SwapTotal", 0) - after_mem.get("SwapFree", 0)
    state_size = _state_bytes(db_path)
    disk_after = _disk_free(state_dir)
    rss_peak = max(rss_start, rss_after_import, rss_after_smoke)

    checks = {
        "rss": _mib(rss_peak) <= args.max_rss_mib,
        "idleCpu": idle_cpu_percent <= args.max_idle_cpu_percent,
        "imports": import_seconds <= args.max_import_seconds,
        "smoke": smoke_seconds <= args.max_smoke_seconds,
        "state": _mib(state_size) <= args.max_state_mib,
        "swap": swap_after <= swap_before,
    }
    failures.extend(name for name, passed in checks.items() if not passed)

    result = {
        "verdict": "PASS_CORE_LIGHT" if not failures else "FAIL_CORE_LIGHT",
        "thresholds": {
            "maxRssMiB": args.max_rss_mib,
            "maxIdleCpuPercent": args.max_idle_cpu_percent,
            "maxImportSeconds": args.max_import_seconds,
            "maxSmokeSeconds": args.max_smoke_seconds,
            "maxStateMiB": args.max_state_mib,
            "minAvailableMiB": args.min_available_mib,
        },
        "observed": {
            "rssStartMiB": _mib(rss_start),
            "rssAfterImportMiB": _mib(rss_after_import),
            "rssAfterSmokeMiB": _mib(rss_after_smoke),
            "rssPeakObservedMiB": _mib(rss_peak),
            "idleCpuPercentOneCore": round(idle_cpu_percent, 3),
            "importSeconds": round(import_seconds, 4),
            "smokeSeconds": round(smoke_seconds, 4),
            "stateMiB": _mib(state_size),
            "availableBeforeMiB": _mib(available_before) if available_before else None,
            "availableAfterMiB": _mib(available_after) if available_after else None,
            "swapDeltaMiB": _mib(max(0, swap_after - swap_before)),
            "diskFreeBeforeMiB": _mib(disk_before),
            "diskFreeAfterMiB": _mib(disk_after),
            "tasksExecuted": args.tasks,
        },
        "failedChecks": failures,
        "scope": "Python core + SQLite only; no Dapr, broker, Workflow control plane, PostgreSQL, Docker, Vault or OTLP",
    }
    report_path.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n")
    print(json.dumps(result, sort_keys=True))
    return 0 if not failures else 3


if __name__ == "__main__":
    raise SystemExit(main())
