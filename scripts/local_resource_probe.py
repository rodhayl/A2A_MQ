#!/usr/bin/env python3
"""Measure the local A2A-MQ core before any broad or infrastructure tests."""
from __future__ import annotations

import argparse
import gc
import json
import math
import os
import resource
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
    return _peak_rss_bytes()


def _peak_rss_bytes() -> int:
    try:
        value = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        return int(value * 1024 if sys.platform != "darwin" else value)
    except (ValueError, OSError):
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



def _memory_known(values: dict[str, int]) -> bool:
    keys = ("MemAvailable", "SwapTotal", "SwapFree")
    return (all(type(values.get(key)) is int and values[key] >= 0 for key in keys)
            and values["SwapFree"] <= values["SwapTotal"])


def _verify_task_count(store, expected: int) -> None:
    """Verify all pages, including probes larger than the storage page limit."""
    seen: set[str] = set()
    cursors: set[str] = set()
    cursor = None
    while True:
        page, next_cursor = store.list_tasks(limit=min(expected, 200), after=cursor)
        for task in page:
            if task["id"] in seen:
                raise RuntimeError("duplicate task projection")
            seen.add(task["id"])
        if len(seen) > expected:
            raise RuntimeError("task projection count mismatch")
        if next_cursor is None:
            break
        if not page or next_cursor in cursors:
            raise RuntimeError("task projection cursor did not advance")
        cursors.add(next_cursor)
        cursor = next_cursor
    if len(seen) != expected:
        raise RuntimeError("task projection count mismatch")


def _measure(args, state_dir: Path) -> tuple[int, dict]:
    before_mem = _meminfo()
    disk_before = _disk_free(state_dir)
    rss_start = _rss_bytes()
    if not _memory_known(before_mem) or disk_before <= 0 or rss_start <= 0:
        return 2, {"verdict": "INCOMPLETE_MEASUREMENT", "tasksExecuted": 0,
                   "reason": "required host memory, RSS or disk measurement unavailable"}
    available_before = before_mem["MemAvailable"]
    swap_before = before_mem["SwapTotal"] - before_mem["SwapFree"]
    if available_before < args.min_available_mib * 1024**2:
        return 2, {
            "verdict": "INCOMPLETE_HOST_HEADROOM",
            "reason": "available memory is below the conservative safety reserve",
            "availableMiB": _mib(available_before),
            "minimumAvailableMiB": args.min_available_mib,
            "diskFreeMiB": _mib(disk_before), "rssMiB": _mib(rss_start), "tasksExecuted": 0,
        }

    sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
    import_started = time.perf_counter()
    from a2a_mq.domain import command
    from a2a_mq.reference import echo_events
    from a2a_mq.storage import SQLiteStore
    import_seconds = time.perf_counter() - import_started
    rss_after_import = _rss_bytes()

    db_path = state_dir / "probe.sqlite"
    smoke_started = time.perf_counter()
    fd = os.open(db_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    os.close(fd)
    store = SQLiteStore(db_path, tenant="probe", consumer="probe")
    for index in range(args.tasks):
        request = command(f"probe-{index}", tenant="probe", skill="echo")
        claim = store.claim(request, "probe-worker")
        if claim.status != "acquired":
            raise RuntimeError("unexpected claim status")
        result = echo_events(request)[-1]
        store.commit_claim(claim, effect_key=f"effect-{index}", effect={"index": index}, result=result)
    _verify_task_count(store, args.tasks)
    smoke_seconds = time.perf_counter() - smoke_started

    gc.collect()
    rss_after_smoke = _rss_bytes()
    cpu_before = time.process_time()
    wall_before = time.perf_counter()
    time.sleep(2.0)
    idle_wall = max(time.perf_counter() - wall_before, 0.001)
    idle_cpu_percent = max(0.0, (time.process_time() - cpu_before) / idle_wall * 100.0)
    after_mem = _meminfo()
    state_size = _state_bytes(db_path)
    disk_after = _disk_free(state_dir)
    peak = _peak_rss_bytes()
    if (not _memory_known(after_mem) or min(rss_after_import, rss_after_smoke, peak) <= 0
            or state_size <= 0 or disk_after <= 0):
        return 2, {"verdict": "INCOMPLETE_MEASUREMENT", "tasksExecuted": args.tasks,
                   "reason": "required final memory, RSS, state or disk measurement unavailable"}
    available_after = after_mem["MemAvailable"]
    swap_after = after_mem["SwapTotal"] - after_mem["SwapFree"]
    rss_peak = max(rss_start, rss_after_import, rss_after_smoke, peak)
    # Compare unrounded values; rounding is for presentation only.
    checks = {
        "rss": rss_peak <= args.max_rss_mib * 1024**2,
        "idleCpu": idle_cpu_percent <= args.max_idle_cpu_percent,
        "imports": import_seconds <= args.max_import_seconds,
        "smoke": smoke_seconds <= args.max_smoke_seconds,
        "state": state_size <= args.max_state_mib * 1024**2,
    }
    failures = [name for name, passed in checks.items() if not passed]
    environmental = []
    if available_after < args.min_available_mib * 1024**2:
        environmental.append("hostHeadroom")
    if swap_after > swap_before:
        environmental.append("hostSwapActivity")
    verdict = "FAIL_CORE_LIGHT" if failures else "PASS_CORE_LIGHT"
    if environmental and not failures:
        verdict = "INCOMPLETE_HOST_HEADROOM"
    result = {
        "verdict": verdict,
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
            "availableBeforeMiB": _mib(available_before),
            "availableAfterMiB": _mib(available_after),
            "swapDeltaMiB": _mib(max(0, swap_after - swap_before)),
            "diskFreeBeforeMiB": _mib(disk_before),
            "diskFreeAfterMiB": _mib(disk_after),
            "tasksExecuted": args.tasks,
        },
        "failedChecks": failures,
        "environmentalChecks": environmental,
        "swapMeasurementScope": "host-wide; not attributable to this process",
        "scope": "Core fixture + SQLite only. Idle CPU measures this probe sleeping, not the running bridge or Dapr/NATS.",
    }
    return (3 if failures else 2 if environmental else 0), result


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
    budgets = (args.min_available_mib, args.max_rss_mib, args.max_idle_cpu_percent,
               args.max_import_seconds, args.max_smoke_seconds, args.max_state_mib)
    if any(not math.isfinite(value) or value < 0 for value in budgets):
        parser.error("resource budgets must be finite and nonnegative")

    state_dir = Path(args.state_dir).absolute()
    report_path = Path(args.json_path).absolute()
    if state_dir.is_symlink() or report_path.is_symlink():
        parser.error("state and report paths must not be symlinks")
    state_dir = state_dir.resolve()
    report_path = report_path.resolve()
    db_paths = {state_dir / ("probe.sqlite" + suffix) for suffix in ("", "-wal", "-shm")}
    if report_path in db_paths:
        parser.error("report path conflicts with the probe database")
    if report_path.exists() or any(p.exists() or p.is_symlink() for p in db_paths):
        parser.error("probe state or report already exists; use fresh paths")
    state_dir.mkdir(parents=True, exist_ok=True)
    report_path.parent.mkdir(parents=True, exist_ok=True)
    flags = os.O_RDWR | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0)
    try:
        fd = os.open(report_path, flags, 0o600)
    except OSError:
        parser.error("cannot create a new private report")
    with os.fdopen(fd, "w+", encoding="utf-8") as report:
        # A terminated run leaves an explicitly incomplete artifact, never an old PASS.
        report.write('{"verdict":"INCOMPLETE_CORE_PROBE"}\n')
        report.flush()
        os.fsync(report.fileno())
        try:
            code, result = _measure(args, state_dir)
        except Exception as error:
            code, result = 3, {"verdict": "FAIL_CORE_LIGHT", "failedChecks": ["execution"],
                               "errorType": type(error).__name__,
                               "reason": "probe execution failed; no economic acceptance established"}
        encoded = json.dumps(result, indent=2, sort_keys=True, allow_nan=False) + "\n"
        report.seek(0)
        report.write(encoded)
        report.truncate()
        report.flush()
        os.fsync(report.fileno())
    print(json.dumps(result, sort_keys=True, allow_nan=False))
    return code


if __name__ == "__main__":
    raise SystemExit(main())
