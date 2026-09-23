#!/usr/bin/env python3
"""Measure the real local-lite Python app without starting daprd or nats-server."""
from __future__ import annotations

import argparse
import asyncio
import gc
import json
import math
import os
from pathlib import Path
import resource
import stat
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


class MeasurementUnavailable(RuntimeError):
    """A required local-process reading is unavailable and cannot count as zero."""


def _tree_bytes(path: Path) -> int:
    if not path.is_dir() or path.is_symlink():
        raise MeasurementUnavailable("runtime state directory unavailable or unsafe")
    total = 0
    try:
        for candidate in path.rglob("*"):
            if candidate.is_symlink():
                raise MeasurementUnavailable("symlinked runtime state cannot be measured")
            info = candidate.stat()
            if stat.S_ISREG(info.st_mode):
                if candidate.suffix == ".tmp":
                    continue
                total += info.st_size
    except MeasurementUnavailable:
        raise
    except OSError:
        raise MeasurementUnavailable("runtime state measurement unavailable") from None
    return total


async def _measure(app, idle_seconds: float) -> tuple[float, int]:
    async with app.router.lifespan_context(app):
        bus = getattr(app.state, "a2amq_bus", None)
        if bus is None or not getattr(bus, "_owns_client", False):
            raise MeasurementUnavailable("owned Dapr publisher unavailable")
        # Warm the owned HTTP client without network I/O so RSS represents the
        # bridge after its first sidecar interaction while daprd remains absent.
        bus._http_client()
        cpu_started = time.process_time()
        wall_started = time.perf_counter()
        await asyncio.sleep(idle_seconds)
        elapsed = max(time.perf_counter() - wall_started, 0.001)
        cpu_percent = max(0.0, (time.process_time() - cpu_started) / elapsed * 100.0)
        rss = max(_rss_bytes(), _peak_rss_bytes())
    return cpu_percent, rss


def _run(args, state: Path) -> tuple[int, dict]:
    root = Path(__file__).resolve().parents[1]
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
    if rss <= 0 or state_bytes <= 0:
        raise MeasurementUnavailable("required RSS or runtime state measurement unavailable")

    checks = {
        "rss": rss <= args.max_rss_mib * 1024**2,
        "idleCpu": idle_cpu <= args.max_idle_cpu_percent,
        "build": build_seconds <= args.max_build_seconds,
        "state": state_bytes <= args.max_state_mib * 1024**2,
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
        "scope": "warmed local-lite Python app + SQLite/background loops; no daprd or nats-server",
    }
    return (0 if not failures else 3), result


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state-dir", type=Path, required=True)
    parser.add_argument("--json", dest="json_path", type=Path, required=True)
    parser.add_argument("--idle-seconds", type=float, default=60.0)
    parser.add_argument("--max-rss-mib", type=float, default=192)
    parser.add_argument("--max-idle-cpu-percent", type=float, default=3)
    parser.add_argument("--max-build-seconds", type=float, default=5)
    parser.add_argument("--max-state-mib", type=float, default=4)
    args = parser.parse_args()

    budgets = (args.idle_seconds, args.max_rss_mib, args.max_idle_cpu_percent,
               args.max_build_seconds, args.max_state_mib)
    if any(not math.isfinite(value) or value < 0 for value in budgets):
        parser.error("resource budgets must be finite and nonnegative")
    if not 0.5 <= args.idle_seconds <= 60:
        parser.error("--idle-seconds must be between 0.5 and 60")

    state = args.state_dir.absolute()
    report = args.json_path.absolute()
    if state.is_symlink() or report.is_symlink() or state.exists() or report.exists():
        parser.error("state/evidence output must be fresh non-symlink paths")
    state = state.resolve()
    report = report.resolve()
    state.mkdir(parents=True, mode=0o700)
    report.parent.mkdir(parents=True, exist_ok=True)

    flags = os.O_RDWR | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0)
    try:
        fd = os.open(report, flags, 0o600)
    except OSError:
        parser.error("cannot create a new private report")

    with os.fdopen(fd, "w+", encoding="utf-8") as stream:
        stream.write('{"verdict":"INCOMPLETE_LOCAL_PYTHON"}\n')
        stream.flush()
        os.fsync(stream.fileno())
        try:
            code, result = _run(args, state)
        except MeasurementUnavailable:
            code, result = 2, {
                "verdict": "INCOMPLETE_LOCAL_PYTHON",
                "failedChecks": ["measurement"],
                "reason": "required local-process measurement unavailable",
            }
        except Exception as error:
            code, result = 3, {
                "verdict": "FAIL_LOCAL_PYTHON",
                "failedChecks": ["execution"],
                "errorType": type(error).__name__,
                "reason": "local Python probe execution failed; no economic acceptance established",
            }
        encoded = json.dumps(result, indent=2, sort_keys=True, allow_nan=False) + "\n"
        stream.seek(0)
        stream.write(encoded)
        stream.truncate()
        stream.flush()
        os.fsync(stream.fileno())

    print(json.dumps(result, sort_keys=True, allow_nan=False))
    return code


if __name__ == "__main__":
    raise SystemExit(main())
