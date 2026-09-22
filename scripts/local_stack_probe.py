#!/usr/bin/env python3
"""Measure a native local A2A-MQ tier without Docker or external databases."""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import signal
import stat
import subprocess
import sys
import time
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


def _mib(value: int | float) -> float:
    return round(float(value) / (1024 * 1024), 3)


def _rss(pid: int) -> int:
    try:
        for line in Path(f"/proc/{pid}/status").read_text().splitlines():
            if line.startswith("VmRSS:"):
                return int(line.split()[1]) * 1024
    except (OSError, ValueError, IndexError):
        return 0
    return 0


def _ppid(pid: int) -> int | None:
    try:
        for line in Path(f"/proc/{pid}/status").read_text().splitlines():
            if line.startswith("PPid:"):
                return int(line.split()[1])
    except (OSError, ValueError, IndexError):
        return None
    return None


def _descendants(root: int) -> list[int]:
    found = {root}
    changed = True
    while changed:
        changed = False
        try:
            candidates = [int(path.name) for path in Path("/proc").iterdir() if path.name.isdigit()]
        except OSError:
            return sorted(found)
        for pid in candidates:
            if pid not in found and _ppid(pid) in found:
                found.add(pid)
                changed = True
    return sorted(found)


def _cmdline(pid: int) -> str:
    try:
        return Path(f"/proc/{pid}/cmdline").read_bytes().replace(b"\0", b" ").decode("utf-8", "replace").strip()
    except OSError:
        return ""


def _cpu_ticks(pid: int) -> int:
    try:
        raw = Path(f"/proc/{pid}/stat").read_text()
        tail = raw[raw.rfind(")") + 2:].split()
        return int(tail[11]) + int(tail[12])
    except (OSError, ValueError, IndexError):
        return 0


def _start_ticks(pid: int) -> int | None:
    """Linux process start time from /proc; stable across PID reuse."""
    try:
        raw = Path(f"/proc/{pid}/stat").read_text()
        tail = raw[raw.rfind(")") + 2:].split()
        return int(tail[19])
    except (OSError, ValueError, IndexError):
        return None


def _identities(pids: list[int]) -> dict[int, int]:
    return {pid: started for pid in pids if (started := _start_ticks(pid)) is not None}


def _tree_bytes(path: Path) -> int:
    total = 0
    if not path.exists():
        return 0
    for candidate in path.rglob("*"):
        try:
            if candidate.is_file() and not candidate.is_symlink():
                total += candidate.stat().st_size
        except OSError:
            pass
    return total


def _sample_total_rss(root_pid: int) -> tuple[int, int]:
    pids = _descendants(root_pid)
    return sum(_rss(pid) for pid in pids), len(pids)


def _ready(url: str, deadline: float, process: subprocess.Popen) -> tuple[int, int]:
    peak_rss = 0
    peak_process_count = 0
    while time.monotonic() < deadline:
        if process.poll() is not None:
            raise RuntimeError(f"local runtime exited during startup with {process.returncode}")
        rss, count = _sample_total_rss(process.pid)
        peak_rss = max(peak_rss, rss)
        peak_process_count = max(peak_process_count, count)
        try:
            request = Request(url, headers={"Cache-Control": "no-cache"})
            with urlopen(request, timeout=.5) as response:
                if response.status == 200:
                    payload = json.loads(response.read())
                    if payload.get("status") == "ready":
                        rss, count = _sample_total_rss(process.pid)
                        return max(peak_rss, rss), max(peak_process_count, count)
        except (URLError, HTTPError, TimeoutError, ValueError, OSError):
            pass
        time.sleep(.1)
    raise RuntimeError("local runtime readiness timeout")


def _same_process(pid: int, started: int) -> bool:
    return _start_ticks(pid) == started


def _all_alive(process: subprocess.Popen, children: dict[int, int]) -> bool:
    return process.poll() is None and all(_same_process(pid, started)
                                          for pid, started in children.items())


def _terminate(process: subprocess.Popen, children: dict[int, int]) -> None:
    if process.poll() is None:
        process.send_signal(signal.SIGTERM)
        try:
            process.wait(timeout=15)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=5)
    for pid, started in children.items():
        if pid == process.pid or not _same_process(pid, started):
            continue
        try:
            os.kill(pid, signal.SIGTERM)
        except ProcessLookupError:
            pass
    time.sleep(.2)
    for pid, started in children.items():
        if pid == process.pid or not _same_process(pid, started):
            continue
        try:
            os.kill(pid, signal.SIGKILL)
        except ProcessLookupError:
            pass


def _read_profile(root: Path) -> dict:
    """Read one bounded regular profile file without following symlinks."""
    if not root.is_dir() or root.is_symlink():
        raise ValueError("profile directory not found or unsafe")
    path = root / "profile.json"
    flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_NONBLOCK", 0)
    try:
        fd = os.open(path, flags)
        with os.fdopen(fd, "rb") as stream:
            info = os.fstat(stream.fileno())
            if not stat.S_ISREG(info.st_mode):
                raise ValueError("profile must be a regular file")
            raw = stream.read(16385)
    except OSError as exc:
        raise ValueError("profile file unavailable") from exc
    if not raw or len(raw) > 16384:
        raise ValueError("profile file exceeds size limit or is empty")
    try:
        value = json.loads(raw.decode("utf-8", errors="strict"))
    except (UnicodeError, ValueError) as exc:
        raise ValueError("invalid profile JSON") from exc
    if not isinstance(value, dict):
        raise ValueError("profile must be an object")
    return value


def _apply_post_shutdown_state(result: dict, state_bytes: int, max_state_mib: float, label: str) -> None:
    """Record flushed on-disk state and fail when clean shutdown exceeds the tier budget."""
    observed = result.get("observed")
    if not isinstance(observed, dict):
        return
    value = _mib(state_bytes)
    observed["postShutdownStateMiB"] = value
    if value <= max_state_mib:
        return
    failures = result.setdefault("failedChecks", [])
    if "postShutdownState" not in failures:
        failures.append("postShutdownState")
    result["verdict"] = f"FAIL_{label}"


def _profile_policy(profile: dict) -> dict:
    mode = profile.get("mode", "local-lite")
    if mode == "local-lite":
        return {
            "mode": mode,
            "module": "a2a_mq.local_runtime",
            "verdict": "LOCAL_LITE",
            "expectedRoles": ("app", "daprd", "nats"),
            "maxTotalRssMiB": 384.0,
            "maxIdleCpuPercent": 5.0,
            "maxStartupSeconds": 15.0,
            "maxRuntimeStateMiB": 32.0,
        }
    if mode == "local-workflow":
        return {
            "mode": mode,
            "module": "a2a_mq.local_workflow_runtime",
            "verdict": "LOCAL_WORKFLOW",
            "expectedRoles": ("app", "daprd", "nats", "placement", "scheduler"),
            "maxTotalRssMiB": 640.0,
            "maxIdleCpuPercent": 8.0,
            "maxStartupSeconds": 25.0,
            "maxRuntimeStateMiB": 64.0,
        }
    raise ValueError("unsupported local profile mode")


def _roles(root_pid: int, commands: dict[int, str]) -> dict[str, list[int]]:
    return {
        "app": [pid for pid in commands if pid == root_pid],
        "daprd": [pid for pid, cmd in commands.items() if "daprd" in cmd],
        "nats": [pid for pid, cmd in commands.items() if "nats-server" in cmd],
        "placement": [pid for pid, cmd in commands.items()
                      if "placement" in cmd and "placement-host-address" not in cmd],
        "scheduler": [pid for pid, cmd in commands.items()
                      if "scheduler" in cmd and "scheduler-host-address" not in cmd],
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--profile", required=True, type=Path)
    parser.add_argument("--json", dest="json_path", required=True, type=Path)
    parser.add_argument("--log", required=True, type=Path)
    parser.add_argument("--startup-timeout", type=float)
    parser.add_argument("--idle-seconds", type=float, default=3)
    parser.add_argument("--max-total-rss-mib", type=float)
    parser.add_argument("--max-idle-cpu-percent", type=float)
    parser.add_argument("--max-startup-seconds", type=float)
    parser.add_argument("--max-runtime-state-mib", type=float)
    args = parser.parse_args()

    if not Path("/proc/self/status").exists():
        raise SystemExit("local stack probe currently requires Linux /proc")
    try:
        profile = _read_profile(args.profile)
    except ValueError as exc:
        raise SystemExit(str(exc)) from exc
    policy = _profile_policy(profile)
    expected_roles = policy["expectedRoles"]
    expected_count = len(expected_roles)
    max_rss = args.max_total_rss_mib or policy["maxTotalRssMiB"]
    max_cpu = args.max_idle_cpu_percent or policy["maxIdleCpuPercent"]
    max_startup = args.max_startup_seconds or policy["maxStartupSeconds"]
    max_state = args.max_runtime_state_mib or policy["maxRuntimeStateMiB"]
    startup_timeout = args.startup_timeout or max_startup + 10

    args.json_path.parent.mkdir(parents=True, exist_ok=True)
    args.log.parent.mkdir(parents=True, exist_ok=True)
    if args.json_path.exists() or args.log.exists():
        raise SystemExit("evidence output already exists; refusing to overwrite")

    started = time.monotonic()
    children: dict[int, int] = {}
    with args.log.open("xb") as log:
        try:
            process = subprocess.Popen(
                [sys.executable, "-m", policy["module"], "run", str(args.profile.resolve())],
                stdout=log, stderr=subprocess.STDOUT, cwd=Path.cwd(),
            )
        except OSError as exc:
            label = policy["verdict"]
            result = {
                "verdict": f"FAIL_{label}",
                "scope": profile.get("mode", "local-lite"),
                "error": type(exc).__name__,
                "message": str(exc)[:512],
                "failedChecks": ["processStart"],
            }
        else:
            try:
                try:
                    startup_peak_rss, startup_peak_process_count = _ready(
                        "http://127.0.0.1:18000/health/ready", started + startup_timeout, process
                    )
                    startup = time.monotonic() - started
                    pids = _descendants(process.pid)
                    children = _identities(pids)
                    commands = {pid: _cmdline(pid) for pid in children}
                    roles = _roles(process.pid, commands)

                    before = {pid: _cpu_ticks(pid) for pid in children}
                    wall = time.monotonic()
                    idle_peak_rss = 0
                    idle_peak_process_count = 0
                    idle_deadline = wall + args.idle_seconds
                    while time.monotonic() < idle_deadline:
                        rss_sample, count_sample = _sample_total_rss(process.pid)
                        idle_peak_rss = max(idle_peak_rss, rss_sample)
                        idle_peak_process_count = max(idle_peak_process_count, count_sample)
                        time.sleep(min(.1, max(0.0, idle_deadline - time.monotonic())))
                    elapsed = max(time.monotonic() - wall, .001)
                    after = {pid: _cpu_ticks(pid) for pid in children}
                    all_alive = _all_alive(process, children)
                    hz = os.sysconf(os.sysconf_names["SC_CLK_TCK"])
                    cpu_percent = sum(max(0, after.get(pid, 0) - before.get(pid, 0))
                                      for pid in children) / hz / elapsed * 100
                    rss = {pid: _rss(pid) for pid in children}
                    steady_total_rss = sum(rss.values())
                    peak_total_rss = max(startup_peak_rss, idle_peak_rss, steady_total_rss)
                    state_bytes = _tree_bytes(args.profile / "state") + _tree_bytes(args.profile / "nats")
                    role_rss = {role: _mib(sum(rss.get(pid, 0) for pid in pids))
                                for role, pids in roles.items()}

                    checks = {
                        "allAlive": all_alive,
                        "exactProcessCount": len(children) == expected_count,
                        "stableIdleProcessCount": idle_peak_process_count == expected_count,
                        **{f"one{role.title()}": len(roles[role]) == 1 for role in expected_roles},
                        "rss": _mib(peak_total_rss) <= max_rss,
                        "idleCpu": cpu_percent <= max_cpu,
                        "startup": startup <= max_startup,
                        "state": _mib(state_bytes) <= max_state,
                    }
                    failures = [name for name, ok in checks.items() if not ok]
                    label = policy["verdict"]
                    result = {
                        "verdict": f"PASS_{label}" if not failures else f"FAIL_{label}",
                        "scope": profile.get("mode", "local-lite"),
                        "thresholds": {
                            "maxTotalRssMiB": max_rss,
                            "maxIdleCpuPercentOneCore": max_cpu,
                            "maxStartupSeconds": max_startup,
                            "maxRuntimeStateMiB": max_state,
                            "expectedProcesses": expected_count,
                            "expectedRoles": list(expected_roles),
                        },
                        "observed": {
                            "processCount": len(children),
                            "allAliveAfterIdle": all_alive,
                            "processes": {str(pid): commands[pid] for pid in children},
                            "rssByRoleMiB": role_rss,
                            "steadyTotalRssMiB": _mib(steady_total_rss),
                            "peakTotalRssMiB": _mib(peak_total_rss),
                            "startupPeakRssMiB": _mib(startup_peak_rss),
                            "idlePeakRssMiB": _mib(idle_peak_rss),
                            "startupPeakProcessCount": startup_peak_process_count,
                            "idlePeakProcessCount": idle_peak_process_count,
                            "idleCpuPercentOneCore": round(cpu_percent, 3),
                            "startupSeconds": round(startup, 3),
                            "runtimeStateMiB": _mib(state_bytes),
                        },
                        "failedChecks": failures,
                    }
                except Exception as exc:
                    label = policy["verdict"]
                    result = {
                        "verdict": f"FAIL_{label}",
                        "scope": profile.get("mode", "local-lite"),
                        "error": type(exc).__name__,
                        "message": str(exc)[:512],
                        "failedChecks": ["startup"],
                    }
                    if not children:
                        children = _identities(_descendants(process.pid))
            finally:
                _terminate(process, children)

    post_shutdown_state = _tree_bytes(args.profile / "state") + _tree_bytes(args.profile / "nats")
    _apply_post_shutdown_state(result, post_shutdown_state, max_state, policy["verdict"])

    with args.json_path.open("x", encoding="utf-8") as output:
        output.write(json.dumps(result, indent=2, sort_keys=True) + "\n")
    print(json.dumps(result, sort_keys=True))
    return 0 if result["verdict"].startswith("PASS_") else 3


if __name__ == "__main__":
    raise SystemExit(main())
