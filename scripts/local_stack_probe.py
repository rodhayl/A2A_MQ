#!/usr/bin/env python3
"""Measure the real three-process local-lite runtime without Docker or extra services."""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import time
from urllib.request import Request, urlopen
from urllib.error import URLError, HTTPError


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


def _ready(url: str, deadline: float, process: subprocess.Popen) -> None:
    while time.monotonic() < deadline:
        if process.poll() is not None:
            raise RuntimeError(f"local runtime exited during startup with {process.returncode}")
        try:
            request = Request(url, headers={"Cache-Control": "no-cache"})
            with urlopen(request, timeout=.5) as response:
                if response.status == 200:
                    payload = json.loads(response.read())
                    if payload.get("status") == "ready":
                        return
        except (URLError, HTTPError, TimeoutError, ValueError, OSError):
            pass
        time.sleep(.1)
    raise RuntimeError("local-lite readiness timeout")


def _terminate(process: subprocess.Popen, children: list[int]) -> None:
    if process.poll() is None:
        process.send_signal(signal.SIGTERM)
        try:
            process.wait(timeout=15)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=5)
    # Fallback only for PIDs captured as descendants of this run.
    for pid in children:
        if pid == process.pid or not Path(f"/proc/{pid}").exists():
            continue
        try:
            os.kill(pid, signal.SIGTERM)
        except ProcessLookupError:
            pass
    time.sleep(.2)
    for pid in children:
        if pid == process.pid or not Path(f"/proc/{pid}").exists():
            continue
        try:
            os.kill(pid, signal.SIGKILL)
        except ProcessLookupError:
            pass


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--profile", required=True, type=Path)
    parser.add_argument("--json", dest="json_path", required=True, type=Path)
    parser.add_argument("--log", required=True, type=Path)
    parser.add_argument("--startup-timeout", type=float, default=20)
    parser.add_argument("--idle-seconds", type=float, default=3)
    parser.add_argument("--max-total-rss-mib", type=float, default=384)
    parser.add_argument("--max-idle-cpu-percent", type=float, default=5)
    parser.add_argument("--max-startup-seconds", type=float, default=15)
    parser.add_argument("--max-runtime-state-mib", type=float, default=32)
    args = parser.parse_args()

    if not Path("/proc/self/status").exists():
        raise SystemExit("local stack probe currently requires Linux /proc")
    if not args.profile.is_dir():
        raise SystemExit("profile directory not found")
    args.json_path.parent.mkdir(parents=True, exist_ok=True)
    args.log.parent.mkdir(parents=True, exist_ok=True)

    started = time.monotonic()
    children: list[int] = []
    with args.log.open("wb") as log:
        process = subprocess.Popen(
            [sys.executable, "-m", "a2a_mq.local_runtime", "run", str(args.profile.resolve())],
            stdout=log, stderr=subprocess.STDOUT, cwd=Path.cwd(),
        )
        try:
            try:
                _ready("http://127.0.0.1:18000/health/ready",
                       started + args.startup_timeout, process)
                startup = time.monotonic() - started
                children = _descendants(process.pid)
                commands = {pid: _cmdline(pid) for pid in children}
                # Exactly one Python app (this child), one daprd and one nats-server.
                roles = {
                    "app": [pid for pid, cmd in commands.items() if pid == process.pid],
                    "daprd": [pid for pid, cmd in commands.items() if "daprd" in cmd],
                    "nats": [pid for pid, cmd in commands.items() if "nats-server" in cmd],
                }
                before = {pid: _cpu_ticks(pid) for pid in children}
                wall = time.monotonic()
                time.sleep(args.idle_seconds)
                elapsed = max(time.monotonic() - wall, .001)
                after = {pid: _cpu_ticks(pid) for pid in children}
                hz = os.sysconf(os.sysconf_names["SC_CLK_TCK"])
                cpu_percent = sum(max(0, after.get(pid, 0) - before.get(pid, 0))
                                  for pid in children) / hz / elapsed * 100
                rss = {pid: _rss(pid) for pid in children}
                total_rss = sum(rss.values())
                state_bytes = _tree_bytes(args.profile / "state") + _tree_bytes(args.profile / "nats")

                role_rss = {}
                for role, pids in roles.items():
                    role_rss[role] = _mib(sum(rss.get(pid, 0) for pid in pids))

                checks = {
                    "exactProcessCount": len(children) == 3,
                    "oneApp": len(roles["app"]) == 1,
                    "oneDaprd": len(roles["daprd"]) == 1,
                    "oneNats": len(roles["nats"]) == 1,
                    "rss": _mib(total_rss) <= args.max_total_rss_mib,
                    "idleCpu": cpu_percent <= args.max_idle_cpu_percent,
                    "startup": startup <= args.max_startup_seconds,
                    "state": _mib(state_bytes) <= args.max_runtime_state_mib,
                }
                failures = [name for name, ok in checks.items() if not ok]
                result = {
                    "verdict": "PASS_LOCAL_LITE" if not failures else "FAIL_LOCAL_LITE",
                    "scope": "native local-lite: one Python A2A-MQ app + one daprd + one nats-server",
                    "thresholds": {
                        "maxTotalRssMiB": args.max_total_rss_mib,
                        "maxIdleCpuPercentOneCore": args.max_idle_cpu_percent,
                        "maxStartupSeconds": args.max_startup_seconds,
                        "maxRuntimeStateMiB": args.max_runtime_state_mib,
                        "expectedProcesses": 3,
                    },
                    "observed": {
                        "processCount": len(children),
                        "processes": {str(pid): commands[pid] for pid in children},
                        "rssByRoleMiB": role_rss,
                        "totalRssMiB": _mib(total_rss),
                        "idleCpuPercentOneCore": round(cpu_percent, 3),
                        "startupSeconds": round(startup, 3),
                        "runtimeStateMiB": _mib(state_bytes),
                    },
                    "failedChecks": failures,
                }
            except Exception as exc:
                result = {
                    "verdict": "FAIL_LOCAL_LITE",
                    "scope": "native local-lite startup/readiness",
                    "error": type(exc).__name__,
                    "message": str(exc)[:512],
                    "failedChecks": ["startup"],
                }
                if not children:
                    children = _descendants(process.pid)
        finally:
            _terminate(process, children)

    args.json_path.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(result, sort_keys=True))
    return 0 if result["verdict"] == "PASS_LOCAL_LITE" else 3


if __name__ == "__main__":
    raise SystemExit(main())
