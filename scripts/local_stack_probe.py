#!/usr/bin/env python3
"""Measure a native local A2A-MQ tier without Docker or external databases."""
from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
from pathlib import Path
import signal
import shlex
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
        argv = Path(f"/proc/{pid}/cmdline").read_bytes().split(b"\0")
        return shlex.join(arg.decode("utf-8", "replace") for arg in argv if arg)
    except OSError:
        return ""


def _cpu_ticks(pid: int) -> int | None:
    try:
        raw = Path(f"/proc/{pid}/stat").read_text()
        tail = raw[raw.rfind(")") + 2:].split()
        return int(tail[11]) + int(tail[12])
    except (OSError, ValueError, IndexError):
        return None


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


def _is_unwritten_wal_scratch(candidate: Path, info: os.stat_result) -> bool:
    """Recognize uncommitted Dapr Scheduler etcd WAL preallocation scratch files.

    Etcd preallocates the next WAL segment as <seq>.tmp with SegmentSizeBytes (64MB)
    of zero bytes via fallocate. These unwritten zero buffers do not contain durable state,
    are deleted on clean shutdown, and loss does not affect recovery.
    If a file contains actual non-zero data or is outside a wal/ directory, it is durable
    state and must be counted.
    """
    if candidate.suffix != ".tmp" or candidate.parent.name != "wal":
        return False
    if info.st_size == 0:
        return True
    try:
        with candidate.open("rb") as stream:
            while chunk := stream.read(1024 * 1024):
                if any(chunk):
                    return False
        return True
    except OSError:
        return False


def _tree_bytes(path: Path) -> int:
    total = 0
    if not path.is_dir() or path.is_symlink():
        raise MeasurementUnavailable("runtime state directory unavailable or unsafe")
    try:
        for candidate in path.rglob("*"):
            if candidate.is_symlink():
                raise MeasurementUnavailable("symlinked runtime state cannot be measured")
            info = candidate.stat()
            if stat.S_ISREG(info.st_mode):
                if _is_unwritten_wal_scratch(candidate, info):
                    continue
                total += info.st_size
    except OSError:
        raise MeasurementUnavailable("runtime state measurement unavailable") from None
    return total


def _sample_total_rss(root_pid: int) -> tuple[int, int]:
    pids = _descendants(root_pid)
    readings = [_rss(pid) for pid in pids]
    if not readings or any(value <= 0 for value in readings):
        raise MeasurementUnavailable("RSS measurement unavailable")
    return sum(readings), len(pids)


def _ready(url: str, deadline: float, process: subprocess.Popen) -> tuple[int, int]:
    peak_rss = 0
    peak_process_count = 0
    while time.monotonic() < deadline:
        if process.poll() is not None:
            raise RuntimeError(f"local runtime exited during startup with {process.returncode}")
        try:
            rss, count = _sample_total_rss(process.pid)
            peak_rss = max(peak_rss, rss)
            peak_process_count = max(peak_process_count, count)
        except MeasurementUnavailable:
            pass
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
    """Record flushed state without rounding away an over-budget result."""
    observed = result.get("observed")
    if not isinstance(observed, dict):
        return
    observed["postShutdownStateMiB"] = _mib(state_bytes)
    if state_bytes <= max_state_mib * 1024**2:
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


class MeasurementUnavailable(RuntimeError):
    """A required reading is absent; it cannot be interpreted as zero usage."""


def _roles(root_pid: int, commands: dict[int, str]) -> dict[str, list[int]]:
    roles = {"app": [], "daprd": [], "nats": [], "placement": [], "scheduler": []}
    names = {"daprd": "daprd", "nats-server": "nats", "placement": "placement", "scheduler": "scheduler"}
    for pid, command in commands.items():
        if pid == root_pid:
            roles["app"].append(pid)
            continue
        try:
            argv = shlex.split(command)
        except ValueError:
            argv = []
        role = names.get(Path(argv[0]).name) if argv else None
        if role:
            roles[role].append(pid)
    return roles


def _validated_tools_bin(root: Path, expected_roles: tuple[str, ...]) -> Path:
    """Validate a private bootstrap directory before placing its bin first on PATH."""
    root = root.expanduser().absolute()
    bin_dir = root / "bin"
    manifest = root / "manifest.json"
    try:
        if (root.is_symlink() or bin_dir.is_symlink() or manifest.is_symlink()
                or not root.is_dir() or not bin_dir.is_dir()):
            raise MeasurementUnavailable("native tools directory unavailable or unsafe")
        manifest_info = manifest.stat()
        if (root.stat().st_mode & 0o077 or bin_dir.stat().st_mode & 0o077
                or manifest_info.st_mode & 0o077 or not stat.S_ISREG(manifest_info.st_mode)):
            raise MeasurementUnavailable("native tools directory is not private")
        names = {"daprd": "daprd", "nats": "nats-server",
                 "placement": "placement", "scheduler": "scheduler"}
        for role in expected_roles:
            if role == "app":
                continue
            path = bin_dir / names[role]
            info = path.stat()
            if path.is_symlink() or not stat.S_ISREG(info.st_mode) or not os.access(path, os.X_OK):
                raise MeasurementUnavailable(f"native tool unavailable or unsafe: {names[role]}")
    except (OSError, KeyError):
        raise MeasurementUnavailable("native tools directory unavailable or unsafe") from None
    return bin_dir


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    try:
        with path.open("rb") as stream:
            for chunk in iter(lambda: stream.read(1024 * 1024), b""):
                digest.update(chunk)
    except OSError:
        raise MeasurementUnavailable("native tool fingerprint unavailable") from None
    return digest.hexdigest()


def _native_fingerprints(root: Path, expected_roles: tuple[str, ...]) -> tuple[str, dict[str, str]]:
    names = {"daprd": "daprd", "nats": "nats-server",
             "placement": "placement", "scheduler": "scheduler"}
    binaries = {
        names[role]: _sha256_file(root / "bin" / names[role])
        for role in expected_roles if role != "app"
    }
    return _sha256_file(root / "manifest.json"), binaries


def _verified_native_report(path: Path, manifest_sha: str, binary_sha: dict[str, str],
                            expected_roles: tuple[str, ...]) -> dict:
    """Require a prior PASS_NATIVE_VERSION for these exact bootstrap bytes."""
    names = {"daprd": "daprd", "nats": "nats-server",
             "placement": "placement", "scheduler": "scheduler"}
    expected = {names[role] for role in expected_roles if role != "app"}
    try:
        if path.is_symlink():
            raise MeasurementUnavailable("native verification unavailable or unsafe")
        flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_NONBLOCK", 0)
        fd = os.open(path, flags)
        with os.fdopen(fd, "rb") as stream:
            info = os.fstat(stream.fileno())
            if not stat.S_ISREG(info.st_mode) or info.st_size <= 0 or info.st_size > 65536:
                raise MeasurementUnavailable("native verification unavailable or unsafe")
            raw = stream.read(65537)
        if len(raw) > 65536:
            raise MeasurementUnavailable("native verification unavailable or unsafe")
        report = json.loads(raw.decode("utf-8", errors="strict"))
    except MeasurementUnavailable:
        raise
    except (OSError, UnicodeError, ValueError, KeyError):
        raise MeasurementUnavailable("native verification unavailable or invalid") from None
    actual = report.get("binarySha256") if isinstance(report, dict) else None
    if (not isinstance(report, dict) or report.get("verdict") != "PASS_NATIVE_VERSION"
            or report.get("bootstrapManifestSha256") != manifest_sha
            or not isinstance(actual, dict) or set(actual) != expected
            or actual != binary_sha):
        raise MeasurementUnavailable("native verification does not match bootstrap tools")
    return report


def _native_tools_match(roles: dict[str, list[int]], commands: dict[int, str],
                        bin_dir: Path, expected_roles: tuple[str, ...]) -> bool:
    names = {"daprd": "daprd", "nats": "nats-server",
             "placement": "placement", "scheduler": "scheduler"}
    for role in expected_roles:
        if role == "app":
            continue
        pids = roles.get(role, [])
        if len(pids) != 1:
            return False
        try:
            argv = shlex.split(commands[pids[0]])
        except ValueError:
            return False
        if not argv or Path(argv[0]).resolve() != (bin_dir / names[role]).resolve():
            return False
    return True


def _by_role(roles: dict[str, list[int]], values: dict[int, int | float]) -> dict[str, float]:
    """Aggregate one per-PID measurement without hiding unclassified descendants."""
    return {role: sum(values.get(pid, 0) for pid in pids) for role, pids in roles.items()}


def _require_stable_tree(process, children: dict[int, int]) -> None:
    if process.poll() is not None:
        raise RuntimeError("local runtime exited during measurement")
    pids = _descendants(process.pid)
    current = _identities(pids)
    if set(pids) != set(current):
        raise MeasurementUnavailable("process identity unavailable")
    if current != children:
        raise RuntimeError("local process topology changed during measurement")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--profile", required=True, type=Path)
    parser.add_argument("--json", dest="json_path", required=True, type=Path)
    parser.add_argument("--log", required=True, type=Path)
    parser.add_argument("--tools-dir", required=True, type=Path,
                        help="private bootstrap root whose bin directory must supply native children")
    parser.add_argument("--native-verification", required=True, type=Path,
                        help="PASS_NATIVE_VERSION JSON for the exact bootstrap bytes")
    parser.add_argument("--startup-timeout", type=float)
    parser.add_argument("--idle-seconds", type=float, default=60)
    parser.add_argument("--max-total-rss-mib", type=float)
    parser.add_argument("--max-idle-cpu-percent", type=float)
    parser.add_argument("--max-startup-seconds", type=float)
    parser.add_argument("--max-runtime-state-mib", type=float)
    args = parser.parse_args()
    limits = (args.max_total_rss_mib, args.max_idle_cpu_percent,
              args.max_startup_seconds, args.max_runtime_state_mib)
    if any(value is not None and (not math.isfinite(value) or value < 0) for value in limits):
        parser.error("resource budgets must be finite and nonnegative")
    if not math.isfinite(args.idle_seconds) or not 0 < args.idle_seconds <= 60:
        parser.error("idle measurement must be greater than zero and at most 60 seconds")
    if args.startup_timeout is not None and (not math.isfinite(args.startup_timeout)
                                             or not 0 < args.startup_timeout <= 300):
        parser.error("startup timeout must be greater than zero and at most 300 seconds")
    if args.json_path.resolve() == args.log.resolve():
        parser.error("JSON evidence and log must use distinct paths")

    if not Path("/proc/self/status").exists():
        raise SystemExit("local stack probe currently requires Linux /proc")
    try:
        profile = _read_profile(args.profile)
    except ValueError as exc:
        raise SystemExit(str(exc)) from exc
    policy = _profile_policy(profile)
    expected_roles = policy["expectedRoles"]
    expected_count = len(expected_roles)

    args.json_path.parent.mkdir(parents=True, exist_ok=True)
    args.log.parent.mkdir(parents=True, exist_ok=True)
    if args.json_path.exists() or args.log.exists():
        raise SystemExit("evidence output already exists; refusing to overwrite")

    try:
        tools_bin = _validated_tools_bin(args.tools_dir, expected_roles)
        tools_manifest_sha, tools_binary_sha = _native_fingerprints(
            args.tools_dir.expanduser().absolute(), expected_roles
        )
        _verified_native_report(args.native_verification, tools_manifest_sha,
                                tools_binary_sha, expected_roles)
    except MeasurementUnavailable as exc:
        result = {"verdict": "INCOMPLETE_MEASUREMENT", "scope": policy["mode"],
                  "message": str(exc), "failedChecks": ["nativeProvenance"]}
        with args.log.open("xb") as log:
            log.write((str(exc) + "\n").encode("utf-8"))
        with args.json_path.open("x", encoding="utf-8") as output:
            output.write(json.dumps(result, indent=2, sort_keys=True) + "\n")
        print(json.dumps(result, sort_keys=True))
        return 2

    max_rss = policy["maxTotalRssMiB"] if args.max_total_rss_mib is None else args.max_total_rss_mib
    max_cpu = policy["maxIdleCpuPercent"] if args.max_idle_cpu_percent is None else args.max_idle_cpu_percent
    max_startup = policy["maxStartupSeconds"] if args.max_startup_seconds is None else args.max_startup_seconds
    max_state = policy["maxRuntimeStateMiB"] if args.max_runtime_state_mib is None else args.max_runtime_state_mib
    startup_timeout = args.startup_timeout or max_startup + 10

    started = time.monotonic()
    children: dict[int, int] = {}
    stage = "processStart"
    with args.log.open("xb") as log:
        process = None
        try:
            try:
                child_env = os.environ.copy()
                child_env["PATH"] = str(tools_bin) + os.pathsep + child_env.get("PATH", "")
                process = subprocess.Popen(
                    [sys.executable, "-m", policy["module"], "run", str(args.profile.resolve())],
                    stdout=log, stderr=subprocess.STDOUT, cwd=Path.cwd(), env=child_env,
                )
                stage = "startup"
                startup_peak_rss, startup_peak_process_count = _ready(
                    "http://127.0.0.1:18000/health/ready", started + startup_timeout, process
                )
                startup = time.monotonic() - started
                pids = _descendants(process.pid)
                children = _identities(pids)
                stage = "measurement"
                if set(pids) != set(children):
                    raise MeasurementUnavailable("process identity unavailable")
                _require_stable_tree(process, children)
                commands = {pid: _cmdline(pid) for pid in children}
                roles = _roles(process.pid, commands)

                before = {pid: _cpu_ticks(pid) for pid in children}
                if any(type(value) is not int or value < 0 for value in before.values()):
                    raise MeasurementUnavailable("CPU measurement unavailable")
                wall = time.monotonic()
                idle_peak_rss = 0
                idle_peak_process_count = 0
                idle_peak_rss_by_pid = {pid: 0 for pid in children}
                idle_deadline = wall + args.idle_seconds
                while time.monotonic() < idle_deadline:
                    _require_stable_tree(process, children)
                    rss_by_pid_sample = {pid: _rss(pid) for pid in children}
                    if any(value <= 0 for value in rss_by_pid_sample.values()):
                        raise MeasurementUnavailable("RSS measurement unavailable")
                    rss_sample = sum(rss_by_pid_sample.values())
                    idle_peak_rss = max(idle_peak_rss, rss_sample)
                    idle_peak_process_count = max(idle_peak_process_count, len(rss_by_pid_sample))
                    for pid, value in rss_by_pid_sample.items():
                        idle_peak_rss_by_pid[pid] = max(idle_peak_rss_by_pid[pid], value)
                    time.sleep(min(.1, max(0.0, idle_deadline - time.monotonic())))
                elapsed = max(time.monotonic() - wall, .001)
                _require_stable_tree(process, children)
                after = {pid: _cpu_ticks(pid) for pid in children}
                if any(type(value) is not int or value < before[pid] for pid, value in after.items()):
                    raise MeasurementUnavailable("CPU measurement unavailable or inconsistent")
                hz = os.sysconf(os.sysconf_names["SC_CLK_TCK"])
                cpu_by_pid = {
                    pid: max(0, after.get(pid, 0) - before.get(pid, 0)) / hz / elapsed * 100
                    for pid in children
                }
                cpu_percent = sum(cpu_by_pid.values())
                rss = {pid: _rss(pid) for pid in children}
                if any(value <= 0 for value in rss.values()):
                    raise MeasurementUnavailable("RSS measurement unavailable")
                _require_stable_tree(process, children)
                steady_total_rss = sum(rss.values())
                peak_total_rss = max(startup_peak_rss, idle_peak_rss, steady_total_rss)
                all_alive = _all_alive(process, children)
                state_bytes = _tree_bytes(args.profile / "state") + _tree_bytes(args.profile / "nats")
                role_rss = {role: _mib(value) for role, value in _by_role(roles, rss).items()}
                idle_peak_role_rss = {
                    role: _mib(value) for role, value in _by_role(roles, idle_peak_rss_by_pid).items()
                }
                role_cpu = {
                    role: round(value, 3) for role, value in _by_role(roles, cpu_by_pid).items()
                }

                checks = {
                    "allAlive": all_alive,
                    "exactProcessCount": len(children) == expected_count,
                    "stableIdleProcessCount": idle_peak_process_count == expected_count,
                    **{f"one{role.title()}": len(roles[role]) == 1 for role in expected_roles},
                    "nativeToolsBound": _native_tools_match(
                        roles, commands, tools_bin, expected_roles),
                    "rss": peak_total_rss <= max_rss * 1024**2,
                    "idleCpu": cpu_percent <= max_cpu,
                    "startup": startup <= max_startup,
                    "state": state_bytes <= max_state * 1024**2,
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
                        "processes": {str(pid): commands[pid] for pid in children},
                        "nativeToolsBin": str(tools_bin),
                        "bootstrapManifestSha256": tools_manifest_sha,
                        "binarySha256": tools_binary_sha,
                        "rssByRoleMiB": role_rss,
                        "idlePeakRssByRoleMiB": idle_peak_role_rss,
                        "idleCpuByRolePercentOneCore": role_cpu,
                        "allAliveAfterIdle": all_alive,
                        "totalRssMiB": _mib(steady_total_rss),
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
            except MeasurementUnavailable as exc:
                if process is not None and not children:
                    children = _identities(_descendants(process.pid))
                result = {"verdict": "INCOMPLETE_MEASUREMENT", "scope": policy["mode"],
                          "message": str(exc), "failedChecks": ["measurement"]}
            except Exception as exc:
                label = policy["verdict"]
                result = {
                    "verdict": f"FAIL_{label}",
                    "scope": profile.get("mode", "local-lite"),
                    "error": type(exc).__name__,
                    "message": str(exc)[:512],
                    "failedChecks": [stage],
                }
                if process is not None and not children:
                    children = _identities(_descendants(process.pid))
        finally:
            if process is not None:
                _terminate(process, children)

    if isinstance(result.get("observed"), dict):
        try:
            post_shutdown_state = _tree_bytes(args.profile / "state") + _tree_bytes(args.profile / "nats")
            _apply_post_shutdown_state(result, post_shutdown_state, max_state, policy["verdict"])
        except MeasurementUnavailable:
            result.setdefault("failedChecks", []).append("postShutdownMeasurement")
            if result["verdict"].startswith("PASS_"):
                result["verdict"] = "INCOMPLETE_MEASUREMENT"

    with args.json_path.open("x", encoding="utf-8") as output:
        output.write(json.dumps(result, indent=2, sort_keys=True) + "\n")
    print(json.dumps(result, sort_keys=True))
    return (0 if result["verdict"].startswith("PASS_")
            else 2 if result["verdict"] == "INCOMPLETE_MEASUREMENT" else 3)


if __name__ == "__main__":
    raise SystemExit(main())
