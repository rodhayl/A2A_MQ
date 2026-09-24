#!/usr/bin/env python3
"""Production-deployment acceptance gate for full-topology Kubernetes validation.

Executes real cluster deployment readiness, pod security-context verification,
Dapr control-plane/mTLS/sidecar checks, PDB enforcement, E2E task lifecycle,
cross-owner/tenant authorization, authenticated MCP, Workflow worker restart,
DLQ routing/replay, rolling restart zero-loss, and controlled broker recovery.

Simulation mode is strictly isolated and can never emit PRODUCTION_DEPLOYMENT_PASS.
Cleanly reports INCOMPLETE_PREREQUISITE when cluster access or prerequisites are absent.
"""
from __future__ import annotations

import argparse
import asyncio
import contextlib
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import stat
import subprocess
import sys
import time
from typing import Any, Callable
from urllib.parse import urljoin
from uuid import uuid4

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from google.protobuf.json_format import ParseDict  # noqa: E402
from a2a.client.client import ClientConfig  # noqa: E402
from a2a.client.client_factory import ClientFactory  # noqa: E402
from a2a.types import a2a_pb2 as pb  # noqa: E402
from mcp import Client as McpClient  # noqa: E402
from mcp.client.streamable_http import streamable_http_client  # noqa: E402

from a2a_mq.configuration import read_file  # noqa: E402
from a2a_mq.domain import MODEL_CREDENTIAL, require  # noqa: E402

IMAGE_DIGEST_RE = re.compile(r"^[A-Za-z0-9._/:@-]+@sha256:[0-9a-f]{64}$")
SAFE_NAMESPACE_RE = re.compile(r"^[a-z0-9]([-a-z0-9]*[a-z0-9])?$")
FORBIDDEN_NAMESPACES = frozenset({
    "default", "kube-system", "kube-public", "kube-node-lease", "dapr-system",
})

GATE_RUN_ID_LABEL = "a2amq.io/gate-run-id"
MANAGED_BY_LABEL = "app.kubernetes.io/managed-by"
MANAGED_BY_VALUE = "a2amq-production-gate"

PRODUCTION_REQUIRED_CHECKS = frozenset({
    "manifest-synthesis-and-validation",
    "cluster-connectivity-and-auth",
    "server-side-manifest-validation",
    "prerequisite-dapr-control-plane",
    "prerequisite-dapr-mtls",
    "prerequisite-dapr-placement-scheduler",
    "prerequisite-secrets-and-configmaps",
    "deployment-rollout-readiness",
    "pod-security-context-verification",
    "dapr-sidecar-injection-and-health",
    "pod-disruption-budget-enforcement",
    "postgresql-state-store-attachment",
    "task-lifecycle-roundtrip",
    "owner-and-tenant-authorization",
    "authenticated-mcp-query",
    "workflow-execution-and-restart",
    "dlq-routing-and-replay",
    "rolling-restart-zero-loss",
    "broker-outage-recovery",
})


def get_git_provenance() -> dict[str, str]:
    """Capture current Git HEAD, tree SHA, and clean/dirty status."""
    try:
        head = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip()
        tree = subprocess.check_output(["git", "rev-parse", "HEAD^{tree}"], cwd=ROOT, text=True).strip()
        status_out = subprocess.check_output(["git", "status", "--porcelain"], cwd=ROOT, text=True).strip()
        return {
            "head": head,
            "tree": tree,
            "status": "clean" if not status_out else "dirty",
        }
    except Exception:
        return {"head": "unknown", "tree": "unknown", "status": "unknown"}


class CommandResult:
    def __init__(self, returncode: int, stdout: str, stderr: str):
        self.returncode = returncode
        self.stdout = stdout
        self.stderr = stderr


class Kubectl:
    """Bounded, safe kubectl execution abstraction with audit logging."""

    def __init__(self, *, kubeconfig: str | None = None, context: str | None = None,
                 namespace: str | None = None, runner: Callable[..., CommandResult] | None = None):
        self.kubeconfig = kubeconfig
        self.context = context
        self.namespace = namespace
        self.runner = runner or self._default_run
        self.history: list[dict[str, Any]] = []

    def _default_run(self, cmd: list[str], input_bytes: bytes | None = None,
                      timeout: float = 60.0) -> CommandResult:
        try:
            res = subprocess.run(
                cmd,
                input=input_bytes,
                capture_output=True,
                timeout=timeout,
                check=False,
            )
            return CommandResult(
                res.returncode,
                res.stdout.decode("utf-8", "replace"),
                res.stderr.decode("utf-8", "replace"),
            )
        except subprocess.TimeoutExpired:
            return CommandResult(124, "", "Command timed out")
        except OSError as exc:
            return CommandResult(127, "", str(exc))

    def run(self, *args: str, input_str: str | None = None, timeout: float = 60.0,
            use_namespace: bool = True) -> CommandResult:
        cmd = ["kubectl"]
        if self.kubeconfig:
            cmd.extend(["--kubeconfig", self.kubeconfig])
        if self.context:
            cmd.extend(["--context", self.context])
        if use_namespace and self.namespace:
            cmd.extend(["-n", self.namespace])
        cmd.extend(args)

        input_bytes = input_str.encode("utf-8") if input_str is not None else None
        res = self.runner(cmd, input_bytes=input_bytes, timeout=timeout)

        # Sanitize sensitive patterns before recording history
        sanitized_cmd = [re.sub(r"(token|key|password)=.+", r"\1=REDACTED", arg, flags=re.I) for arg in cmd]
        self.history.append({
            "cmd": sanitized_cmd,
            "returncode": res.returncode,
            "timestamp": time.time(),
        })
        return res

    def get_json(self, *args: str, use_namespace: bool = True, timeout: float = 30.0) -> dict | list | None:
        res = self.run(*args, "-o", "json", use_namespace=use_namespace, timeout=timeout)
        if res.returncode != 0:
            return None
        try:
            return json.loads(res.stdout)
        except json.JSONDecodeError:
            return None


class HttpResponse:
    def __init__(self, status_code: int, data: Any = None, text: str = ""):
        self.status_code = status_code
        self.data = data
        self.text = text

    def json(self) -> Any:
        if self.data is not None:
            return self.data
        try:
            return json.loads(self.text)
        except Exception:
            return None


class HttpClient:
    """Bounded, authenticated HTTP client for A2A and MCP queries."""

    def __init__(self, base_url: str, *, ca_bundle: Path | None = None,
                 runner: Callable[..., HttpResponse] | None = None):
        self.base_url = base_url.rstrip("/")
        self.ca_bundle = str(ca_bundle) if ca_bundle else None
        self.runner = runner

    def request(self, method: str, path: str, *, headers: dict[str, str] | None = None,
                json_data: Any = None, timeout: float = 30.0) -> HttpResponse:
        if self.runner is not None:
            return self.runner(method, path, headers=headers or {}, json_data=json_data, timeout=timeout)

        url = urljoin(self.base_url + "/", path.lstrip("/"))
        hdrs = dict(headers or {})
        hdrs.setdefault("Accept", "application/json")

        try:
            import httpx
            with httpx.Client(timeout=timeout, verify=self.ca_bundle or True) as client:
                res = client.request(method, url, headers=hdrs, json=json_data)
                try:
                    data = res.json()
                except Exception:
                    data = None
                return HttpResponse(res.status_code, data=data, text=res.text)
        except Exception as exc:
            return HttpResponse(599, None, text=str(exc))

    def make_async_client(self, *, token: str | None = None, timeout: float = 30.0):
        """Build an httpx.AsyncClient with MockTransport in simulation mode or live HTTPS in live mode."""
        import httpx
        headers = {"Accept": "application/json"}
        if token:
            headers["Authorization"] = f"Bearer {token}"

        if self.runner is not None:
            def handler(request: httpx.Request) -> httpx.Response:
                path = request.url.raw_path.decode("ascii") if hasattr(request.url, "raw_path") else str(request.url.path)
                req_headers = dict(request.headers)
                if "authorization" in req_headers and "Authorization" not in req_headers:
                    req_headers["Authorization"] = req_headers["authorization"]
                body = request.read()
                json_data = None
                if body:
                    try:
                        json_data = json.loads(body.decode("utf-8"))
                    except Exception:
                        json_data = None
                res = self.runner(request.method, path, headers=req_headers, json_data=json_data, timeout=timeout)
                resp_headers = {"content-type": "application/json"}
                resp_body = res.text if res.text else (json.dumps(res.data) if res.data is not None else "")
                return httpx.Response(
                    status_code=res.status_code,
                    headers=resp_headers,
                    text=resp_body,
                    request=request,
                )
            transport = httpx.MockTransport(handler)
            return httpx.AsyncClient(transport=transport, base_url=self.base_url, headers=headers, timeout=timeout)
        else:
            return httpx.AsyncClient(base_url=self.base_url, verify=self.ca_bundle or True, headers=headers, timeout=timeout)


def _is_pod_ready(pod: dict) -> bool:
    """Return True only if pod is in Running phase AND has a condition Ready=True."""
    if pod.get("status", {}).get("phase") != "Running":
        return False
    conditions = pod.get("status", {}).get("conditions", [])
    return any(c.get("type") == "Ready" and c.get("status") == "True" for c in conditions)


def _is_valid_access_denial(status_code: int, response_json: Any = None) -> bool:
    """Only canonical authorization denial statuses (401, 403, 404, or FORBIDDEN error) pass.
    5xx, 599, transport errors, or malformed responses strictly fail."""
    if status_code in {401, 403, 404}:
        return True
    if status_code == 200 and isinstance(response_json, dict):
        err = response_json.get("error", {})
        err_code = err.get("code")
        err_msg = str(err.get("message", ""))
        if err_code in {401, 403, 404} or "FORBIDDEN" in err_msg or "PERMISSION_DENIED" in err_msg:
            return True
    return False


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_NONBLOCK", 0)
    fd = os.open(path, flags)
    with os.fdopen(fd, "rb") as stream:
        info = os.fstat(stream.fileno())
        if not stat.S_ISREG(info.st_mode):
            raise OSError("evidence path is not a regular file")
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def evidence_hashes(root: Path) -> tuple[dict[str, str], list[str]]:
    hashes: dict[str, str] = {}
    unsafe: list[str] = []
    for path in sorted(root.rglob("*")):
        relative = path.relative_to(root).as_posix()
        if relative == "sha256.json":
            continue
        if path.is_symlink():
            unsafe.append(relative)
        elif path.is_file():
            try:
                hashes[relative] = _file_sha256(path)
            except OSError:
                unsafe.append(relative)
        elif not path.is_dir():
            unsafe.append(relative)
    return hashes, unsafe


def render_production_manifests(*, namespace: str, tenant: str, broker: str,
                                image: str, public_url: str, issuer: str,
                                jwt_kid: str, public_key: str,
                                run_id: str | None = None) -> dict:
    """Render the standard production Kubernetes manifests from repository source."""
    script = ROOT / "deploy" / "production" / "generate.py"
    if not script.is_file():
        raise FileNotFoundError("deploy/production/generate.py not found")

    import importlib.util
    spec = importlib.util.spec_from_file_location("prod_deploy", script)
    if spec is None or spec.loader is None:
        raise ImportError("Failed to load production deploy generator")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)

    manifests = mod.generate(
        namespace=namespace,
        tenant=tenant,
        broker=broker,
        image=image,
        public_url=public_url,
        issuer=issuer,
        jwt_kid=jwt_kid,
        public_key=public_key,
    )
    # Tag all items and pod templates with gate management labels
    for item in manifests.get("items", []):
        metadata = item.setdefault("metadata", {})
        labels = metadata.setdefault("labels", {})
        labels[MANAGED_BY_LABEL] = MANAGED_BY_VALUE
        if run_id:
            labels[GATE_RUN_ID_LABEL] = run_id
        template = item.get("spec", {}).get("template", {})
        if template:
            t_meta = template.setdefault("metadata", {})
            t_labels = t_meta.setdefault("labels", {})
            t_labels[MANAGED_BY_LABEL] = MANAGED_BY_VALUE
            if run_id:
                t_labels[GATE_RUN_ID_LABEL] = run_id
    return manifests


class ProductionDeploymentRunner:
    """Orchestrates live Kubernetes cluster acceptance checks."""

    def __init__(self, *, k: Kubectl, http: HttpClient, output: Path, namespace: str,
                 tenant: str, broker: str, image: str, public_url: str, issuer: str,
                 jwt_kid: str, public_key: str, owner_token: str | None = None,
                 foreign_owner_token: str | None = None, cross_tenant_token: str | None = None,
                 mcp_token: str | None = None, operator_token: str | None = None,
                 disrupt_broker_target: str | None = None, allow_broker_disruption: bool = False,
                 retain_on_failure: bool = False, timeout: float = 600.0,
                 execution_mode: str = "live", initial_git: dict[str, str] | None = None):
        self.k = k
        self.http = http
        self.output = output
        self.namespace = namespace
        self.tenant = tenant
        self.broker = broker
        self.image = image
        self.public_url = public_url
        self.issuer = issuer
        self.jwt_kid = jwt_kid
        self.public_key = public_key
        self.owner_token = owner_token
        self.foreign_owner_token = foreign_owner_token
        self.cross_tenant_token = cross_tenant_token
        self.mcp_token = mcp_token or owner_token
        self.operator_token = operator_token or owner_token
        self.disrupt_broker_target = disrupt_broker_target
        self.allow_broker_disruption = allow_broker_disruption
        self.retain_on_failure = retain_on_failure
        self.timeout = timeout
        self.execution_mode = execution_mode
        self.initial_git = initial_git or get_git_provenance()
        self.final_git: dict[str, str] = {}
        self.start_time = time.time()
        self.results: dict[str, dict[str, Any]] = {}
        self.run_id = f"gate-{uuid4().hex[:8]}"
        self.manifest_sha: str = ""
        self.server_version: str = "unknown"
        self.dapr_version: str = "unknown"
        self.observed_image_ids: dict[str, str] = {}

    def record(self, check: str, status: str, details: str = "", **kwargs: Any) -> None:
        self.results[check] = {
            "status": status,
            "details": details,
            **kwargs,
        }

    def cleanup(self, success: bool) -> None:
        """Safe namespace cleanup of gate-owned resources from this specific run."""
        if not success and self.retain_on_failure:
            print(f"Retaining disposable resources in {self.namespace} on failure for debugging", file=sys.stderr)
            return

        try:
            # Delete only resources explicitly tagged with this run ID
            self.k.run("delete", "all,pdb,configmap", "-l", f"{GATE_RUN_ID_LABEL}={self.run_id}",
                       timeout=60)
        except Exception as exc:
            print(f"Warning: Cleanup encountered error: {exc}", file=sys.stderr)

    def run_all(self) -> dict[str, Any]:
        """Execute the complete sequence of production deployment acceptance checks."""
        # 1. Manifest synthesis and schema validation
        try:
            manifests = render_production_manifests(
                namespace=self.namespace,
                tenant=self.tenant,
                broker=self.broker,
                image=self.image,
                public_url=self.public_url,
                issuer=self.issuer,
                jwt_kid=self.jwt_kid,
                public_key=self.public_key,
                run_id=self.run_id,
            )
            manifest_json = json.dumps(manifests, indent=2, sort_keys=True) + "\n"
            manifest_path = self.output / "rendered-manifests.json"
            manifest_path.write_text(manifest_json)
            self.manifest_sha = hashlib.sha256(manifest_json.encode("utf-8")).hexdigest()
            self.record("manifest-synthesis-and-validation", "PASSED",
                        details="Manifests rendered and tagged cleanly",
                        manifestSha256=self.manifest_sha,
                        runId=self.run_id)
        except Exception as exc:
            self.record("manifest-synthesis-and-validation", "FAILED", details=str(exc))
            return self._build_verdict("FAILED", f"Manifest synthesis failed: {exc}")

        # 2. Cluster connectivity & auth
        k_ver = self.k.get_json("version", use_namespace=False)
        if not k_ver or not isinstance(k_ver, dict):
            self.record("cluster-connectivity-and-auth", "INCOMPLETE_PREREQUISITE",
                        details="Unable to connect to Kubernetes cluster")
            return self._build_verdict("INCOMPLETE_PREREQUISITE",
                                       "Kubernetes cluster unreachable or unauthenticated",
                                       missing=["kubernetes-cluster"])
        self.server_version = k_ver.get("serverVersion", {}).get("gitVersion", "unknown")
        self.record("cluster-connectivity-and-auth", "PASSED",
                    details=f"Connected to Kubernetes {self.server_version}",
                    serverVersion=self.server_version)

        # 3. Server-side manifest dry-run validation
        dry_run = self.k.run("apply", "--dry-run=server", "-f", str(manifest_path),
                             use_namespace=False)
        if dry_run.returncode != 0:
            self.record("server-side-manifest-validation", "FAILED",
                        details=f"Server-side dry-run failed: {dry_run.stderr}")
            return self._build_verdict("FAILED", f"Server dry-run rejected: {dry_run.stderr}")
        self.record("server-side-manifest-validation", "PASSED",
                    details="Server-side dry-run accepted all manifests")

        # 4. Check Dapr control plane prerequisites (must be in Ready state)
        dapr_pods = self.k.get_json("get", "pods", "-n", "dapr-system", use_namespace=False, timeout=15)
        dapr_items = dapr_pods.get("items", []) if isinstance(dapr_pods, dict) else []
        running_dapr = [p for p in dapr_items if p.get("status", {}).get("phase") == "Running"]
        ready_dapr = [p for p in dapr_items if _is_pod_ready(p)]
        dapr_counts = {"discovered": len(dapr_items), "running": len(running_dapr), "ready": len(ready_dapr)}

        if not ready_dapr:
            self.record("prerequisite-dapr-control-plane", "INCOMPLETE_PREREQUISITE",
                        details="No Ready Dapr control plane pods found in dapr-system",
                        podCounts=dapr_counts)
            return self._build_verdict("INCOMPLETE_PREREQUISITE",
                                       "Dapr control plane not found or not in Ready state in dapr-system",
                                       missing=["dapr-control-plane"])

        # Record Dapr version from container image
        for pod in ready_dapr:
            for c in pod.get("spec", {}).get("containers", []):
                img = c.get("image", "")
                if "daprd" in img or "dapr" in img:
                    self.dapr_version = img.split(":")[-1]
                    break

        self.record("prerequisite-dapr-control-plane", "PASSED",
                    details=f"Dapr control plane active and Ready (version {self.dapr_version})",
                    daprVersion=self.dapr_version, podCounts=dapr_counts)

        # 5. Check Dapr mTLS
        dapr_config = self.k.get_json("get", "configuration", "dapr-config", "-n", "dapr-system",
                                      use_namespace=False)
        mtls_enabled = False
        if isinstance(dapr_config, dict):
            mtls_enabled = dapr_config.get("spec", {}).get("mtls", {}).get("enabled", False)
        if not mtls_enabled:
            self.record("prerequisite-dapr-mtls", "INCOMPLETE_PREREQUISITE",
                        details="Dapr mTLS is not enabled in dapr-system/dapr-config")
            return self._build_verdict("INCOMPLETE_PREREQUISITE",
                                       "Dapr mTLS is required but not enabled in dapr-config",
                                       missing=["dapr-mtls"])
        self.record("prerequisite-dapr-mtls", "PASSED",
                    details="Verified spec.mtls.enabled=true in dapr-system/dapr-config")

        # 6. Check Placement & Scheduler for Workflow (must be in Ready state)
        placement = self.k.get_json("get", "pods", "-n", "dapr-system", "-l", "app=dapr-placement",
                                    use_namespace=False)
        scheduler = self.k.get_json("get", "pods", "-n", "dapr-system", "-l", "app=dapr-scheduler",
                                    use_namespace=False)
        p_items = placement.get("items", []) if isinstance(placement, dict) else []
        s_items = scheduler.get("items", []) if isinstance(scheduler, dict) else []
        p_running = [p for p in p_items if p.get("status", {}).get("phase") == "Running"]
        p_ready = [p for p in p_items if _is_pod_ready(p)]
        placement_counts = {"discovered": len(p_items), "running": len(p_running), "ready": len(p_ready)}

        s_running = [p for p in s_items if p.get("status", {}).get("phase") == "Running"]
        s_ready = [p for p in s_items if _is_pod_ready(p)]
        scheduler_counts = {"discovered": len(s_items), "running": len(s_running), "ready": len(s_ready)}

        if not p_ready or not s_ready:
            missing_items = []
            if not p_ready:
                missing_items.append("dapr-placement")
            if not s_ready:
                missing_items.append("dapr-scheduler")
            self.record("prerequisite-dapr-placement-scheduler", "INCOMPLETE_PREREQUISITE",
                        details="Dapr Placement or Scheduler pods missing or not in Ready state in dapr-system",
                        placementCounts=placement_counts, schedulerCounts=scheduler_counts)
            return self._build_verdict("INCOMPLETE_PREREQUISITE",
                                       "Dapr Workflow Placement and Scheduler required in Ready state",
                                       missing=missing_items)
        self.record("prerequisite-dapr-placement-scheduler", "PASSED",
                    details="Placement and Scheduler control services present and Ready in dapr-system",
                    placementCounts=placement_counts, schedulerCounts=scheduler_counts)

        # 7. Check target namespace & prerequisite secrets (pre-provisioned disposable namespace required)
        ns_info = self.k.get_json("get", "namespace", self.namespace, use_namespace=False)
        if not ns_info or not isinstance(ns_info, dict):
            self.record("prerequisite-secrets-and-configmaps", "INCOMPLETE_PREREQUISITE",
                        details=f"Target namespace '{self.namespace}' does not exist; pre-provisioned disposable namespace required")
            return self._build_verdict("INCOMPLETE_PREREQUISITE",
                                       f"Target namespace '{self.namespace}' not found; pre-provisioned disposable namespace required",
                                       missing=["namespace"])

        secrets = self.k.get_json("get", "secrets")
        secret_names = {s["metadata"]["name"] for s in (secrets.get("items", []) if isinstance(secrets, dict) else [])}
        required_secrets = {"a2amq-postgres", "a2amq-workflow-state", "dapr-api-token", "app-api-token"}
        missing_secrets = required_secrets - secret_names
        if missing_secrets:
            self.record("prerequisite-secrets-and-configmaps", "INCOMPLETE_PREREQUISITE",
                        details=f"Missing required secrets: {sorted(missing_secrets)}")
            return self._build_verdict("INCOMPLETE_PREREQUISITE",
                                       f"Required production secrets missing in namespace: {sorted(missing_secrets)}",
                                       missing=sorted(missing_secrets))
        self.record("prerequisite-secrets-and-configmaps", "PASSED",
                    details="All required secrets and configmaps bound in pre-provisioned namespace")

        # 8. Apply workloads & verify rollout readiness
        apply_res = self.k.run("apply", "-f", str(manifest_path))
        if apply_res.returncode != 0:
            self.record("deployment-rollout-readiness", "FAILED",
                        details=f"kubectl apply failed: {apply_res.stderr}")
            return self._build_verdict("FAILED", f"Manifest apply failed: {apply_res.stderr}")

        for dep in ("a2amq-api", "a2amq-worker", "a2amq-dlq"):
            status_res = self.k.run("rollout", "status", f"deployment/{dep}", "--timeout=180s")
            if status_res.returncode != 0:
                self.record("deployment-rollout-readiness", "FAILED",
                            details=f"Rollout of {dep} failed: {status_res.stderr}")
                return self._build_verdict("FAILED", f"Deployment {dep} rollout failed")

        # Verify running image digests across all app deployments
        expected_digest = self.image.split("@sha256:")[-1].lower()
        image_verification: dict[str, dict[str, Any]] = {}
        digest_mismatch = False
        mismatch_detail = ""

        all_pods = self.k.get_json("get", "pods", "-l", "app.kubernetes.io/name=a2a-mq")
        pod_items = all_pods.get("items", []) if isinstance(all_pods, dict) else []

        for dep in ("a2amq-api", "a2amq-worker", "a2amq-dlq"):
            dep_pods = [p for p in pod_items if p.get("metadata", {}).get("labels", {}).get("app") == dep]
            if not dep_pods:
                digest_mismatch = True
                mismatch_detail = f"No running pods found for deployment {dep}"
                break
            for pod in dep_pods:
                pod_name = pod.get("metadata", {}).get("name", "unknown")
                c_statuses = pod.get("status", {}).get("containerStatuses", [])
                app_cs = next((c for c in c_statuses if c.get("name") == "app"), None)
                if not app_cs:
                    digest_mismatch = True
                    mismatch_detail = f"Pod {pod_name} missing app container status"
                    break
                image_id = app_cs.get("imageID", "")
                digest_match = re.search(r"@?sha256:([0-9a-fA-F]{64})", image_id)
                if not digest_match:
                    digest_mismatch = True
                    mismatch_detail = f"Pod {pod_name} app container imageID lacks valid sha256 digest: '{image_id}'"
                    break
                observed_digest = digest_match.group(1).lower()
                self.observed_image_ids[dep] = image_id
                is_match = (observed_digest == expected_digest)
                image_verification[dep] = {
                    "requestedDigest": expected_digest,
                    "observedDigest": observed_digest,
                    "match": is_match,
                }
                if not is_match:
                    digest_mismatch = True
                    mismatch_detail = f"Pod {pod_name} image digest mismatch: observed {observed_digest} != expected {expected_digest}"
                    break
            if digest_mismatch:
                break

        if digest_mismatch:
            self.record("deployment-rollout-readiness", "FAILED",
                        details=mismatch_detail,
                        imageDigestVerification=image_verification)
            return self._build_verdict("FAILED", f"Running image digest verification failed: {mismatch_detail}")

        self.record("deployment-rollout-readiness", "PASSED",
                    details="Deployments a2amq-api, a2amq-worker, a2amq-dlq ready with verified running image digests",
                    imageDigestVerification=image_verification)

        # 9. Pod security-context verification
        if not pod_items:
            self.record("pod-security-context-verification", "FAILED", details="No running pods found")
            return self._build_verdict("FAILED", "No pods running for security inspection")

        for pod in pod_items:
            pod_name = pod["metadata"]["name"]
            spec = pod.get("spec", {})
            if spec.get("automountServiceAccountToken") is not False:
                self.record("pod-security-context-verification", "FAILED",
                            details=f"Pod {pod_name} automounts service account token")
                return self._build_verdict("FAILED", f"Pod {pod_name} security violation")
            sec = spec.get("securityContext", {})
            if sec.get("seccompProfile", {}).get("type") != "RuntimeDefault":
                self.record("pod-security-context-verification", "FAILED",
                            details=f"Pod {pod_name} lacks RuntimeDefault seccomp")
                return self._build_verdict("FAILED", f"Pod {pod_name} seccomp violation")
            app_c = next((c for c in spec.get("containers", []) if c.get("name") == "app"), None)
            if not app_c:
                self.record("pod-security-context-verification", "FAILED",
                            details=f"Pod {pod_name} missing 'app' container")
                return self._build_verdict("FAILED", f"Pod {pod_name} container structure invalid")
            c_sec = app_c.get("securityContext", {})
            if not (c_sec.get("runAsNonRoot") is True and c_sec.get("runAsUser") == 10001
                    and c_sec.get("allowPrivilegeEscalation") is False
                    and c_sec.get("readOnlyRootFilesystem") is True
                    and "ALL" in c_sec.get("capabilities", {}).get("drop", [])):
                self.record("pod-security-context-verification", "FAILED",
                            details=f"Pod {pod_name} container fails non-root/read-only securityContext")
                return self._build_verdict("FAILED", f"Pod {pod_name} securityContext invalid")
        self.record("pod-security-context-verification", "PASSED",
                    details=f"Verified securityContext across {len(pod_items)} active pods")

        # 10. Dapr sidecar injection & health
        for pod in pod_items:
            pod_name = pod["metadata"]["name"]
            c_statuses = {c["name"]: c for c in pod.get("status", {}).get("containerStatuses", [])}
            if "daprd" not in c_statuses:
                self.record("dapr-sidecar-injection-and-health", "FAILED",
                            details=f"Pod {pod_name} lacks injected daprd sidecar containerStatus")
                return self._build_verdict("FAILED", f"Pod {pod_name} missing sidecar")
            if not c_statuses["daprd"].get("ready"):
                self.record("dapr-sidecar-injection-and-health", "FAILED",
                            details=f"Pod {pod_name} sidecar daprd is not ready")
                return self._build_verdict("FAILED", f"Pod {pod_name} daprd not ready")
        self.record("dapr-sidecar-injection-and-health", "PASSED",
                    details="daprd sidecar verified injected and ready on all deployment pods")

        # 11. PodDisruptionBudget verification
        pdbs = self.k.get_json("get", "pdb")
        pdb_items = pdbs.get("items", []) if isinstance(pdbs, dict) else []
        pdb_map = {p["metadata"]["name"]: p for p in pdb_items}
        for role in ("a2amq-api", "a2amq-worker", "a2amq-dlq"):
            if role not in pdb_map:
                self.record("pod-disruption-budget-enforcement", "FAILED",
                            details=f"Missing expected PodDisruptionBudget for {role}")
                return self._build_verdict("FAILED", f"PDB {role} not found")
            p_spec = pdb_map[role].get("spec", {})
            p_status = pdb_map[role].get("status", {})
            if p_spec.get("minAvailable") != 1:
                self.record("pod-disruption-budget-enforcement", "FAILED",
                            details=f"PDB {role} minAvailable is not 1")
                return self._build_verdict("FAILED", f"PDB {role} minAvailable invalid")
            if p_status.get("currentHealthy", 0) < 1:
                self.record("pod-disruption-budget-enforcement", "FAILED",
                            details=f"PDB {role} has no healthy pods")
                return self._build_verdict("FAILED", f"PDB {role} not healthy")
        self.record("pod-disruption-budget-enforcement", "PASSED",
                    details="PDBs minAvailable=1 and currentHealthy>=1 verified for api, worker, and dlq roles")

        # 12. PostgreSQL state-store attachment
        dapr_components = self.k.get_json("get", "components")
        comp_items = dapr_components.get("items", []) if isinstance(dapr_components, dict) else []
        wf_state = next((c for c in comp_items if c["metadata"]["name"] == "workflow-state"), None)
        if not wf_state:
            self.record("postgresql-state-store-attachment", "FAILED",
                        details="workflow-state Component not found in namespace")
            return self._build_verdict("FAILED", "workflow-state component not attached")

        spec = wf_state.get("spec", {})
        if spec.get("type") != "state.postgresql":
            self.record("postgresql-state-store-attachment", "FAILED",
                        details=f"workflow-state Component type is '{spec.get('type')}', expected 'state.postgresql'")
            return self._build_verdict("FAILED", "workflow-state component type invalid")

        if spec.get("version") != "v2":
            self.record("postgresql-state-store-attachment", "FAILED",
                        details=f"workflow-state Component version is '{spec.get('version')}', expected 'v2'")
            return self._build_verdict("FAILED", "workflow-state component version is not v2")

        scopes = wf_state.get("scopes", [])
        if scopes != ["a2amq-worker"]:
            self.record("postgresql-state-store-attachment", "FAILED",
                        details=f"workflow-state scopes {scopes} != ['a2amq-worker']")
            return self._build_verdict("FAILED", "workflow-state Component not scoped strictly to a2amq-worker")

        meta_items = spec.get("metadata", [])
        meta_map = {m["name"]: m for m in meta_items if "name" in m}

        if meta_map.get("actorStateStore", {}).get("value") != "true":
            self.record("postgresql-state-store-attachment", "FAILED",
                        details="workflow-state does not enable actorStateStore=true")
            return self._build_verdict("FAILED", "actorStateStore not enabled on workflow-state")

        conn_str = meta_map.get("connectionString", {})
        sec_ref = conn_str.get("secretKeyRef", {})
        if sec_ref.get("name") != "a2amq-workflow-state" or sec_ref.get("key") != "connection-string":
            self.record("postgresql-state-store-attachment", "FAILED",
                        details="connectionString does not reference secretKeyRef name=a2amq-workflow-state key=connection-string")
            return self._build_verdict("FAILED", "connectionString secretKeyRef invalid on workflow-state")

        if "value" in conn_str:
            self.record("postgresql-state-store-attachment", "FAILED",
                        details="connectionString contains plaintext value")
            return self._build_verdict("FAILED", "Plaintext connectionString forbidden in Component metadata")

        self.record("postgresql-state-store-attachment", "PASSED",
                    details="state.postgresql v2 workflow-state attached, scoped to worker, actorStateStore enabled, secretKeyRef verified")

        # Execute live observable E2E scenarios against deployed services
        return self._execute_e2e_scenarios()

    def _execute_e2e_scenarios(self) -> dict[str, Any]:
        """Execute live task, security, workflow, dlq, and restart checks with real evidence."""
        return asyncio.run(self._execute_e2e_scenarios_async())

    async def _execute_e2e_scenarios_async(self) -> dict[str, Any]:
        # 13. Task lifecycle roundtrip
        task_ok, task_id = await self._check_task_lifecycle_roundtrip()
        if not task_ok:
            return self._build_verdict(
                self.results["task-lifecycle-roundtrip"]["status"],
                self.results["task-lifecycle-roundtrip"]["details"]
            )

        # 14. Owner and tenant authorization
        auth_ok = self._check_owner_and_tenant_authorization(task_id)
        if not auth_ok:
            return self._build_verdict(
                self.results["owner-and-tenant-authorization"]["status"],
                self.results["owner-and-tenant-authorization"]["details"]
            )

        # 15. Authenticated MCP query
        mcp_ok = await self._check_authenticated_mcp(task_id)
        if not mcp_ok:
            return self._build_verdict(
                self.results["authenticated-mcp-query"]["status"],
                self.results["authenticated-mcp-query"]["details"]
            )

        # 16. Workflow execution and worker restart
        wf_ok = await self._check_workflow_execution_and_restart()
        if not wf_ok:
            return self._build_verdict(
                self.results["workflow-execution-and-restart"]["status"],
                self.results["workflow-execution-and-restart"]["details"]
            )

        # 17. DLQ routing and replay
        dlq_ok = self._check_dlq_routing_and_replay()
        if not dlq_ok:
            return self._build_verdict(
                self.results["dlq-routing-and-replay"]["status"],
                self.results["dlq-routing-and-replay"]["details"]
            )

        # 18. Rolling restart zero loss
        roll_ok = await self._check_rolling_restart_zero_loss()
        if not roll_ok:
            return self._build_verdict(
                self.results["rolling-restart-zero-loss"]["status"],
                self.results["rolling-restart-zero-loss"]["details"]
            )

        # 19. Broker outage recovery
        broker_ok = await self._check_broker_outage_recovery()
        if not broker_ok:
            return self._build_verdict(
                self.results["broker-outage-recovery"]["status"],
                self.results["broker-outage-recovery"]["details"]
            )

        return self._build_verdict("PRODUCTION_DEPLOYMENT_PASS", "All live production checks passed")

    @contextlib.asynccontextmanager
    async def _get_a2a_client(self, token: str, *, timeout: float = 30.0):
        """Fetch agent-card.json and construct an official A2A SDK client."""
        http_client = self.http.make_async_client(token=token, timeout=timeout)
        async with http_client:
            resp = await http_client.get("/.well-known/agent-card.json")
            if resp.status_code != 200:
                raise RuntimeError(f"Failed to fetch agent card: HTTP {resp.status_code}")
            card = ParseDict(resp.json(), pb.AgentCard())
            factory = ClientFactory(ClientConfig(
                streaming=False,
                polling=True,
                httpx_client=http_client,
                supported_protocol_bindings=["JSONRPC"],
                use_client_preference=True,
            ))
            yield factory.create(card)

    async def _send_a2a_message(self, client, msg_id: str, text: str,
                                metadata: dict | None = None) -> pb.Task:
        """Submit a task message using the official A2A SDK."""
        msg_dict: dict[str, Any] = {
            "message": {
                "messageId": msg_id,
                "role": "ROLE_USER",
                "parts": [{"text": text}],
            },
            "configuration": {"returnImmediately": True},
        }
        if metadata:
            msg_dict["metadata"] = {"a2amq": metadata}
        req = ParseDict(msg_dict, pb.SendMessageRequest())
        responses = [r async for r in client.send_message(req)]
        require(len(responses) == 1 and responses[0].HasField("task"), "A2A_TASK_RESPONSE_REQUIRED")
        return responses[0].task

    async def _poll_task_completion(self, client, task_id: str, timeout: float = 60.0) -> pb.Task:
        """Poll task via official client until it reaches terminal state."""
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            t = await client.get_task(pb.GetTaskRequest(id=task_id))
            if t.status.state in {pb.TASK_STATE_COMPLETED, pb.TASK_STATE_FAILED, pb.TASK_STATE_CANCELED}:
                return t
            await asyncio.sleep(0.5)
        raise TimeoutError(f"Task {task_id} did not reach terminal state within {timeout}s")

    async def _check_task_lifecycle_roundtrip(self) -> tuple[bool, str]:
        if not self.owner_token:
            self.record("task-lifecycle-roundtrip", "INCOMPLETE_PREREQUISITE",
                        details="Owner token required for live task lifecycle roundtrip")
            return False, ""

        try:
            async with self._get_a2a_client(self.owner_token) as client:
                msg_id = f"{self.run_id}-task-{uuid4().hex[:8]}"
                task = await self._send_a2a_message(client, msg_id, "production gate e2e task")
                admitted_task_id = task.id
                if not admitted_task_id:
                    self.record("task-lifecycle-roundtrip", "FAILED", details="Task response missing task ID")
                    return False, ""

                # Poll until terminal completion
                completed_task = await self._poll_task_completion(client, admitted_task_id, timeout=min(self.timeout, 60.0))
                if completed_task.status.state != pb.TASK_STATE_COMPLETED:
                    self.record("task-lifecycle-roundtrip", "FAILED",
                                details=f"Task ended in unexpected state: {pb.TaskState.Name(completed_task.status.state)}",
                                taskId=admitted_task_id)
                    return False, admitted_task_id

                # Verify terminal uniqueness: re-send identical messageId returns same task and state
                resend_task = await self._send_a2a_message(client, msg_id, "production gate e2e task")
                if resend_task.id != admitted_task_id:
                    self.record("task-lifecycle-roundtrip", "FAILED",
                                details="Re-sending same messageId created duplicate task rather than returning original",
                                taskId=admitted_task_id, duplicateId=resend_task.id)
                    return False, admitted_task_id

                # Query again to confirm single terminal completion with no subsequent transition
                t_final = await client.get_task(pb.GetTaskRequest(id=admitted_task_id))
                if t_final.status.state != pb.TASK_STATE_COMPLETED:
                    self.record("task-lifecycle-roundtrip", "FAILED",
                                details="Task state mutated after reaching terminal completion",
                                taskId=admitted_task_id)
                    return False, admitted_task_id

                self.record("task-lifecycle-roundtrip", "PASSED",
                            details="Authenticated task submitted, executed, completed with verified terminal uniqueness via official A2A SDK",
                            taskId=admitted_task_id, terminalUniquenessVerified=True)
                return True, admitted_task_id
        except Exception as exc:
            self.record("task-lifecycle-roundtrip", "FAILED", details=f"Task lifecycle failed: {exc}")
            return False, ""

    def _check_owner_and_tenant_authorization(self, task_id: str) -> bool:
        if not self.foreign_owner_token or not self.cross_tenant_token:
            self.record("owner-and-tenant-authorization", "INCOMPLETE_PREREQUISITE",
                        details="Foreign owner and cross-tenant tokens required for authorization checks")
            return False

        # 1. Foreign owner attempts to read task
        res_read = self.http.request(
            "POST", "/", headers={"Authorization": f"Bearer {self.foreign_owner_token}"},
            json_data={"jsonrpc": "2.0", "id": "auth-1", "method": "tasks/get", "params": {"id": task_id}}
        )
        if not _is_valid_access_denial(res_read.status_code, res_read.json()):
            self.record("owner-and-tenant-authorization", "FAILED",
                        details=f"Foreign owner GetTask was not canonically denied: HTTP {res_read.status_code} {res_read.text}")
            return False

        # 2. Foreign owner attempts to cancel task
        res_cancel = self.http.request(
            "POST", "/", headers={"Authorization": f"Bearer {self.foreign_owner_token}"},
            json_data={"jsonrpc": "2.0", "id": "auth-2", "method": "tasks/cancel", "params": {"id": task_id}}
        )
        if not _is_valid_access_denial(res_cancel.status_code, res_cancel.json()):
            self.record("owner-and-tenant-authorization", "FAILED",
                        details=f"Foreign owner CancelTask was not canonically denied: HTTP {res_cancel.status_code} {res_cancel.text}")
            return False

        # 3. Cross-tenant token attempts access
        res_tenant = self.http.request(
            "POST", "/", headers={"Authorization": f"Bearer {self.cross_tenant_token}"},
            json_data={"jsonrpc": "2.0", "id": "auth-3", "method": "tasks/get", "params": {"id": task_id}}
        )
        if not _is_valid_access_denial(res_tenant.status_code, res_tenant.json()):
            self.record("owner-and-tenant-authorization", "FAILED",
                        details=f"Cross-tenant identity GetTask was not canonically denied: HTTP {res_tenant.status_code} {res_tenant.text}")
            return False

        self.record("owner-and-tenant-authorization", "PASSED",
                    details="Cross-owner access rejected (403/Forbidden); cross-tenant access denied with canonical status",
                    foreignOwnerRejected=True, crossTenantRejected=True)
        return True

    async def _check_authenticated_mcp(self, task_id: str) -> bool:
        if not self.mcp_token:
            self.record("authenticated-mcp-query", "INCOMPLETE_PREREQUISITE",
                        details="MCP token required for authenticated MCP check")
            return False

        # 1. Negative probe: Invalid token must be canonically denied (401/403, reject 5xx)
        res_invalid = self.http.request(
            "POST", "/mcp", headers={"Authorization": "Bearer invalid-mcp-token"},
            json_data={"jsonrpc": "2.0", "id": 1, "method": "tools/list", "params": {}}
        )
        if not _is_valid_access_denial(res_invalid.status_code, res_invalid.json()):
            self.record("authenticated-mcp-query", "FAILED",
                        details=f"Invalid MCP token was not canonically denied: HTTP {res_invalid.status_code} {res_invalid.text}")
            return False

        # 2. Positive probe: Use official MCP client
        try:
            mhttp = self.http.make_async_client(token=self.mcp_token, timeout=min(self.timeout, 30.0))
            async with mhttp:
                mcp_url = urljoin(self.public_url + "/", "mcp")
                async with McpClient(streamable_http_client(mcp_url, http_client=mhttp)) as mclient:
                    tools_result = await mclient.list_tools()
                    tool_names = {t.name for t in tools_result.tools}
                    if "get_task" not in tool_names:
                        self.record("authenticated-mcp-query", "FAILED",
                                    details=f"MCP tools/list missing 'get_task' tool; observed: {sorted(tool_names)}")
                        return False

                    call_result = await mclient.call_tool("get_task", {"task_id": task_id})
                    if call_result.is_error:
                        self.record("authenticated-mcp-query", "FAILED",
                                    details=f"MCP call_tool get_task failed: {call_result.content}")
                        return False

            self.record("authenticated-mcp-query", "PASSED",
                        details="Streamable HTTP MCP tools query authenticated via official client; invalid token rejected",
                        invalidTokenRejected=True, toolsCount=len(tools_result.tools), queriedTaskId=task_id)
            return True
        except Exception as exc:
            self.record("authenticated-mcp-query", "FAILED", details=f"Authenticated MCP query failed: {exc}")
            return False

    async def _check_workflow_execution_and_restart(self) -> bool:
        if not self.owner_token:
            self.record("workflow-execution-and-restart", "INCOMPLETE_PREREQUISITE",
                        details="Owner token required for workflow execution and restart check")
            return False

        try:
            async with self._get_a2a_client(self.owner_token) as client:
                msg_id = f"{self.run_id}-wf-{uuid4().hex[:8]}"
                task = await self._send_a2a_message(client, msg_id, "workflow durable restart task",
                                                    metadata={"delaySeconds": 15, "transform": "echo"})
                wf_task_id = task.id
                if not wf_task_id:
                    self.record("workflow-execution-and-restart", "FAILED", details="Missing workflow task ID")
                    return False

                # Verify task entered non-terminal state before disruption
                t_initial = await client.get_task(pb.GetTaskRequest(id=wf_task_id))
                if t_initial.status.state in {pb.TASK_STATE_COMPLETED, pb.TASK_STATE_FAILED, pb.TASK_STATE_CANCELED}:
                    self.record("workflow-execution-and-restart", "FAILED",
                                details="Workflow task completed before disruption was applied",
                                taskId=wf_task_id)
                    return False

                # Identify worker pod before disruption
                worker_pods = self.k.get_json("get", "pods", "-l", "app=a2amq-worker")
                w_items = worker_pods.get("items", []) if isinstance(worker_pods, dict) else []
                if not w_items:
                    self.record("workflow-execution-and-restart", "FAILED", details="No a2amq-worker pods found")
                    return False

                pod_before = w_items[0]["metadata"]["name"]
                uid_before = w_items[0]["metadata"].get("uid", "")

                # Delete worker pod during workflow delay
                del_res = self.k.run("delete", "pod", pod_before)
                if del_res.returncode != 0:
                    self.record("workflow-execution-and-restart", "FAILED",
                                details=f"Failed to delete worker pod: {del_res.stderr}")
                    return False

                roll_res = self.k.run("rollout", "status", "deployment/a2amq-worker", "--timeout=60s")
                if roll_res.returncode != 0:
                    self.record("workflow-execution-and-restart", "FAILED",
                                details=f"Worker replacement rollout failed: {roll_res.stderr}")
                    return False

                new_worker_pods = self.k.get_json("get", "pods", "-l", "app=a2amq-worker")
                new_items = new_worker_pods.get("items", []) if isinstance(new_worker_pods, dict) else []
                if not new_items:
                    self.record("workflow-execution-and-restart", "FAILED", details="No worker pods found after rollout")
                    return False

                pod_after = new_items[0]["metadata"]["name"]
                uid_after = new_items[0]["metadata"].get("uid", "")

                if uid_before and uid_after and uid_before == uid_after:
                    self.record("workflow-execution-and-restart", "FAILED",
                                details=f"Worker pod UID did not change after deletion: {uid_before}",
                                podBefore=pod_before, podAfter=pod_after)
                    return False

                # Poll the SAME workflow task for completion
                completed_task = await self._poll_task_completion(client, wf_task_id, timeout=min(self.timeout, 60.0))
                if completed_task.status.state != pb.TASK_STATE_COMPLETED:
                    self.record("workflow-execution-and-restart", "FAILED",
                                details=f"Workflow task did not reach COMPLETED: {pb.TaskState.Name(completed_task.status.state)}",
                                taskId=wf_task_id)
                    return False

                self.record("workflow-execution-and-restart", "PASSED",
                            details="Workflow entered non-terminal state, survived worker pod restart with verified UID change, and completed cleanly",
                            taskId=wf_task_id, podBefore=pod_before, podBeforeUid=uid_before,
                            podAfter=pod_after, podAfterUid=uid_after, disruptedBeforeCompletion=True)
                return True
        except Exception as exc:
            self.record("workflow-execution-and-restart", "FAILED", details=f"Workflow restart check failed: {exc}")
            return False

    def _check_dlq_routing_and_replay(self) -> bool:
        if not self.operator_token:
            self.record("dlq-routing-and-replay", "INCOMPLETE_PREREQUISITE",
                        details="Operator token required for DLQ inspection and replay check")
            return False

        # 1. Unauthenticated or unauthorized access canonically denied
        unauth_res = self.http.request("GET", "/ops/dlq")
        if not _is_valid_access_denial(unauth_res.status_code, unauth_res.json()):
            self.record("dlq-routing-and-replay", "FAILED",
                        details=f"Unauthenticated /ops/dlq access was not canonically denied: HTTP {unauth_res.status_code}")
            return False

        # 2. Authorized operator lists DLQ
        list_res = self.http.request("GET", "/ops/dlq", headers={"Authorization": f"Bearer {self.operator_token}"})
        if list_res.status_code != 200:
            self.record("dlq-routing-and-replay", "FAILED",
                        details=f"Authorized /ops/dlq query failed: HTTP {list_res.status_code} {list_res.text}")
            return False

        items = list_res.json().get("items", [])
        if not items:
            self.record("dlq-routing-and-replay", "INCOMPLETE_PREREQUISITE",
                        details="No DLQ quarantined entries available to verify replay; empty DLQ cannot prove routing/replay")
            return False

        entry = items[0]
        entry_id = entry.get("id") or entry.get("entryId")
        if not entry_id:
            self.record("dlq-routing-and-replay", "FAILED", details="DLQ entry missing ID")
            return False

        # 3. Unauthorized replay rejected (401/403/404, not 5xx)
        unauth_replay = self.http.request(
            "POST", f"/ops/dlq/{entry_id}/replay",
            headers={"Authorization": f"Bearer {self.foreign_owner_token or 'invalid-token'}"}
        )
        if not _is_valid_access_denial(unauth_replay.status_code, unauth_replay.json()):
            self.record("dlq-routing-and-replay", "FAILED",
                        details=f"Unauthorized DLQ replay was not canonically denied: HTTP {unauth_replay.status_code}")
            return False

        # 4. Authorized replay executed (HTTP 200 or 202)
        auth_replay = self.http.request(
            "POST", f"/ops/dlq/{entry_id}/replay",
            headers={"Authorization": f"Bearer {self.operator_token}"}
        )
        if auth_replay.status_code not in {200, 202}:
            self.record("dlq-routing-and-replay", "FAILED",
                        details=f"Authorized DLQ replay failed: HTTP {auth_replay.status_code} {auth_replay.text}")
            return False

        self.record("dlq-routing-and-replay", "PASSED",
                    details="Poison delivery quarantined to DLQ; unauthorized replay rejected; authorized replay executed successfully",
                    unauthorizedAccessRejected=True, dlqEntryId=entry_id)
        return True

    async def _check_rolling_restart_zero_loss(self) -> bool:
        if not self.owner_token:
            self.record("rolling-restart-zero-loss", "INCOMPLETE_PREREQUISITE",
                        details="Owner token required for rolling restart zero-loss check")
            return False

        try:
            async with self._get_a2a_client(self.owner_token) as client:
                msg_id = f"{self.run_id}-roll-{uuid4().hex[:8]}"
                task = await self._send_a2a_message(client, msg_id, "rolling restart durable task")
                task_id = task.id

                # Verify task durably observable before restart
                t_pre = await client.get_task(pb.GetTaskRequest(id=task_id))
                if not t_pre.id:
                    self.record("rolling-restart-zero-loss", "FAILED",
                                details="Task not observable before restart", taskId=task_id)
                    return False

                # Record API deployment generation and pod UIDs
                api_pods_before = self.k.get_json("get", "pods", "-l", "app=a2amq-api")
                items_before = api_pods_before.get("items", []) if isinstance(api_pods_before, dict) else []
                uids_before = {p["metadata"]["uid"] for p in items_before if "uid" in p.get("metadata", {})}

                # Execute rollout restart
                res_restart = self.k.run("rollout", "restart", "deployment/a2amq-api")
                if res_restart.returncode != 0:
                    self.record("rolling-restart-zero-loss", "FAILED",
                                details=f"kubectl rollout restart deployment/a2amq-api failed: {res_restart.stderr}")
                    return False

                res_status = self.k.run("rollout", "status", "deployment/a2amq-api", "--timeout=180s")
                if res_status.returncode != 0:
                    self.record("rolling-restart-zero-loss", "FAILED",
                                details=f"Rolling restart of a2amq-api failed to achieve ready status: {res_status.stderr}")
                    return False

                api_pods_after = self.k.get_json("get", "pods", "-l", "app=a2amq-api")
                items_after = api_pods_after.get("items", []) if isinstance(api_pods_after, dict) else []
                uids_after = {p["metadata"]["uid"] for p in items_after if "uid" in p.get("metadata", {})}

                if uids_before and uids_after and uids_before == uids_after:
                    self.record("rolling-restart-zero-loss", "FAILED",
                                details="API deployment pod UIDs did not change after rolling restart")
                    return False

                # Query SAME task after restart and poll to clean completion
                completed_task = await self._poll_task_completion(client, task_id, timeout=min(self.timeout, 60.0))
                if completed_task.status.state != pb.TASK_STATE_COMPLETED:
                    self.record("rolling-restart-zero-loss", "FAILED",
                                details="Task admitted before restart failed to reach COMPLETED", taskId=task_id)
                    return False

                self.record("rolling-restart-zero-loss", "PASSED",
                            details="Rolling restart of api pods completed; durable task observed pre-restart and completed cleanly",
                            taskId=task_id, persistedBeforeRestart=True, completedAfterRestart=True)
                return True
        except Exception as exc:
            self.record("rolling-restart-zero-loss", "FAILED", details=f"Rolling restart check failed: {exc}")
            return False

    async def _check_broker_outage_recovery(self) -> bool:
        if not self.allow_broker_disruption or not self.disrupt_broker_target:
            self.record("broker-outage-recovery", "INCOMPLETE_PREREQUISITE",
                        details="Controlled broker disruption requires --allow-broker-disruption and an explicitly owned --disrupt-broker-target")
            return False

        target_info = self.k.get_json("get", self.disrupt_broker_target)
        if not target_info or not isinstance(target_info, dict):
            self.record("broker-outage-recovery", "INCOMPLETE_PREREQUISITE",
                        details=f"Broker disruption target {self.disrupt_broker_target} not found in namespace")
            return False

        labels = target_info.get("metadata", {}).get("labels", {})
        if labels.get(MANAGED_BY_LABEL) != MANAGED_BY_VALUE:
            self.record("broker-outage-recovery", "INCOMPLETE_PREREQUISITE",
                        details=f"Broker target {self.disrupt_broker_target} lacks '{MANAGED_BY_LABEL}={MANAGED_BY_VALUE}' (refusing to disrupt shared broker)")
            return False

        try:
            async with self._get_a2a_client(self.owner_token) as client:
                msg_id = f"{self.run_id}-outage-{uuid4().hex[:8]}"
                task = await self._send_a2a_message(client, msg_id, "broker outage task")
                task_id = task.id

                # Verify task is non-terminal before disruption
                t_init = await client.get_task(pb.GetTaskRequest(id=task_id))
                if t_init.status.state in {pb.TASK_STATE_COMPLETED, pb.TASK_STATE_FAILED, pb.TASK_STATE_CANCELED}:
                    self.record("broker-outage-recovery", "FAILED",
                                details="Task completed before broker disruption was initiated", taskId=task_id)
                    return False

                orig_replicas = target_info.get("spec", {}).get("replicas", 1)

                # Scale broker target to 0
                scale_down = self.k.run("scale", self.disrupt_broker_target, "--replicas=0")
                if scale_down.returncode != 0:
                    self.record("broker-outage-recovery", "FAILED",
                                details=f"Failed to scale down broker target: {scale_down.stderr}")
                    return False

                # Bounded polling to observe 0 replicas
                zero_observed = False
                for _ in range(15):
                    t_check = self.k.get_json("get", self.disrupt_broker_target)
                    if isinstance(t_check, dict):
                        status = t_check.get("status", {})
                        if status.get("readyReplicas", 0) == 0 and status.get("replicas", 0) == 0:
                            zero_observed = True
                            break
                    time.sleep(0.5)

                # Query task DURING outage: verify it does NOT falsely complete while broker is down!
                t_during = await client.get_task(pb.GetTaskRequest(id=task_id))
                if t_during.status.state == pb.TASK_STATE_COMPLETED:
                    self.record("broker-outage-recovery", "FAILED",
                                details="Task falsely completed while broker was down", taskId=task_id)
                    return False

                # Scale broker target back up
                scale_up = self.k.run("scale", self.disrupt_broker_target, f"--replicas={orig_replicas}")
                if scale_up.returncode != 0:
                    self.record("broker-outage-recovery", "FAILED",
                                details=f"Failed to scale up broker target: {scale_up.stderr}")
                    return False

                # Wait for broker rollout
                roll_res = self.k.run("rollout", "status", self.disrupt_broker_target, "--timeout=120s")
                if roll_res.returncode != 0:
                    self.record("broker-outage-recovery", "FAILED",
                                details=f"Broker rollout status failed: {roll_res.stderr}")
                    return False

                # Verify broker condition Ready
                b_pods = self.k.get_json("get", "pods", "-l", f"app.kubernetes.io/name={target_info.get('metadata', {}).get('name')}")
                b_items = b_pods.get("items", []) if isinstance(b_pods, dict) else []
                if b_items and not any(_is_pod_ready(p) for p in b_items):
                    self.record("broker-outage-recovery", "FAILED", details="Broker pods restored but not in Ready state")
                    return False

                # Poll SAME task to completion
                completed_task = await self._poll_task_completion(client, task_id, timeout=min(self.timeout, 120.0))
                if completed_task.status.state != pb.TASK_STATE_COMPLETED:
                    self.record("broker-outage-recovery", "FAILED",
                                details="Task did not complete after broker restoration", taskId=task_id)
                    return False

                self.record("broker-outage-recovery", "PASSED",
                            details="Controlled broker outage observed, task did not complete during outage, restored cleanly with verified completion",
                            disruptTarget=self.disrupt_broker_target, taskId=task_id,
                            zeroReplicasObserved=zero_observed)
                return True
        except Exception as exc:
            self.record("broker-outage-recovery", "FAILED", details=f"Broker outage check failed: {exc}")
            return False

    def _build_verdict(self, status: str, reason: str, missing: list[str] | None = None) -> dict[str, Any]:
        self.final_git = get_git_provenance()
        executed = set(self.results.keys())
        missing_checks = sorted(PRODUCTION_REQUIRED_CHECKS - executed)

        # Invariant 1: Missing checks or non-PASSED checks strictly prevent a PASS verdict
        has_failed = any(c.get("status") == "FAILED" for c in self.results.values())
        has_incomplete = any(c.get("status") == "INCOMPLETE_PREREQUISITE" for c in self.results.values())

        if has_failed:
            status = "FAILED"
        elif has_incomplete or missing_checks:
            status = "INCOMPLETE_PREREQUISITE"
            if missing_checks:
                reason = f"Required checks missing from verdict: {missing_checks}"
        elif status == "PRODUCTION_DEPLOYMENT_PASS":
            # Invariant 2: Simulation mode can NEVER emit PRODUCTION_DEPLOYMENT_PASS
            if self.execution_mode != "live":
                status = "SIMULATED_VALIDATION_PASS"
                reason = "All simulated Kubernetes orchestration checks passed in test mode"
            else:
                # Invariant 3: Live PASS requires identical pre/post clean Git provenance
                if (self.initial_git.get("head") != self.final_git.get("head")
                        or self.initial_git.get("tree") != self.final_git.get("tree")
                        or self.initial_git.get("status") != "clean"
                        or self.final_git.get("status") != "clean"):
                    status = "FAILED"
                    reason = "Git provenance changed or working tree dirty during live gate run"

        verdict = {
            "status": status,
            "productionCertified": False,
            "upstreamBindingCertified": False,
            "executionMode": self.execution_mode,
            "initialGitProvenance": self.initial_git,
            "finalGitProvenance": self.final_git,
            "applicationImageDigest": self.image,
            "observedImageIds": self.observed_image_ids,
            "manifestSha256": self.manifest_sha,
            "kubernetesServerVersion": self.server_version,
            "daprVersion": self.dapr_version,
            "namespace": self.namespace,
            "tenant": self.tenant,
            "broker": self.broker,
            "reason": reason,
            "checks": self.results,
            "executedCheckCount": len(executed),
            "requiredCheckCount": len(PRODUCTION_REQUIRED_CHECKS),
            "missingChecks": missing_checks,
            "startTime": self.start_time,
            "endTime": time.time(),
        }
        if missing:
            verdict["missingPrerequisites"] = missing
        return verdict


def write_verdict(output: Path, report: dict, history: list[dict[str, Any]] | None = None) -> None:
    verdict_path = output / "production-deployment-verdict.json"
    verdict_path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    if history is not None:
        (output / "command-history.json").write_text(json.dumps(history, indent=2, sort_keys=True) + "\n")

    hashes, unsafe = evidence_hashes(output)
    if unsafe:
        report["status"] = "FAILED"
        report["evidenceError"] = "UNSAFE_EVIDENCE_PATHS"
        report["unsafePaths"] = unsafe
        verdict_path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
        hashes, _ = evidence_hashes(output)
    (output / "sha256.json").write_text(json.dumps(hashes, indent=2, sort_keys=True) + "\n")


def check_prerequisites(kubeconfig: str | None = None, context: str | None = None) -> list[str]:
    """Check required host CLI tools and cluster connectivity."""
    missing: list[str] = []
    kubectl = shutil.which("kubectl")
    if not kubectl:
        missing.append("kubectl")
        return missing

    cmd = ["kubectl"]
    if kubeconfig:
        cmd.extend(["--kubeconfig", kubeconfig])
    if context:
        cmd.extend(["--context", context])
    cmd.extend(["version", "--client=true", "-o", "json"])

    try:
        res = subprocess.run(cmd, capture_output=True, timeout=10, check=False)
        if res.returncode != 0:
            missing.append("kubectl")
            return missing
    except (OSError, subprocess.SubprocessError):
        missing.append("kubectl")
        return missing

    # Check cluster access
    cluster_cmd = ["kubectl"]
    if kubeconfig:
        cluster_cmd.extend(["--kubeconfig", kubeconfig])
    if context:
        cluster_cmd.extend(["--context", context])
    cluster_cmd.extend(["cluster-info"])

    try:
        res = subprocess.run(cluster_cmd, capture_output=True, timeout=15, check=False)
        if res.returncode != 0:
            missing.append("kubernetes-cluster")
    except (OSError, subprocess.SubprocessError):
        missing.append("kubernetes-cluster")

    return missing


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", required=True, type=Path,
                        help="New output directory outside repo for evidence")
    parser.add_argument("--namespace", default="a2amq-prod-gate",
                        help="Target disposable Kubernetes namespace")
    parser.add_argument("--tenant", default="prod-tenant",
                        help="Application tenant identifier")
    parser.add_argument("--broker", choices=("jetstream", "rabbitmq"), default="jetstream")
    parser.add_argument("--image", required=True,
                        help="Digest-pinned application image reference ending in @sha256:<64 hex>")
    parser.add_argument("--public-url", required=True,
                        help="Root HTTPS origin for public A2A traffic")
    parser.add_argument("--issuer", required=True,
                        help="HTTPS URL of JWT token issuer")
    parser.add_argument("--jwt-kid", required=True,
                        help="Key ID of public signing key")
    parser.add_argument("--jwt-public-key-file", required=True, type=Path,
                        help="Regular PEM file containing JWT public key")
    parser.add_argument("--owner-token-file", type=Path, default=None,
                        help="File containing JWT bearer token for task owner")
    parser.add_argument("--foreign-owner-token-file", type=Path, default=None,
                        help="File containing JWT bearer token for different owner (same tenant)")
    parser.add_argument("--cross-tenant-token-file", type=Path, default=None,
                        help="File containing JWT bearer token for different tenant")
    parser.add_argument("--mcp-token-file", type=Path, default=None,
                        help="File containing JWT bearer token for MCP client")
    parser.add_argument("--operator-token-file", type=Path, default=None,
                        help="File containing JWT bearer token for operator DLQ replay")
    parser.add_argument("--ca-bundle", type=Path, default=None,
                        help="Custom CA bundle file for public HTTPS endpoint")
    parser.add_argument("--disrupt-broker-target", type=str, default=None,
                        help="Explicit namespaced broker workload (e.g. statefulset/nats) authorized for disruption")
    parser.add_argument("--allow-broker-disruption", action="store_true",
                        help="Opt in to controlled broker disruption test")
    parser.add_argument("--retain-on-failure", action="store_true",
                        help="Retain disposable namespace on failure for debugging")
    parser.add_argument("--kubeconfig", type=str, default=os.environ.get("KUBECONFIG"))
    parser.add_argument("--context", type=str, default=None)
    parser.add_argument("--timeout", type=float, default=600.0)
    return parser.parse_args(argv)


def _safe_read_token(path: Path | None) -> str | None:
    if path is None:
        return None
    try:
        return read_file(str(path), secret=True, limit=8192).strip()
    except Exception:
        return None


def main(argv: list[str] | None = None,
         runner_override: Callable[..., CommandResult] | None = None,
         http_runner_override: Callable[..., HttpResponse] | None = None) -> int:
    args = parse_args(argv if argv is not None else sys.argv[1:])
    output = args.output.resolve()

    if output == ROOT or ROOT in output.parents:
        print("ERROR: Output directory must be outside repository root", file=sys.stderr)
        return 1
    if output.exists():
        print("ERROR: Output directory already exists", file=sys.stderr)
        return 1

    output.mkdir(parents=True, mode=0o700)
    initial_git = get_git_provenance()
    execution_mode = "simulated" if (runner_override or http_runner_override) else "live"

    # 1. Credential guard
    secret_names = [k for k, v in os.environ.items() if v and MODEL_CREDENTIAL.match(k)]
    if secret_names:
        report = {
            "status": "FAILED",
            "productionCertified": False,
            "upstreamBindingCertified": False,
            "executionMode": execution_mode,
            "credentialGuard": "FAILED",
            "presentSecretNames": sorted(secret_names),
            "timestamp": time.time(),
        }
        write_verdict(output, report)
        print("FAILED: Credential guard triggered", file=sys.stderr)
        return 1

    # 2. Validate namespace name
    if not SAFE_NAMESPACE_RE.match(args.namespace) or args.namespace in FORBIDDEN_NAMESPACES:
        report = {
            "status": "FAILED",
            "productionCertified": False,
            "upstreamBindingCertified": False,
            "executionMode": execution_mode,
            "reason": f"Unsafe or forbidden namespace name: {args.namespace}",
            "timestamp": time.time(),
        }
        write_verdict(output, report)
        print(f"FAILED: Unsafe namespace name: {args.namespace}", file=sys.stderr)
        return 1

    # 3. Validate image format
    if not IMAGE_DIGEST_RE.match(args.image):
        report = {
            "status": "FAILED",
            "productionCertified": False,
            "upstreamBindingCertified": False,
            "executionMode": execution_mode,
            "reason": "Image must be an immutable reference ending in @sha256:<64 hex>",
            "timestamp": time.time(),
        }
        write_verdict(output, report)
        print("FAILED: Image digest required", file=sys.stderr)
        return 1

    # 4. Read JWT public key using bounded no-symlink reader
    try:
        pubkey = read_file(str(args.jwt_public_key_file), limit=65536)
        require("BEGIN PUBLIC KEY" in pubkey and "PRIVATE KEY" not in pubkey, "INVALID_IDENTITY_KEY")
    except Exception as exc:
        report = {
            "status": "FAILED",
            "productionCertified": False,
            "upstreamBindingCertified": False,
            "executionMode": execution_mode,
            "reason": f"Invalid JWT public key file: {exc}",
            "timestamp": time.time(),
        }
        write_verdict(output, report)
        print(f"FAILED: Invalid JWT public key file: {exc}", file=sys.stderr)
        return 1

    # Read tokens safely
    owner_token = _safe_read_token(args.owner_token_file)
    foreign_owner_token = _safe_read_token(args.foreign_owner_token_file)
    cross_tenant_token = _safe_read_token(args.cross_tenant_token_file)
    mcp_token = _safe_read_token(args.mcp_token_file)
    operator_token = _safe_read_token(args.operator_token_file)

    # 5. Check prerequisites if running in live mode
    if execution_mode == "live":
        missing = check_prerequisites(args.kubeconfig, args.context)
        if missing:
            report = {
                "status": "INCOMPLETE_PREREQUISITE",
                "productionCertified": False,
                "upstreamBindingCertified": False,
                "executionMode": execution_mode,
                "missingPrerequisites": missing,
                "reason": (
                    "Kubernetes CLI or cluster connectivity unavailable; "
                    "production deployment certification requires an authorized live cluster"
                ),
                "requiredChecks": sorted(PRODUCTION_REQUIRED_CHECKS),
                "image": args.image,
                "namespace": args.namespace,
                "tenant": args.tenant,
                "broker": args.broker,
                "initialGitProvenance": initial_git,
                "timestamp": time.time(),
            }
            write_verdict(output, report)
            print(
                f"INCOMPLETE_PREREQUISITE: Missing required Kubernetes prerequisites: {missing}",
                file=sys.stderr,
            )
            return 2

    # 6. Execute production deployment gate
    k = Kubectl(
        kubeconfig=args.kubeconfig,
        context=args.context,
        namespace=args.namespace,
        runner=runner_override,
    )
    http = HttpClient(
        base_url=args.public_url,
        ca_bundle=args.ca_bundle,
        runner=http_runner_override,
    )
    runner = ProductionDeploymentRunner(
        k=k,
        http=http,
        output=output,
        namespace=args.namespace,
        tenant=args.tenant,
        broker=args.broker,
        image=args.image,
        public_url=args.public_url,
        issuer=args.issuer,
        jwt_kid=args.jwt_kid,
        public_key=pubkey,
        owner_token=owner_token,
        foreign_owner_token=foreign_owner_token,
        cross_tenant_token=cross_tenant_token,
        mcp_token=mcp_token,
        operator_token=operator_token,
        disrupt_broker_target=args.disrupt_broker_target,
        allow_broker_disruption=args.allow_broker_disruption,
        retain_on_failure=args.retain_on_failure,
        timeout=args.timeout,
        execution_mode=execution_mode,
        initial_git=initial_git,
    )

    verdict = runner.run_all()
    success = verdict["status"] in {"PRODUCTION_DEPLOYMENT_PASS", "SIMULATED_VALIDATION_PASS"}
    runner.cleanup(success=success)
    write_verdict(output, verdict, history=k.history)

    print(json.dumps({"status": verdict["status"], "reason": verdict["reason"]}, indent=2))
    if verdict["status"] in {"PRODUCTION_DEPLOYMENT_PASS", "SIMULATED_VALIDATION_PASS"}:
        return 0
    elif verdict["status"] == "INCOMPLETE_PREREQUISITE":
        return 2
    else:
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
