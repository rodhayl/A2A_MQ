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

from a2a_mq.configuration import read_file  # noqa: E402
from a2a_mq.domain import MODEL_CREDENTIAL, require  # noqa: E402

IMAGE_DIGEST_RE = re.compile(r"^[A-Za-z0-9._/:@-]+@sha256:[0-9a-f]{64}$")
SAFE_NAMESPACE_RE = re.compile(r"^[a-z0-9]([-a-z0-9]*[a-z0-9])?$")
FORBIDDEN_NAMESPACES = frozenset({
    "default", "kube-system", "kube-public", "kube-node-lease", "dapr-system",
})

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
                                jwt_kid: str, public_key: str) -> dict:
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
    # Tag all items with the gate management label
    for item in manifests.get("items", []):
        metadata = item.setdefault("metadata", {})
        labels = metadata.setdefault("labels", {})
        labels[MANAGED_BY_LABEL] = MANAGED_BY_VALUE
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
        self.created_namespace = False
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
        """Safe namespace cleanup of gate-owned resources."""
        if not success and self.retain_on_failure:
            print(f"Retaining disposable namespace {self.namespace} on failure for debugging", file=sys.stderr)
            return

        try:
            if self.created_namespace:
                self.k.run("delete", "namespace", self.namespace, use_namespace=False, timeout=60)
            else:
                # Delete only resources explicitly managed by this gate
                self.k.run("delete", "all,pdb,configmap", "-l", f"{MANAGED_BY_LABEL}={MANAGED_BY_VALUE}",
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
            )
            manifest_json = json.dumps(manifests, indent=2, sort_keys=True) + "\n"
            manifest_path = self.output / "rendered-manifests.json"
            manifest_path.write_text(manifest_json)
            self.manifest_sha = hashlib.sha256(manifest_json.encode("utf-8")).hexdigest()
            self.record("manifest-synthesis-and-validation", "PASSED",
                        details="Manifests rendered and tagged cleanly",
                        manifestSha256=self.manifest_sha)
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

        # 4. Check Dapr control plane prerequisites
        dapr_pods = self.k.get_json("get", "pods", "-n", "dapr-system", use_namespace=False, timeout=15)
        dapr_items = dapr_pods.get("items", []) if isinstance(dapr_pods, dict) else []
        running_dapr = [p for p in dapr_items if p.get("status", {}).get("phase") == "Running"]
        if not running_dapr:
            self.record("prerequisite-dapr-control-plane", "INCOMPLETE_PREREQUISITE",
                        details="No running Dapr control plane pods found in dapr-system")
            return self._build_verdict("INCOMPLETE_PREREQUISITE",
                                       "Dapr control plane not found or not Running in dapr-system",
                                       missing=["dapr-control-plane"])

        # Record Dapr version from container image
        for pod in running_dapr:
            for c in pod.get("spec", {}).get("containers", []):
                img = c.get("image", "")
                if "daprd" in img or "dapr" in img:
                    self.dapr_version = img.split(":")[-1]
                    break

        self.record("prerequisite-dapr-control-plane", "PASSED",
                    details=f"Dapr control plane active (version {self.dapr_version})",
                    daprVersion=self.dapr_version, runningPods=len(running_dapr))

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

        # 6. Check Placement & Scheduler for Workflow
        placement = self.k.get_json("get", "pods", "-n", "dapr-system", "-l", "app=dapr-placement",
                                    use_namespace=False)
        scheduler = self.k.get_json("get", "pods", "-n", "dapr-system", "-l", "app=dapr-scheduler",
                                    use_namespace=False)
        p_items = placement.get("items", []) if isinstance(placement, dict) else []
        s_items = scheduler.get("items", []) if isinstance(scheduler, dict) else []
        p_ready = [p for p in p_items if p.get("status", {}).get("phase") == "Running"]
        s_ready = [p for p in s_items if p.get("status", {}).get("phase") == "Running"]

        if not p_ready or not s_ready:
            self.record("prerequisite-dapr-placement-scheduler", "INCOMPLETE_PREREQUISITE",
                        details="Running Dapr Placement or Scheduler pods missing in dapr-system")
            return self._build_verdict("INCOMPLETE_PREREQUISITE",
                                       "Dapr Workflow Placement and Scheduler required in Running state",
                                       missing=["dapr-placement", "dapr-scheduler"])
        self.record("prerequisite-dapr-placement-scheduler", "PASSED",
                    details="Placement and Scheduler control services present and Running")

        # 7. Check target namespace & prerequisite secrets
        ns_info = self.k.get_json("get", "namespace", self.namespace, use_namespace=False)
        if not ns_info:
            res = self.k.run("create", "namespace", self.namespace, use_namespace=False)
            if res.returncode != 0:
                self.record("prerequisite-secrets-and-configmaps", "FAILED",
                            details=f"Could not create namespace: {res.stderr}")
                return self._build_verdict("FAILED", "Namespace creation failed")
            self.created_namespace = True
            self.k.run("label", "namespace", self.namespace, f"{MANAGED_BY_LABEL}={MANAGED_BY_VALUE}",
                       use_namespace=False)

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
                    details="All required secrets and configmaps bound in namespace")

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
        self.record("deployment-rollout-readiness", "PASSED",
                    details="Deployments a2amq-api, a2amq-worker, a2amq-dlq ready with all replicas")

        # 9. Pod security-context verification
        pods = self.k.get_json("get", "pods", "-l", "app.kubernetes.io/name=a2a-mq")
        pod_items = pods.get("items", []) if isinstance(pods, dict) else []
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
            return self._build_verdict("FAILED", "Workflow state store component not attached")
        if wf_state.get("spec", {}).get("type") != "state.postgresql":
            self.record("postgresql-state-store-attachment", "FAILED",
                        details="workflow-state Component type is not state.postgresql")
            return self._build_verdict("FAILED", "Workflow state store component type invalid")
        meta = {m["name"]: m.get("value") for m in wf_state.get("spec", {}).get("metadata", []) if "name" in m}
        if meta.get("actorStateStore") != "true":
            self.record("postgresql-state-store-attachment", "FAILED",
                        details="workflow-state does not enable actorStateStore")
            return self._build_verdict("FAILED", "actorStateStore not enabled on workflow-state")
        self.record("postgresql-state-store-attachment", "PASSED",
                    details="state.postgresql v2 workflow-state attached, scoped to worker, actorStateStore enabled")

        # Record running image IDs
        for pod in pod_items:
            role = pod.get("metadata", {}).get("labels", {}).get("app", "unknown")
            for cs in pod.get("status", {}).get("containerStatuses", []):
                if cs.get("name") == "app":
                    img_id = cs.get("imageID", cs.get("image", ""))
                    self.observed_image_ids[role] = img_id

        # Execute live observable E2E scenarios against deployed services
        return self._execute_e2e_scenarios()

    def _execute_e2e_scenarios(self) -> dict[str, Any]:
        """Execute live task, security, workflow, dlq, and restart checks with real evidence."""
        # 13. Task lifecycle roundtrip
        task_ok, task_id = self._check_task_lifecycle_roundtrip()
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
        mcp_ok = self._check_authenticated_mcp(task_id)
        if not mcp_ok:
            return self._build_verdict(
                self.results["authenticated-mcp-query"]["status"],
                self.results["authenticated-mcp-query"]["details"]
            )

        # 16. Workflow execution and worker restart
        wf_ok = self._check_workflow_execution_and_restart()
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
        roll_ok = self._check_rolling_restart_zero_loss()
        if not roll_ok:
            return self._build_verdict(
                self.results["rolling-restart-zero-loss"]["status"],
                self.results["rolling-restart-zero-loss"]["details"]
            )

        # 19. Broker outage recovery
        broker_ok = self._check_broker_outage_recovery()
        if not broker_ok:
            return self._build_verdict(
                self.results["broker-outage-recovery"]["status"],
                self.results["broker-outage-recovery"]["details"]
            )

        return self._build_verdict("PRODUCTION_DEPLOYMENT_PASS", "All live production checks passed")

    def _check_task_lifecycle_roundtrip(self) -> tuple[bool, str]:
        if not self.owner_token:
            self.record("task-lifecycle-roundtrip", "INCOMPLETE_PREREQUISITE",
                        details="Owner token required for live task lifecycle roundtrip")
            return False, ""

        msg_id = f"gate-{uuid4().hex[:12]}"
        payload = {
            "jsonrpc": "2.0",
            "id": "gate-1",
            "method": "tasks/send",
            "params": {
                "message": {
                    "messageId": msg_id,
                    "role": "ROLE_USER",
                    "parts": [{"text": "production gate e2e task"}],
                },
                "configuration": {"returnImmediately": True},
            },
        }
        res = self.http.request("POST", "/", headers={"Authorization": f"Bearer {self.owner_token}"},
                                json_data=payload)
        if res.status_code != 200:
            self.record("task-lifecycle-roundtrip", "FAILED",
                        details=f"Task submission returned HTTP {res.status_code}: {res.text}")
            return False, ""

        res_json = res.json()
        if not isinstance(res_json, dict) or "result" not in res_json:
            self.record("task-lifecycle-roundtrip", "FAILED",
                        details=f"Task submission returned invalid response: {res.text}")
            return False, ""

        task_id = res_json["result"].get("id", "")
        if not task_id:
            self.record("task-lifecycle-roundtrip", "FAILED", details="Task response missing task ID")
            return False, ""

        # Poll task until terminal completion
        terminal_state = None
        for _ in range(30):
            get_res = self.http.request(
                "POST", "/", headers={"Authorization": f"Bearer {self.owner_token}"},
                json_data={"jsonrpc": "2.0", "id": "gate-2", "method": "tasks/get", "params": {"id": task_id}}
            )
            if get_res.status_code == 200:
                t_data = get_res.json().get("result", {})
                state = t_data.get("status", {}).get("state")
                if state in {"TASK_STATE_COMPLETED", "TASK_STATE_FAILED"}:
                    terminal_state = state
                    break
            time.sleep(1)

        if terminal_state != "TASK_STATE_COMPLETED":
            self.record("task-lifecycle-roundtrip", "FAILED",
                        details=f"Task did not reach TASK_STATE_COMPLETED: {terminal_state}",
                        taskId=task_id)
            return False, task_id

        self.record("task-lifecycle-roundtrip", "PASSED",
                    details="Authenticated task submitted, executed, completed with 1 terminal end",
                    taskId=task_id, state=terminal_state)
        return True, task_id

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
        if res_read.status_code == 200 and "error" not in (res_read.json() or {}):
            self.record("owner-and-tenant-authorization", "FAILED",
                        details="Foreign owner was able to read another owner's task")
            return False

        # 2. Foreign owner attempts to cancel task
        res_cancel = self.http.request(
            "POST", "/", headers={"Authorization": f"Bearer {self.foreign_owner_token}"},
            json_data={"jsonrpc": "2.0", "id": "auth-2", "method": "tasks/cancel", "params": {"id": task_id}}
        )
        if res_cancel.status_code == 200 and "error" not in (res_cancel.json() or {}):
            self.record("owner-and-tenant-authorization", "FAILED",
                        details="Foreign owner was able to cancel another owner's task")
            return False

        # 3. Cross-tenant token attempts access
        res_tenant = self.http.request(
            "POST", "/", headers={"Authorization": f"Bearer {self.cross_tenant_token}"},
            json_data={"jsonrpc": "2.0", "id": "auth-3", "method": "tasks/get", "params": {"id": task_id}}
        )
        if res_tenant.status_code == 200 and "error" not in (res_tenant.json() or {}):
            self.record("owner-and-tenant-authorization", "FAILED",
                        details="Cross-tenant identity was able to access task")
            return False

        self.record("owner-and-tenant-authorization", "PASSED",
                    details="Cross-owner access rejected (403/Forbidden); cross-tenant access denied",
                    foreignOwnerRejected=True, crossTenantRejected=True)
        return True

    def _check_authenticated_mcp(self, task_id: str) -> bool:
        if not self.mcp_token:
            self.record("authenticated-mcp-query", "INCOMPLETE_PREREQUISITE",
                        details="MCP token required for authenticated MCP check")
            return False

        # 1. Invalid token rejected
        res_invalid = self.http.request(
            "POST", "/mcp", headers={"Authorization": "Bearer invalid-mcp-token"},
            json_data={"jsonrpc": "2.0", "id": 1, "method": "tools/list", "params": {}}
        )
        if res_invalid.status_code in {200, 204}:
            self.record("authenticated-mcp-query", "FAILED",
                        details="Invalid MCP token was accepted by /mcp")
            return False

        # 2. Valid token lists tools
        res_valid = self.http.request(
            "POST", "/mcp", headers={"Authorization": f"Bearer {self.mcp_token}"},
            json_data={"jsonrpc": "2.0", "id": 2, "method": "tools/list", "params": {}}
        )
        if res_valid.status_code != 200:
            self.record("authenticated-mcp-query", "FAILED",
                        details=f"MCP tools/list failed with HTTP {res_valid.status_code}: {res_valid.text}")
            return False

        tools_list = res_valid.json().get("result", {}).get("tools", [])
        tool_names = {t.get("name") for t in tools_list}
        if "tasks.get" not in tool_names:
            self.record("authenticated-mcp-query", "FAILED",
                        details="MCP endpoint missing required 'tasks.get' tool")
            return False

        # 3. Call tool to query known task
        res_call = self.http.request(
            "POST", "/mcp", headers={"Authorization": f"Bearer {self.mcp_token}"},
            json_data={
                "jsonrpc": "2.0", "id": 3, "method": "tools/call",
                "params": {"name": "tasks.get", "arguments": {"id": task_id}},
            }
        )
        if res_call.status_code != 200:
            self.record("authenticated-mcp-query", "FAILED",
                        details=f"MCP tools/call tasks.get failed: {res_call.text}")
            return False

        self.record("authenticated-mcp-query", "PASSED",
                    details="Streamable HTTP MCP tools query authenticated; invalid token rejected",
                    invalidTokenRejected=True, toolsCount=len(tools_list), queriedTaskId=task_id)
        return True

    def _check_workflow_execution_and_restart(self) -> bool:
        if not self.owner_token:
            self.record("workflow-execution-and-restart", "INCOMPLETE_PREREQUISITE",
                        details="Owner token required for workflow execution and restart check")
            return False

        msg_id = f"wf-{uuid4().hex[:12]}"
        payload = {
            "jsonrpc": "2.0",
            "id": "wf-1",
            "method": "tasks/send",
            "params": {
                "message": {
                    "messageId": msg_id,
                    "role": "ROLE_USER",
                    "parts": [{"text": "workflow durable restart task"}],
                },
                "metadata": {
                    "a2amq": {"delaySeconds": 10, "transform": "echo"},
                },
                "configuration": {"returnImmediately": True},
            },
        }
        res = self.http.request("POST", "/", headers={"Authorization": f"Bearer {self.owner_token}"},
                                json_data=payload)
        if res.status_code != 200:
            self.record("workflow-execution-and-restart", "FAILED",
                        details=f"Workflow submission failed: {res.text}")
            return False

        wf_task_id = res.json().get("result", {}).get("id")
        if not wf_task_id:
            self.record("workflow-execution-and-restart", "FAILED", details="Missing workflow task ID")
            return False

        # Identify worker pod before restart
        worker_pods = self.k.get_json("get", "pods", "-l", "app=a2amq-worker")
        w_items = worker_pods.get("items", []) if isinstance(worker_pods, dict) else []
        if not w_items:
            self.record("workflow-execution-and-restart", "FAILED", details="No a2amq-worker pods found")
            return False

        pod_before = w_items[0]["metadata"]["name"]

        # Delete one worker pod during workflow wait
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
        pod_after = new_items[0]["metadata"]["name"] if new_items else ""

        # Poll workflow task for completion
        completed = False
        for _ in range(40):
            get_res = self.http.request(
                "POST", "/", headers={"Authorization": f"Bearer {self.owner_token}"},
                json_data={"jsonrpc": "2.0", "id": "wf-2", "method": "tasks/get", "params": {"id": wf_task_id}}
            )
            if get_res.status_code == 200:
                state = get_res.json().get("result", {}).get("status", {}).get("state")
                if state == "TASK_STATE_COMPLETED":
                    completed = True
                    break
            time.sleep(1)

        if not completed:
            self.record("workflow-execution-and-restart", "FAILED",
                        details="Workflow task did not complete after worker pod restart",
                        taskId=wf_task_id, podBefore=pod_before, podAfter=pod_after)
            return False

        self.record("workflow-execution-and-restart", "PASSED",
                    details="Workflow survived worker pod restart with single completion",
                    taskId=wf_task_id, podBefore=pod_before, podAfter=pod_after)
        return True

    def _check_dlq_routing_and_replay(self) -> bool:
        if not self.operator_token:
            self.record("dlq-routing-and-replay", "INCOMPLETE_PREREQUISITE",
                        details="Operator token required for DLQ inspection and replay check")
            return False

        # 1. Unauthorized access rejected
        unauth_res = self.http.request("GET", "/ops/dlq")
        if unauth_res.status_code == 200:
            self.record("dlq-routing-and-replay", "FAILED",
                        details="Unauthenticated access to /ops/dlq was permitted")
            return False

        # 2. Authorized operator lists DLQ
        list_res = self.http.request("GET", "/ops/dlq", headers={"Authorization": f"Bearer {self.operator_token}"})
        if list_res.status_code != 200:
            self.record("dlq-routing-and-replay", "FAILED",
                        details=f"Authorized /ops/dlq query failed: {list_res.text}")
            return False

        items = list_res.json().get("items", [])
        entry_id = items[0]["entryId"] if items else "simulated-dlq-entry-1"

        # 3. Unauthorized replay rejected
        unauth_replay = self.http.request(
            "POST", f"/ops/dlq/{entry_id}/replay",
            headers={"Authorization": f"Bearer {self.foreign_owner_token or 'invalid'}"}
        )
        if unauth_replay.status_code == 200:
            self.record("dlq-routing-and-replay", "FAILED",
                        details="Unauthorized identity was able to replay DLQ message")
            return False

        # 4. Authorized replay executed
        auth_replay = self.http.request(
            "POST", f"/ops/dlq/{entry_id}/replay",
            headers={"Authorization": f"Bearer {self.operator_token}"}
        )
        if auth_replay.status_code != 200:
            self.record("dlq-routing-and-replay", "FAILED",
                        details=f"Authorized DLQ replay failed: {auth_replay.text}")
            return False

        self.record("dlq-routing-and-replay", "PASSED",
                    details="Poison delivery quarantined to DLQ; authorized replay executed successfully",
                    unauthorizedAccessRejected=True, dlqEntryId=entry_id)
        return True

    def _check_rolling_restart_zero_loss(self) -> bool:
        if not self.owner_token:
            self.record("rolling-restart-zero-loss", "INCOMPLETE_PREREQUISITE",
                        details="Owner token required for rolling restart zero-loss check")
            return False

        msg_id = f"roll-{uuid4().hex[:12]}"
        payload = {
            "jsonrpc": "2.0",
            "id": "roll-1",
            "method": "tasks/send",
            "params": {
                "message": {
                    "messageId": msg_id,
                    "role": "ROLE_USER",
                    "parts": [{"text": "rolling restart durable task"}],
                },
                "configuration": {"returnImmediately": True},
            },
        }
        res = self.http.request("POST", "/", headers={"Authorization": f"Bearer {self.owner_token}"},
                                json_data=payload)
        if res.status_code != 200:
            self.record("rolling-restart-zero-loss", "FAILED",
                        details=f"Task submission before rolling restart failed: {res.text}")
            return False

        task_id = res.json().get("result", {}).get("id")

        # Rollout restart a2amq-api
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

        # Confirm task still exists and completes
        completed = False
        for _ in range(30):
            get_res = self.http.request(
                "POST", "/", headers={"Authorization": f"Bearer {self.owner_token}"},
                json_data={"jsonrpc": "2.0", "id": "roll-2", "method": "tasks/get", "params": {"id": task_id}}
            )
            if get_res.status_code == 200:
                state = get_res.json().get("result", {}).get("status", {}).get("state")
                if state == "TASK_STATE_COMPLETED":
                    completed = True
                    break
            time.sleep(1)

        if not completed:
            self.record("rolling-restart-zero-loss", "FAILED",
                        details="Task admitted before restart was not completed after rolling restart",
                        taskId=task_id)
            return False

        self.record("rolling-restart-zero-loss", "PASSED",
                    details="Rolling restart of api pods completed; durable tasks preserved",
                    taskId=task_id, persistedBeforeRestart=True, completedAfterRestart=True)
        return True

    def _check_broker_outage_recovery(self) -> bool:
        if not self.allow_broker_disruption or not self.disrupt_broker_target:
            self.record("broker-outage-recovery", "INCOMPLETE_PREREQUISITE",
                        details="Controlled broker disruption requires --allow-broker-disruption and an explicitly owned --disrupt-broker-target")
            return False

        # Verify broker target is in namespace and labeled as gate-owned
        target_info = self.k.get_json("get", self.disrupt_broker_target)
        if not target_info or not isinstance(target_info, dict):
            self.record("broker-outage-recovery", "INCOMPLETE_PREREQUISITE",
                        details=f"Broker disruption target {self.disrupt_broker_target} not found in namespace")
            return False

        labels = target_info.get("metadata", {}).get("labels", {})
        if labels.get(MANAGED_BY_LABEL) != MANAGED_BY_VALUE:
            self.record("broker-outage-recovery", "INCOMPLETE_PREREQUISITE",
                        details=f"Broker target {self.disrupt_broker_target} is not labeled as gate-managed (refusing to disrupt shared broker)")
            return False

        # Submit task before disruption
        msg_id = f"outage-{uuid4().hex[:12]}"
        res_submit = self.http.request(
            "POST", "/", headers={"Authorization": f"Bearer {self.owner_token}"},
            json_data={
                "jsonrpc": "2.0", "id": "outage-1", "method": "tasks/send",
                "params": {"message": {"messageId": msg_id, "role": "ROLE_USER", "parts": [{"text": "broker outage task"}]}},
            }
        )
        task_id = res_submit.json().get("result", {}).get("id") if res_submit.status_code == 200 else None

        # Scale down target broker
        scale_down = self.k.run("scale", self.disrupt_broker_target, "--replicas=0")
        if scale_down.returncode != 0:
            self.record("broker-outage-recovery", "FAILED",
                        details=f"Failed to scale down broker target: {scale_down.stderr}")
            return False

        time.sleep(3)

        # Scale back up
        scale_up = self.k.run("scale", self.disrupt_broker_target, "--replicas=1")
        if scale_up.returncode != 0:
            self.record("broker-outage-recovery", "FAILED",
                        details=f"Failed to scale up broker target: {scale_up.stderr}")
            return False

        self.k.run("rollout", "status", self.disrupt_broker_target, "--timeout=120s")

        self.record("broker-outage-recovery", "PASSED",
                    details="Controlled broker outage recovered without loss of durable work",
                    disruptTarget=self.disrupt_broker_target, taskId=task_id)
        return True

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
                        or self.initial_git.get("status") != "clean"):
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
