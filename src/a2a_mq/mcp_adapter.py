"""Official MCP v2 adapter over the same authorized durable TaskAPI.

The HTTP transport is authenticated by the MCP SDK. Stdio is intentionally not
constructed here because it has no request bearer identity and must be launched by
a separately trusted supervisor.
"""
from __future__ import annotations

from typing import Any
from urllib.parse import urlsplit

from .domain import MAX_BYTES, PermanentError, RetryableError, require
from .identity import JWTVerifier, Principal
from .task_api import TaskAPI


def create_mcp(api: TaskAPI, verifier: JWTVerifier, *, resource_url: str,
               name: str = "A2A-MQ durable tasks"):
    """Return a configured official MCPServer; caller chooses the HTTP mount."""
    parsed = urlsplit(resource_url)
    require(isinstance(resource_url, str) and parsed.scheme == "https" and bool(parsed.hostname)
            and parsed.path == "/mcp" and not parsed.username and not parsed.password
            and not parsed.query and not parsed.fragment
            and "?" not in resource_url and "#" not in resource_url
            and 8 < len(resource_url) <= 2048, "MCP_HTTPS_RESOURCE_REQUIRED")
    require(verifier.audience == resource_url and urlsplit(verifier.issuer).scheme == "https",
            "MCP_AUDIENCE_MISMATCH")
    try:
        from pydantic import AnyHttpUrl
        from mcp.server.auth.middleware.auth_context import get_access_token
        from mcp.server.auth.provider import AccessToken, TokenVerifier
        from mcp.server.auth.settings import AuthSettings
        from mcp.server.mcpserver import MCPServer
    except ImportError:
        raise PermanentError("MCP_EXTRA_REQUIRED") from None

    class ExistingJWTVerifier(TokenVerifier):
        async def verify_token(self, encoded: str):
            try:
                principal = verifier.verify(encoded)
            except PermanentError:
                return None
            return AccessToken(
                token=encoded,
                client_id=principal.subject,
                subject=principal.subject,
                scopes=sorted(principal.scopes),
                resource=resource_url,
                claims={"tenant": principal.tenant, "issuer": verifier.issuer},
            )

    server = MCPServer(
        name,
        token_verifier=ExistingJWTVerifier(),
        auth=AuthSettings(
            issuer_url=AnyHttpUrl(verifier.issuer),
            resource_server_url=AnyHttpUrl(resource_url),
            required_scopes=[],
            # JWTVerifier already enforces the exact resource URL as aud.
            validate_token_resource=False,
        ),
    )

    def principal() -> Principal:
        access = get_access_token()
        require(access is not None and access.subject and isinstance(access.claims, dict), "UNAUTHENTICATED")
        tenant = access.claims.get("tenant")
        require(isinstance(tenant, str), "UNAUTHENTICATED")
        return Principal(access.subject, tenant, frozenset(access.scopes))

    async def result(awaitable) -> dict[str, Any]:
        try:
            return {"ok": True, "result": await awaitable}
        except RetryableError as error:
            return {"ok": False, "error": error.code, "retryable": True}
        except PermanentError as error:
            return {"ok": False, "error": error.code, "retryable": False}

    @server.tool(description="Get one durable A2A task visible to the authenticated principal.")
    async def get_task(task_id: str, history_length: int | None = None) -> dict[str, Any]:
        return await result(api.get(principal(), task_id, history_length=history_length))

    @server.tool(description="List durable A2A tasks with bounded owner-scoped keyset pagination.")
    async def list_tasks(page_size: int = 50, page_token: str = "", context_id: str | None = None,
                         status: str | None = None, history_length: int | None = None,
                         include_artifacts: bool = False) -> dict[str, Any]:
        return await result(api.list(principal(), page_size=page_size, page_token=page_token,
                                     context_id=context_id, status=status,
                                     history_length=history_length,
                                     include_artifacts=include_artifacts))

    @server.tool(description="Submit a bounded text task. message_id is the caller's durable idempotency key.")
    async def send_text(text: str, message_id: str, context_id: str | None = None,
                        transform: str = "echo", delay_seconds: float = 0,
                        input_required: bool = False, approval_required: bool = False,
                        return_immediately: bool = False) -> dict[str, Any]:
        require(isinstance(text, str) and len(text.encode("utf-8")) <= 16384, "WORKFLOW_INPUT_TOO_LARGE")
        require(isinstance(message_id, str) and 0 < len(message_id) <= 256, "INVALID_MESSAGE_ID")
        message: dict[str, Any] = {"messageId": message_id, "role": "ROLE_USER", "parts": [{"text": text}]}
        if context_id is not None:
            message["contextId"] = context_id
        request = {
            "message": message,
            "configuration": {"returnImmediately": return_immediately},
            "metadata": {"a2amq": {"transform": transform, "delaySeconds": delay_seconds,
                                      "inputRequired": input_required,
                                      "authRequired": approval_required}},
        }
        return await result(api.send(principal(), request))

    @server.tool(description="Continue a task that is waiting for user input.")
    async def provide_input(task_id: str, context_id: str, message_id: str, text: str,
                            return_immediately: bool = False) -> dict[str, Any]:
        request = {"message": {"messageId": message_id, "role": "ROLE_USER", "taskId": task_id,
                               "contextId": context_id, "parts": [{"text": text}]},
                   "configuration": {"returnImmediately": return_immediately}}
        return await result(api.send(principal(), request))

    @server.tool(description="Request owner-coordinated cancellation of a durable task.")
    async def cancel_task(task_id: str) -> dict[str, Any]:
        return await result(api.cancel(principal(), task_id))

    @server.tool(description="Resolve an AUTH_REQUIRED task using its advertised request id.")
    async def approve_task(task_id: str, request_id: str, approved: bool) -> dict[str, Any]:
        require(type(approved) is bool, "INVALID_APPROVAL")
        return await result(api.approve(principal(), task_id, request_id, approved))

    return server


def streamable_http_app(api: TaskAPI, verifier: JWTVerifier, *, resource_url: str,
                        name: str = "A2A-MQ durable tasks", path: str = "/mcp"):
    """Create the authenticated MCP Streamable HTTP ASGI application."""
    require(isinstance(path, str) and path.startswith("/") and "//" not in path
            and "?" not in path and "#" not in path, "INVALID_MCP_PATH")
    server = create_mcp(api, verifier, resource_url=resource_url, name=name)
    try:
        from mcp.server.transport_security import TransportSecuritySettings
    except ImportError:
        raise PermanentError("MCP_EXTRA_REQUIRED") from None
    parsed = urlsplit(resource_url)
    origin = f"{parsed.scheme}://{parsed.netloc}"
    security = TransportSecuritySettings(
        enable_dns_rebinding_protection=True,
        allowed_hosts=[parsed.netloc],
        allowed_origins=[origin],
    )
    app = server.streamable_http_app(streamable_http_path=path,
                                     transport_security=security,
                                     max_request_body_size=MAX_BYTES,
                                     session_idle_timeout=600, max_sessions=512)
    app.state.a2amq_mcp_server = server
    return app
