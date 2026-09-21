"""Official MCP v2 tools over the same authorized durable TaskAPI as A2A.

Each call re-verifies the current SDK access token. No principal is retained on
an MCP session, no tenant argument grants authority, and no model is invoked.
"""
from __future__ import annotations

from functools import wraps
from urllib.parse import urlsplit

from .domain import PermanentError, RetryableError, require
from .http_boundary import BoundedJSON
from .storage import _key


class MCPToolFailure(ValueError):
    """Safe tool error: MCP serializes it with is_error=True, never as success."""


def safe_tool(function):
    @wraps(function)
    async def wrapped(*args, **kwargs):
        try:
            return await function(*args, **kwargs)
        except RetryableError as error:
            raise MCPToolFailure("RETRYABLE:" + error.code) from None
        except PermanentError as error:
            raise MCPToolFailure(error.code) from None
        except MCPToolFailure:
            raise
        except Exception:
            raise MCPToolFailure("INTERNAL_ERROR") from None
    return wrapped


class MCPTools:
    def __init__(self, api, verifier, access_token):
        self.api, self.verifier, self.access_token = api, verifier, access_token

    def caller(self):
        encoded = self.access_token()
        require(isinstance(encoded, str) and bool(encoded), "UNAUTHENTICATED")
        principal = self.verifier.verify(encoded)
        require(principal.tenant == self.api.ledger.store.scope.tenant, "FORBIDDEN")
        return principal

    @safe_tool
    async def submit_task(self, request_id: str, text: str, context_id: str | None = None,
                          transform: str = "echo", input_required: bool = False,
                          approval_required: bool = False, delay_seconds: int = 0) -> dict:
        """Submit text durably. Reuse request_id and identical arguments after an ambiguous response."""
        principal = self.caller()
        _key(request_id)
        require(isinstance(text, str), "TEXT_REQUIRED")
        message = {"messageId": request_id, "role": "ROLE_USER", "parts": [{"text": text}]}
        if context_id is not None:
            message["contextId"] = _key(context_id)
        return await self.api.send(principal, {"message": message,
            "configuration": {"returnImmediately": True},
            "metadata": {"a2amq": {"transform": transform, "inputRequired": input_required,
                "authRequired": approval_required, "delaySeconds": delay_seconds}}})

    @safe_tool
    async def submit_task_lite(self, request_id: str, text: str, context_id: str | None = None,
                               transform: str = "echo") -> dict:
        """Submit an immediate local-lite text task without Workflow-only options."""
        principal = self.caller()
        _key(request_id)
        require(isinstance(text, str), "TEXT_REQUIRED")
        message = {"messageId": request_id, "role": "ROLE_USER", "parts": [{"text": text}]}
        if context_id is not None:
            message["contextId"] = _key(context_id)
        return await self.api.send(principal, {"message": message,
            "configuration": {"returnImmediately": True},
            "metadata": {"a2amq": {"transform": transform}}})

    @safe_tool
    async def get_task(self, task_id: str, history_length: int = 0) -> dict:
        """Read an owned task. Terminal output is durable; a missing task is not a failed publish."""
        return await self.api.get(self.caller(), _key(task_id), history_length=history_length)

    @safe_tool
    async def list_tasks(self, page_size: int = 20, page_token: str = "",
                         context_id: str | None = None, status: str | None = None,
                         include_artifacts: bool = False) -> dict:
        """Read one bounded, owner-scoped page; use nextPageToken to continue."""
        return await self.api.list(self.caller(), page_size=page_size, page_token=page_token,
            context_id=context_id, status=status, include_artifacts=include_artifacts, history_length=0)

    @safe_tool
    async def continue_task(self, task_id: str, request_id: str, text: str) -> dict:
        """Provide input to INPUT_REQUIRED. Reuse request_id for the same continuation."""
        principal = self.caller()
        _key(request_id)
        require(isinstance(text, str), "TEXT_REQUIRED")
        current = await self.api.get(principal, _key(task_id), history_length=0)
        return await self.api.send(principal, {"message": {"messageId": request_id,
            "role": "ROLE_USER", "taskId": task_id, "contextId": current["contextId"],
            "parts": [{"text": text}]}, "configuration": {"returnImmediately": True}})

    @safe_tool
    async def cancel_task(self, task_id: str) -> dict:
        """Request owner-coordinated cancellation; completion races may make a task noncancelable."""
        return await self.api.cancel(self.caller(), _key(task_id))

    @safe_tool
    async def approve_task(self, task_id: str, request_id: str, approved: bool) -> dict:
        """Answer AUTH_REQUIRED with explicit approval authority; this is not an authorization bypass."""
        require(type(approved) is bool, "INVALID_APPROVAL")
        return await self.api.approve(self.caller(), _key(task_id), _key(request_id), approved)


def create_mcp(api, verifier, *, resource_url: str, profile: str = "workflow",
               allow_insecure_loopback: bool = False):
    """Return (official SDK server, authenticated Streamable HTTP app).

    Mount the app at '/', and enter server.session_manager.run() from the host
    lifespan. Modern requests are sessionless; legacy sessions are disabled too.
    """
    url = urlsplit(resource_url)
    secure = url.scheme == "https"
    loopback = (allow_insecure_loopback and url.scheme == "http"
                and url.hostname in {"127.0.0.1", "localhost", "::1"})
    require((secure or loopback) and bool(url.hostname) and url.path == "/mcp"
            and not url.username and not url.password and not url.query and not url.fragment,
            "MCP_HTTPS_RESOURCE_REQUIRED")
    require(verifier.audience == resource_url and urlsplit(verifier.issuer).scheme == "https",
            "MCP_RESOURCE_AUDIENCE_REQUIRED")
    require(profile in {"workflow", "local-lite"}, "INVALID_RUNTIME_PROFILE")
    try:
        from pydantic import AnyHttpUrl
        from mcp.server import MCPServer
        from mcp.server.auth.middleware.auth_context import get_access_token
        from mcp.server.auth.provider import AccessToken
        from mcp.server.auth.settings import AuthSettings
        from mcp.server.transport_security import TransportSecuritySettings
        from mcp.types import ToolAnnotations
    except ImportError:
        raise PermanentError("MCP_EXTRA_REQUIRED") from None

    class Verifier:
        async def verify_token(self, encoded):
            try:
                p = verifier.verify(encoded)
                require(p.tenant == api.ledger.store.scope.tenant, "FORBIDDEN")
                return AccessToken(token=encoded, client_id=p.subject, subject=p.subject,
                                   scopes=sorted(p.scopes))
            except PermanentError:
                return None

    def current_token():
        token = get_access_token()
        return None if token is None else token.token

    tools = MCPTools(api, verifier, current_token)
    server = MCPServer("A2A-MQ durable tasks", token_verifier=Verifier(),
        auth=AuthSettings(issuer_url=AnyHttpUrl(verifier.issuer),
                          resource_server_url=AnyHttpUrl(resource_url), required_scopes=[]))
    registered = ({
        "submit_task": tools.submit_task_lite,
        "get_task": tools.get_task,
        "list_tasks": tools.list_tasks,
        "cancel_task": tools.cancel_task,
    } if profile == "local-lite" else {
        "submit_task": tools.submit_task,
        "get_task": tools.get_task,
        "list_tasks": tools.list_tasks,
        "continue_task": tools.continue_task,
        "cancel_task": tools.cancel_task,
        "approve_task": tools.approve_task,
    })
    for name, function in registered.items():
        read_only = name in {"get_task", "list_tasks"}
        server.tool(name=name, annotations=ToolAnnotations(read_only_hint=read_only,
            destructive_hint=name in {"cancel_task", "approve_task"}, idempotent_hint=True,
            open_world_hint=False))(function)
    origin = f"{url.scheme}://{url.netloc}"
    transport = TransportSecuritySettings(allowed_hosts=[url.netloc], allowed_origins=[origin])
    app = server.streamable_http_app(stateless_http=True, json_response=True, transport_security=transport)
    return server, BoundedJSON(_SingleAuthorization(app))


class _SingleAuthorization:
    """Keep OAuth discovery native; reject ambiguous credentials before the SDK."""

    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        if scope["type"] == "http":
            values = [v for k, v in scope.get("headers", []) if k.lower() == b"authorization"]
            if len(values) > 1:
                from starlette.responses import JSONResponse
                await JSONResponse({"error": "invalid_token"}, status_code=401)(scope, receive, send)
                return
        await self.app(scope, receive, send)
