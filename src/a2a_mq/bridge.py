"""Official A2A 1.0 SDK routes backed by authorized durable task operations.

Wire codecs, protocol version checks, JSON-RPC, REST and SSE framing belong to
pinned a2a-sdk. This module deliberately implements no competing A2A binding.
"""
from __future__ import annotations

from contextlib import contextmanager
from urllib.parse import urlsplit

from .binding import assert_agent_card
from .domain import PermanentError, RetryableError, TERMINAL, canonical, require, state_of
from .http_boundary import BoundedJSON, sanitize_sdk_logs
from .identity import BearerIdentity, current_principal


def create_bridge(api, verifier, *, public_url: str, profile: str = "workflow",
                  allow_insecure_loopback: bool = False):
    try:
        parsed = urlsplit(public_url)
        port = parsed.port
    except (TypeError, ValueError):
        raise PermanentError("PUBLIC_HTTPS_URL_REQUIRED") from None
    secure = parsed.scheme == "https"
    loopback = (allow_insecure_loopback and parsed.scheme == "http"
                and parsed.hostname in {"127.0.0.1", "localhost", "::1"})
    require(isinstance(public_url, str) and (secure or loopback) and bool(parsed.hostname)
            and not parsed.username and not parsed.password and not parsed.query and not parsed.fragment
            and "?" not in public_url and "#" not in public_url
            and not any(c.isspace() for c in public_url)
            and parsed.path in {"", "/"} and (port is None or 1 <= port <= 65535),
            "PUBLIC_HTTPS_URL_REQUIRED")
    require(profile in {"workflow", "local-lite"}, "INVALID_RUNTIME_PROFILE")
    base_url = public_url.rstrip("/")
    try:
        from google.protobuf.json_format import MessageToDict, ParseDict
        from starlette.applications import Starlette
        from starlette.exceptions import HTTPException
        from starlette.responses import JSONResponse
        from starlette.routing import Route
        from a2a.server.request_handlers.request_handler import RequestHandler
        from a2a.server.routes.agent_card_routes import create_agent_card_routes
        from a2a.server.routes.jsonrpc_routes import create_jsonrpc_routes
        from a2a.server.routes.rest_routes import create_rest_routes
        from a2a.types import a2a_pb2 as pb
        from a2a.utils import errors
        from a2a.utils.proto_utils import validate_proto_required_fields
    except ImportError:
        raise PermanentError("BRIDGE_EXTRA_REQUIRED") from None

    tenant = api.ledger.store.scope.tenant
    local_lite = profile == "local-lite"
    description = ("Lightweight durable immediate text tasks over Dapr Pub/Sub. "
                   "Workflow waits/timers/approval are disabled in this profile."
                   if local_lite else
                   "Durable text tasks over Dapr Pub/Sub and Dapr Workflow. Experimental, not certified.")
    skill_description = ("Echo or uppercase immediate text without durable workflow waits."
                         if local_lite else
                         "Echo or uppercase text with optional input and approval pauses.")
    skill_tags = ["text", "local-lite"] if local_lite else ["text", "workflow"]
    card = pb.AgentCard(
        name="A2A-MQ durable reference", version="0.1.0",
        description=description,
        supported_interfaces=[
            pb.AgentInterface(url=base_url + "/rpc", protocol_binding="JSONRPC", protocol_version="1.0"),
            pb.AgentInterface(url=base_url, protocol_binding="HTTP+JSON", protocol_version="1.0"),
        ],
        capabilities=pb.AgentCapabilities(streaming=True, push_notifications=False, extended_agent_card=True),
        default_input_modes=["text/plain"], default_output_modes=["text/plain"],
        skills=[pb.AgentSkill(id="durable-text", name="Durable text transformation",
                description=skill_description, tags=skill_tags,
                input_modes=["text/plain"], output_modes=["text/plain"])],
        security_schemes={"bearer": pb.SecurityScheme(http_auth_security_scheme=
            pb.HTTPAuthSecurityScheme(scheme="Bearer", bearer_format="JWT"))},
        security_requirements=[pb.SecurityRequirement(schemes={"bearer": pb.StringList(list=[])})],
    )
    assert_agent_card(card, base_url)

    @contextmanager
    def mapped_errors():
        try:
            yield
        except RetryableError as error:
            raise HTTPException(status_code=503, detail=error.code, headers={"Retry-After": "1"}) from None
        except PermanentError as error:
            if error.code in {"UNAUTHENTICATED", "FORBIDDEN"}:
                raise HTTPException(status_code=401 if error.code == "UNAUTHENTICATED" else 403,
                                    detail=error.code) from None
            if error.code == "TASK_NOT_FOUND":
                raise errors.TaskNotFoundError() from None
            if error.code in {"TASK_NOT_CANCELABLE", "TASK_TERMINAL"}:
                raise errors.TaskNotCancelableError() from None
            if error.code == "PUSH_NOTIFICATIONS_UNSUPPORTED":
                raise errors.PushNotificationNotSupportedError() from None
            if error.code == "CONTENT_TYPE_UNSUPPORTED":
                raise errors.ContentTypeNotSupportedError() from None
            raise errors.InvalidParamsError(message=error.code) from None

    def principal(params):
        validate_proto_required_fields(params)
        caller = current_principal()
        requested_tenant = getattr(params, "tenant", "")
        require(not requested_tenant or requested_tenant == caller.tenant == tenant, "FORBIDDEN")
        require(caller.tenant == tenant, "FORBIDDEN")
        return caller

    def task_proto(value):
        return ParseDict(value, pb.Task())

    def optional_length(params):
        return params.history_length if params.HasField("history_length") else None

    async def events(snapshots):
        previous = None
        try:
            async for value in snapshots:
                current = task_proto(value)
                if previous is None:
                    yield current
                else:
                    if canonical(previous.get("artifacts", [])) != canonical(value.get("artifacts", [])):
                        for artifact in current.artifacts:
                            yield pb.TaskArtifactUpdateEvent(task_id=current.id, context_id=current.context_id,
                                                             artifact=artifact, append=False, last_chunk=True)
                    yield pb.TaskStatusUpdateEvent(task_id=current.id, context_id=current.context_id,
                                                   status=current.status)
                previous = value
        except (RetryableError, PermanentError):
            if previous is None:
                raise
            raise errors.InternalError(message="STREAM_INTERRUPTED_RECONNECT_TO_TASK") from None
        finally:
            await snapshots.aclose()

    class Handler(RequestHandler):
        async def on_message_send(self, params, context):
            with mapped_errors():
                caller = principal(params)
                request = MessageToDict(params)
                request.pop("tenant", None)
                return task_proto(await api.send(caller, request))

        async def on_get_task(self, params, context):
            with mapped_errors():
                return task_proto(await api.get(principal(params), params.id,
                                                 history_length=optional_length(params)))

        async def on_list_tasks(self, params, context):
            with mapped_errors():
                caller = principal(params)
                status = pb.TaskState.Name(params.status).removeprefix("TASK_STATE_") if params.status else None
                result = await api.list(caller,
                    page_size=params.page_size if params.HasField("page_size") else 50,
                    page_token=params.page_token, context_id=params.context_id or None, status=status,
                    status_timestamp_after=params.status_timestamp_after.ToJsonString()
                        if params.HasField("status_timestamp_after") else None,
                    history_length=optional_length(params), include_artifacts=params.include_artifacts)
                return ParseDict(result, pb.ListTasksResponse())

        async def on_cancel_task(self, params, context):
            with mapped_errors():
                return task_proto(await api.cancel(principal(params), params.id))

        async def on_message_send_stream(self, params, context):
            with mapped_errors():
                caller = principal(params)
                request = MessageToDict(params)
                request.pop("tenant", None)
                stream = events(api.send_snapshots(caller, request))
                try:
                    async for event in stream:
                        yield event
                finally:
                    await stream.aclose()

        async def on_subscribe_to_task(self, params, context):
            with mapped_errors():
                caller = principal(params)
                task = await api.get(caller, params.id)
                if state_of(task) in TERMINAL:
                    raise errors.UnsupportedOperationError()
                stream = events(api.snapshots(caller, params.id))
                try:
                    async for event in stream:
                        yield event
                finally:
                    await stream.aclose()

        async def on_get_extended_agent_card(self, params, context):
            with mapped_errors():
                principal(params).authorize("read", tenant)
                result = pb.AgentCard()
                result.CopyFrom(card)
                return result

        async def on_create_task_push_notification_config(self, params, context):
            raise errors.PushNotificationNotSupportedError()

        async def on_get_task_push_notification_config(self, params, context):
            raise errors.PushNotificationNotSupportedError()

        async def on_list_task_push_notification_configs(self, params, context):
            raise errors.PushNotificationNotSupportedError()

        async def on_delete_task_push_notification_config(self, params, context):
            raise errors.PushNotificationNotSupportedError()

    async def approve(request):
        with mapped_errors():
            body = await request.json()
            require(isinstance(body, dict) and set(body) == {"requestId", "approved"}, "INVALID_APPROVAL")
            task = await api.approve(current_principal(), request.path_params["task_id"],
                                     body["requestId"], body["approved"])
            return JSONResponse({"task": task, "approvalQueued": state_of(task) == "AUTH_REQUIRED"})

    def enforce_tenant(inner):
        async def asgi(scope, receive, send):
            if scope["type"] == "http":
                principal = scope.get("state", {}).get("principal")
                if principal is not None and principal.tenant != tenant:
                    response = JSONResponse({"error": {"code": 403, "message": "FORBIDDEN", "status": "PERMISSION_DENIED"}},
                                            status_code=403,
                                            headers={"Cache-Control": "no-store"})
                    await response(scope, receive, send)
                    return
            await inner(scope, receive, send)
        return asgi

    handler = Handler()
    app = Starlette(routes=[*create_agent_card_routes(card),
        *create_jsonrpc_routes(handler, rpc_url="/rpc", enable_v0_3_compat=False),
        Route("/operations/tasks/{task_id}/approve", approve, methods=["POST"]),
        *create_rest_routes(handler, enable_v0_3_compat=False)])
    sanitize_sdk_logs()
    app.state.a2amq_handler = handler
    app.state.a2amq_card = card
    return BearerIdentity(BoundedJSON(enforce_tenant(app)), verifier,
                          public_paths=frozenset({"/.well-known/agent-card.json"}))
