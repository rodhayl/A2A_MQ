"""Pinned MCP v2 registration contract; HTTP auth is delegated to the official SDK."""
import pytest

pytestmark = pytest.mark.mcp

from a2a_mq.domain import PermanentError
from a2a_mq.identity import Principal
from a2a_mq.mcp_adapter import create_mcp, streamable_http_app


class Verifier:
    issuer = "https://issuer.example"
    audience = "https://mcp.example/mcp"
    def verify(self, token):
        return Principal("alice", "demo", frozenset({"a2a:read", "a2a:send", "a2a:cancel", "a2a:approve"}))


class API:
    async def get(self, *args, **kwargs): return {"id": "task"}
    async def list(self, *args, **kwargs): return {"tasks": []}
    async def send(self, *args, **kwargs): return {"id": "task"}
    async def cancel(self, *args, **kwargs): return {"id": "task"}
    async def approve(self, *args, **kwargs): return {"id": "task"}


async def test_official_server_registers_bounded_task_tools():
    pytest.importorskip("mcp", reason="mcp extra required by this contract")
    server = create_mcp(API(), Verifier(), resource_url=Verifier.audience)
    names = {tool.name for tool in await server.list_tools()}
    assert names == {"get_task", "list_tasks", "send_text", "provide_input", "cancel_task", "approve_task"}


@pytest.mark.parametrize("url", [
    "http://mcp.example/mcp",
    "https://mcp.example/",
    "https://mcp.example/other",
    "https://mcp.example/mcp?",
    "https://mcp.example/mcp#",
    "https://mcp.example:70000/mcp",
    "https://mcp.example/mcp\n",
    "https://mcp.example/mcp\t",
])
def test_mcp_adapter_requires_exact_https_resource_before_sdk_import(url):
    verifier = Verifier()
    verifier.audience = url
    with pytest.raises(PermanentError, match="MCP_HTTPS_RESOURCE_REQUIRED"):
        create_mcp(API(), verifier, resource_url=url)


def test_mcp_requires_exact_resource_audience_before_sdk_import():
    verifier = Verifier(); verifier.audience = "https://other.example/mcp"
    with pytest.raises(PermanentError, match="MCP_AUDIENCE_MISMATCH"):
        create_mcp(API(), verifier, resource_url="https://mcp.example/mcp")


def test_streamable_http_app_is_bounded_and_authenticated():
    pytest.importorskip("mcp", reason="mcp extra required by this contract")
    app = streamable_http_app(API(), Verifier(), resource_url=Verifier.audience)
    assert callable(app)
