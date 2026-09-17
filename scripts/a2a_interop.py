#!/usr/bin/env python3
"""Official A2A 1.0 client smoke against a deployed A2A-MQ API.

Runs discovery plus send/get/list over each requested official HTTP binding. Token
is read from a protected file and never placed in argv after process startup logs.
"""
from __future__ import annotations

import argparse
import asyncio
import json
from pathlib import Path
from urllib.parse import urlsplit
import uuid


def _url(value: str, allow_loopback_http: bool) -> str:
    parsed = urlsplit(value)
    loopback = parsed.hostname in {"127.0.0.1", "localhost", "::1"}
    if not parsed.hostname or parsed.username or parsed.password or parsed.query or parsed.fragment:
        raise ValueError("INVALID_URL")
    if parsed.scheme != "https" and not (allow_loopback_http and parsed.scheme == "http" and loopback):
        raise ValueError("HTTPS_REQUIRED")
    return value.rstrip("/")


async def run(base_url: str, token_file: Path, protocols: list[str], *, allow_loopback_http: bool) -> dict:
    base_url = _url(base_url, allow_loopback_http)
    from a2a.client import ClientConfig, ClientFactory
    from a2a.types import GetTaskRequest, ListTasksRequest, Message, Part, Role, SendMessageRequest
    import httpx
    from a2a_mq.vault import FileToken

    token = FileToken(token_file)()
    report = {"url": base_url, "protocols": {}}
    for protocol in protocols:
        async with httpx.AsyncClient(headers={"Authorization": "Bearer " + token},
                                     follow_redirects=False, timeout=30, trust_env=False) as http:
            factory = ClientFactory(ClientConfig(
                streaming=True, httpx_client=http,
                supported_protocol_bindings=[protocol], use_client_preference=True,
                accepted_output_modes=["text/plain"],
            ))
            client = await factory.create_from_url(base_url)
            task_id = None
            events = 0
            request = SendMessageRequest(message=Message(
                role=Role.ROLE_USER, message_id=str(uuid.uuid4()), parts=[Part(text="a2amq interop")]))
            try:
                async for event in client.send_message(request=request):
                    events += 1
                    if event.HasField("task"):
                        task_id = event.task.id
                if not task_id:
                    raise RuntimeError("NO_TASK_FROM_SEND")
                task = await client.get_task(request=GetTaskRequest(id=task_id))
                listing = await client.list_tasks(request=ListTasksRequest(page_size=10))
                if task.id != task_id or not any(item.id == task_id for item in listing.tasks):
                    raise RuntimeError("TASK_QUERY_MISMATCH")
                report["protocols"][protocol] = {"events": events, "taskId": task_id,
                                                  "state": int(task.status.state), "listed": True}
            finally:
                await client.close()
    return report


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", required=True)
    parser.add_argument("--token-file", required=True, type=Path)
    parser.add_argument("--protocol", action="append", choices=("JSONRPC", "HTTP+JSON"))
    parser.add_argument("--allow-loopback-http", action="store_true")
    args = parser.parse_args()
    protocols = args.protocol or ["JSONRPC", "HTTP+JSON"]
    try:
        result = asyncio.run(run(args.url, args.token_file, protocols,
                                 allow_loopback_http=args.allow_loopback_http))
        print(json.dumps(result, sort_keys=True))
        return 0
    except Exception as exc:
        print(json.dumps({"error": type(exc).__name__}))
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
