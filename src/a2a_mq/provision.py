"""Bounded deployment bootstrap and health checks; never used by task execution."""
import argparse
import asyncio
import json
import os
from pathlib import Path
import time
from urllib.parse import quote, urlsplit

import httpx

from .configuration import read_file
from .domain import require
from .vault import FileToken


def rabbit(url, password_file, plan, *, development=False, client=None):
    parsed = urlsplit(url)
    require(parsed.scheme == ("http" if development else "https") and parsed.hostname
            and not parsed.username and not parsed.password and not parsed.query and not parsed.fragment
            and "?" not in url and "#" not in url
            and parsed.path in {"", "/"}, "PROVISION_TLS_REQUIRED")
    password = FileToken(password_file)()
    owns = client is None
    client = client or httpx.Client(timeout=5, follow_redirects=False, trust_env=False)
    try:
        deadline = time.monotonic() + 120
        while True:
            try:
                response = client.get(url.rstrip("/")+"/api/overview", auth=("a2amq", password))
                if response.status_code == 200:
                    break
            except httpx.RequestError:
                pass
            require(time.monotonic() < deadline, "BROKER_START_TIMEOUT")
            time.sleep(1)
        def put(path, data):
            result = client.put(url.rstrip("/")+path, json=data, auth=("a2amq",password))
            require(result.status_code in {201, 204}, "BROKER_PROVISION_REJECTED")
        for exchange in plan["exchanges"]:
            put("/api/exchanges/%2F/"+quote(exchange["name"],safe=""),
                {"type": "fanout", "durable": True, "auto_delete": False, "internal": False, "arguments": {}})
        for policy in plan["policies"]:
            data = {k: v for k, v in policy.items() if k != "name"}
            put("/api/policies/%2F/"+quote(policy["name"],safe=""), data)
            actual = client.get(url.rstrip("/")+"/api/policies/%2F/"+quote(policy["name"],safe=""), auth=("a2amq",password))
            require(actual.status_code == 200 and all(actual.json().get(k) == v for k,v in data.items()),
                    "BROKER_POLICY_CONFLICT")
        for queue in plan["queues"]:
            put("/api/queues/%2F/"+quote(queue["name"],safe=""),
                {"durable": True, "auto_delete": False, "arguments": queue["arguments"]})
            result = client.post(url.rstrip("/")+"/api/bindings/%2F/e/"+quote(queue["topic"],safe="")+
                "/q/"+quote(queue["name"],safe=""), json={"routing_key": "", "arguments": {}}, auth=("a2amq",password))
            require(result.status_code in {201, 204}, "BROKER_BINDING_REJECTED")
            result = client.get(url.rstrip("/")+"/api/queues/%2F/"+quote(queue["name"],safe=""), auth=("a2amq",password))
            require(result.status_code == 200, "BROKER_VERIFY_FAILED")
            actual = result.json()
            require(actual.get("durable") is True and actual.get("arguments") == queue["arguments"], "BROKER_CONFIG_CONFLICT")
        return {"exchanges": len(plan["exchanges"]), "queues": len(plan["queues"]), "verified": True}
    finally:
        if owns:
            client.close()


async def jetstream(url, password_file, plan, *, development=False, connect=None):
    parsed = urlsplit(url)
    require(parsed.scheme == ("nats" if development else "tls") and parsed.hostname
            and not parsed.username and not parsed.password and not parsed.query and not parsed.fragment
            and "?" not in url and "#" not in url,
            "PROVISION_TLS_REQUIRED")
    if connect is None:
        import nats
        connect = nats.connect
    credential = FileToken(password_file)()
    nc = None
    try:
        async with asyncio.timeout(120):
            while nc is None:
                try:
                    nc = await connect(servers=[url], token=credential, connect_timeout=5,
                                       max_reconnect_attempts=0, allow_reconnect=False)
                except Exception:
                    await asyncio.sleep(1)
            stream = plan["stream"]
            name = stream["name"]
            require(name == "A2A_MQ", "INVALID_STREAM_NAME")
            response = await nc.request("$JS.API.STREAM.INFO."+name, b"", timeout=5)
            data = json.loads(response.data)
            if data.get("error", {}).get("code") == 404:
                response = await nc.request("$JS.API.STREAM.CREATE."+name, json.dumps(stream).encode(), timeout=5)
                data = json.loads(response.data)
            require("error" not in data, "BROKER_PROVISION_REJECTED")
            actual = data.get("config", {})
            require(all(actual.get(k) == v for k, v in stream.items()), "BROKER_CONFIG_CONFLICT")
            return {"stream": name, "verified": True}
    finally:
        if nc is not None:
            await nc.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--readiness", action="store_true")
    parser.add_argument("--rabbit-url")
    parser.add_argument("--nats-url")
    parser.add_argument("--password-file")
    parser.add_argument("--plan", type=Path)
    parser.add_argument("--development", action="store_true")
    args = parser.parse_args()
    if args.readiness:
        with httpx.Client(trust_env=False, timeout=8) as client:
            response = client.get("http://127.0.0.1:8000/health/ready", headers={
                "dapr-api-token": FileToken(os.environ["APP_API_TOKEN_FILE"])()})
            raise SystemExit(0 if response.status_code == 200 else 1)
    require(bool(args.rabbit_url) != bool(args.nats_url) and args.password_file and args.plan, "PROVISION_ARGUMENTS_REQUIRED")
    if args.nats_url:
        print(json.dumps(asyncio.run(jetstream(args.nats_url, args.password_file,
            json.loads(read_file(str(args.plan))), development=args.development))))
        return
    print(json.dumps(rabbit(args.rabbit_url, args.password_file, json.loads(read_file(str(args.plan))),
                            development=args.development)))


if __name__ == "__main__":
    main()
