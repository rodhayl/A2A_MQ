#!/usr/bin/env python3
"""Native component-integration demo: protected topics, worker and independent observers.

Uses real Dapr/NATS and maintained library primitives, NOT ServiceApplication or
its public A2A API. LocalKeyRing is development-only; Vault is not exercised.
All generated secrets/logs stay in a fresh owner-private fixture directory.
"""
from __future__ import annotations

import argparse
import asyncio
import base64
from contextlib import asynccontextmanager
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import secrets
import signal
import socket
import subprocess
import sys
import time

import httpx
import nats

from a2a_mq.domain import Envelope, canonical, command
from a2a_mq.protection import EnvelopeProtector, LocalKey, LocalKeyRing
from a2a_mq.reference import echo_events
from a2a_mq.runtime import AsyncSQLite
from a2a_mq.secure_transport import ProtectedPublisher, SEALED_MIME
from a2a_mq.storage import SQLiteStore
from a2a_mq.transport import DaprPublisher, cloud_event, subscriber
from check_local_native_versions import _verified_bootstrap
from portfolio_demo import FAULT_NAMES, OwnedStack, check, stop_signal

TENANT = "busdemo"
COMMAND = f"a2a.v1.{TENANT}.command.skill.echo"
EVENT = f"a2a.v1.{TENANT}.event.task"
DLQ = f"a2a.v1.{TENANT}.dlq"
ROLES = ("producer", "worker", "observer-a", "observer-b")
ROOT = Path(__file__).resolve().parents[1]


def digest(raw):
    return hashlib.sha256(raw).hexdigest()


def write_private(path, value):
    with path.open("x", encoding="utf-8") as stream:
        json.dump(value, stream, sort_keys=True)
    path.chmod(0o600)


def ports_available(base):
    ports = [base] + [base + 10 + i * 10 + j for i in range(4) for j in range(4)]
    for port in ports:
        with socket.socket() as sock:
            sock.bind(("127.0.0.1", port))
    return ports


def prepare(root, base):
    check(not root.exists(), "FIXTURE_ALREADY_EXISTS")
    ports_available(base)
    root.mkdir(mode=0o700)
    broker_token = secrets.token_hex(24)
    config = {"basePort": base, "natsToken": broker_token}
    write_private(root / "fixture.json", config)
    (root / "nats").mkdir(mode=0o700)
    (root / "nats.conf").write_text(
        f'host: "127.0.0.1"\nport: {base}\nauthorization {{ token: "{broker_token}" }}\n'
        f'jetstream {{ store_dir: {json.dumps(str(root / "nats"))} }}\n')
    (root / "nats.conf").chmod(0o600)
    keys = {k: base64.b64encode(os.urandom(32)).decode() for k in ("command", "event")}
    for index, role in enumerate(ROLES):
        folder = root / role
        folder.mkdir(mode=0o700)
        (folder / "components").mkdir(mode=0o700)
        port = base + 10 + index * 10
        topic = COMMAND if role == "worker" else EVENT if role.startswith("observer") else None
        publishes = [COMMAND] if role == "producer" else [EVENT, DLQ] if role == "worker" else [DLQ]
        role_keys = {k: v for k, v in keys.items()
                     if role == "worker" or k == ("command" if role == "producer" else "event")}
        role_config = {"role": role, "appPort": port, "httpPort": port + 1,
                       "grpcPort": port + 2, "internalPort": port + 3,
                       "appToken": secrets.token_hex(24), "daprToken": secrets.token_hex(24),
                       "keys": role_keys, "topic": topic}
        write_private(folder / "config.json", role_config)
        meta = {"natsURL": f"nats://127.0.0.1:{base}", "token": broker_token,
                "name": role, "streamName": "A2A_MQ", "allowedTopics": ",".join([COMMAND, EVENT, DLQ]),
                "protectedTopics": ",".join([COMMAND, EVENT, DLQ]),
                "publishingScopes": role + "=" + ",".join(publishes),
                "subscriptionScopes": role + "=" + (topic or ""),
                "ackPolicy": "explicit", "deliverPolicy": "all", "ackWait": "3s",
                "maxDeliver": "20", "maxAckPending": "8", "replicas": "1", "memoryStorage": "false"}
        if topic:
            meta.update(durableName="demo-" + role, queueGroupName="demo-" + role)
        component = {"apiVersion": "dapr.io/v1alpha1", "kind": "Component",
                     "metadata": {"name": "bus"}, "scopes": [role],
                     "spec": {"type": "pubsub.jetstream", "version": "v1",
                              "metadata": [{"name": k, "value": v} for k, v in meta.items()]}}
        write_private(folder / "components/bus.yaml", component)
    return config


def role_config(root, role):
    return json.loads((root / role / "config.json").read_text())


def protectors(config):
    return {COMMAND if kind == "command" else EVENT:
            EnvelopeProtector(LocalKeyRing([LocalKey("dev-v1", TENANT, kind + "-readers",
                base64.b64decode(material))], "dev-v1"), tenant=TENANT, receiver=kind + "-readers")
            for kind, material in config["keys"].items()}


def bus_for(config):
    return DaprPublisher(f"http://127.0.0.1:{config['httpPort']}", api_token=config["daprToken"])


def receipt(root, role, envelope):
    value = envelope.as_dict()
    payload = value["payload"]
    record = {"pid": os.getpid(), "messageId": value["messageId"], "route": envelope.route,
              "correlationId": value["correlationId"], "payloadSha256": digest(canonical(payload).encode())}
    if "taskId" in value:
        record.update(taskId=value["taskId"], sequence=value["sequence"])
    with (root / role / "receipts.jsonl").open("a", encoding="utf-8") as stream:
        stream.write(canonical(record) + "\n")


def make_app(root, role):
    cfg = role_config(root, role)
    bus = bus_for(cfg)
    crypto = protectors(cfg)
    lane = AsyncSQLite(SQLiteStore(root / role / "cache.db", tenant=TENANT, consumer=role))
    publisher = ProtectedPublisher(bus, lane, crypto)

    async def handle(envelope):
        entry = {"pid": os.getpid(), "messageId": envelope.as_dict()["messageId"], "route": envelope.route}
        with (root / role / "handler-entries.jsonl").open("a") as stream:
            stream.write(canonical(entry) + "\n")
        if role == "worker":
            for event in echo_events(envelope):
                await publisher.publish(event)
        receipt(root, role, envelope)

    @asynccontextmanager
    async def lifespan(app):
        try:
            yield
        finally:
            await bus.close()
            await lane.aclose()

    return subscriber(handle, bus, tenant=TENANT, consumer=role, topic=cfg["topic"],
                      app_token=cfg["appToken"], protector=crypto[cfg["topic"]], lifespan=lifespan)


class RecordedPublisher(DaprPublisher):
    """Record a sanitized digest only after the real sidecar accepts publication."""
    def __init__(self, config, root):
        super().__init__(f"http://127.0.0.1:{config['httpPort']}", api_token=config["daprToken"])
        self.root = root

    async def _post(self, topic, event):
        await super()._post(topic, event)
        record = {"pid": os.getpid(), "messageId": event["id"], "topic": topic,
                  "sealedSha256": digest(canonical(event["data"]).encode())}
        with (self.root / "producer/publications.jsonl").open("a") as stream:
            stream.write(canonical(record) + "\n")


async def publish_command(root, filename):
    cfg = role_config(root, "producer")
    envelope = Envelope.from_dict(json.loads((root / filename).read_text()))
    lane = AsyncSQLite(SQLiteStore(root / "producer/cache.db", tenant=TENANT, consumer="producer"))
    bus = RecordedPublisher(cfg, root)
    try:
        await ProtectedPublisher(bus, lane, protectors(cfg)).publish(envelope)
        receipt(root, "producer", envelope)
    finally:
        await bus.close()
        await lane.aclose()


def launch(root, tools, name, argv, extra=None):
    owned = OwnedStack(root, tools)
    owned.log = (root / (name + ".log")).open("a", encoding="utf-8")
    safe = {k: v for k, v in os.environ.items() if k in {"PATH", "HOME", "LANG", "TMPDIR"}}
    safe.update(PYTHONPATH=str(ROOT / "src"), PYTHONNOUSERSITE="1", DAPR_HOST_IP="127.0.0.1")
    safe.update(extra or {})
    for key in FAULT_NAMES:
        safe.pop(key, None)
    try:
        owned.process = subprocess.Popen(argv, env=safe, stdout=owned.log, stderr=subprocess.STDOUT,
                                         start_new_session=True)
    except BaseException:
        owned.log.close()
        raise
    return owned


def start_role(root, tools, role, owned):
    cfg = role_config(root, role)
    if role != "producer":
        owned[role] = launch(root, tools, role, [sys.executable, str(Path(__file__).resolve()),
                            "--work-dir", str(root), "--role", role])
    argv = [str(tools / "daprd"), "--app-id", role,
            "--dapr-http-port", str(cfg["httpPort"]), "--dapr-grpc-port", str(cfg["grpcPort"]),
            "--dapr-internal-grpc-port", str(cfg["internalPort"]),
            "--dapr-internal-grpc-listen-address", "127.0.0.1",
            "--dapr-listen-addresses", "127.0.0.1", "--resources-path", str(root / role / "components"),
            "--enable-metrics=false", "--log-level", "warn"]
    if role != "producer":
        argv += ["--app-port", str(cfg["appPort"]), "--app-channel-address", "127.0.0.1"]
    owned[role + "-dapr"] = launch(root, tools, role + "-dapr", argv,
        {"DAPR_API_TOKEN": cfg["daprToken"], "APP_API_TOKEN": cfg["appToken"]})
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        check(owned[role + "-dapr"].process.poll() is None, "SIDECAR_EXITED")
        try:
            with httpx.Client(trust_env=False, timeout=.5) as client:
                response = client.get(f"http://127.0.0.1:{cfg['httpPort']}/v1.0/healthz/outbound",
                                      headers={"dapr-api-token": cfg["daprToken"]})
                if response.status_code == 204:
                    return
        except httpx.HTTPError:
            pass
        time.sleep(.1)
    raise RuntimeError("SIDECAR_NOT_READY")


def read_receipts(root, role):
    path = root / role / "receipts.jsonl"
    return [json.loads(line) for line in path.read_text().splitlines()] if path.exists() else []


def await_receipts(root, request, roles=("worker", "observer-a", "observer-b")):
    expected = echo_events(request)
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        good = True
        for role in roles:
            wanted = [request] if role == "worker" else expected
            records = read_receipts(root, role)
            for envelope in wanted:
                e = envelope.as_dict()
                good &= any(row["messageId"] == e["messageId"] and row["route"] == envelope.route
                    and row["payloadSha256"] == digest(canonical(e["payload"]).encode()) for row in records)
        if good:
            return
        time.sleep(.1)
    raise RuntimeError("RECIPIENT_EVIDENCE_TIMEOUT")


async def connect_broker(config):
    return await nats.connect(servers=[f"nats://127.0.0.1:{config['basePort']}"], token=config["natsToken"],
                              connect_timeout=5, allow_reconnect=False)


async def stream_records(config):
    nc = await connect_broker(config)
    try:
        js = nc.jetstream()
        info = await js.stream_info("A2A_MQ")
        check(info.state.messages < 100, "UNBOUNDED_FIXTURE_STREAM")
        values = []
        for seq in range(info.state.first_seq, info.state.last_seq + 1):
            msg = await js.get_msg("A2A_MQ", seq=seq)
            values.append({"sequence": seq, "subject": msg.subject, "event": json.loads(msg.data)})
        return values
    finally:
        await nc.close()


def inspect_protected(records, request):
    expected = [request, *echo_events(request)]
    found = []
    text = request.as_dict()["payload"]["message"]["parts"][0]["text"].encode()
    for envelope in expected:
        mid = envelope.as_dict()["messageId"]
        matches = [row for row in records if row["event"]["id"] == mid]
        check(len(matches) == 1, "BROKER_MESSAGE_NOT_UNIQUE")
        row = matches[0]
        event = row["event"]
        check(row["subject"] == envelope.route and event["datacontenttype"] == SEALED_MIME,
              "BROKER_PAYLOAD_NOT_SEALED")
        check(text not in canonical(event).encode(), "PLAINTEXT_CANARY_ON_BROKER")
        check(set(event["data"]) == {"protected", "ciphertext"}, "BROKER_ENVELOPE_NOT_SEALED")
        found.append({"messageId": mid, "topic": row["subject"], "sequence": row["sequence"],
                      "sealedSha256": digest(canonical(event["data"]).encode())})
    return found


async def negative_publications(root):
    cfg = role_config(root, "producer")
    protector = protectors(cfg)[COMMAND]
    bus = bus_for(cfg)
    ids = {}
    try:
        for case in ("plaintext", "tamper", "wrong-receiver", "wrong-key"):
            envelope = command("invalid synthetic fixture", tenant=TENANT)
            event = cloud_event(envelope)
            if case != "plaintext":
                chosen = protector
                if case in {"wrong-receiver", "wrong-key"}:
                    receiver = "other-readers" if case == "wrong-receiver" else "command-readers"
                    chosen = EnvelopeProtector(LocalKeyRing([LocalKey("dev-v1", TENANT, receiver,
                        os.urandom(32))], "dev-v1"), tenant=TENANT, receiver=receiver)
                data = json.loads(await chosen.seal(envelope))
                if case == "tamper":
                    raw = bytearray(base64.b64decode(data["ciphertext"]))
                    raw[0] ^= 1
                    data["ciphertext"] = base64.b64encode(raw).decode()
                event.update(data=data, datacontenttype=SEALED_MIME)
            await bus._post(COMMAND, event)
            ids[case] = event["id"]
    finally:
        await bus.close()
    return ids


def stop_owned(owned, report):
    failed = False
    for process in reversed(list(owned.values())):
        try:
            process.stop()
        except Exception:
            failed = True
    report["ownedProcessesStopped"] = not failed and all(p.process is None for p in owned.values())
    if not report["ownedProcessesStopped"]:
        report["status"] = "FAIL"
        raise RuntimeError("OWNED_PROCESS_CLEANUP_FAILED")


def run(root, tools, base, report):
    config = prepare(root, base)
    owned = {}
    try:
        owned["nats"] = launch(root, tools, "nats", [str(tools / "nats-server"), "-c", str(root / "nats.conf")])
        time.sleep(.3)
        async def provision():
            nc = await connect_broker(config)
            try:
                await nc.jetstream().add_stream(name="A2A_MQ", subjects=[f"a2a.v1.{TENANT}.>"],
                    storage="file", max_msgs=100, max_bytes=8 * 1024 * 1024)
            finally:
                await nc.close()
        asyncio.run(provision())
        for role in ROLES:
            start_role(root, tools, role, owned)
        report["initialProcesses"] = {role: {"appPid": owned[role].process.pid if role != "producer" else None,
                                      "sidecarPid": owned[role + "-dapr"].process.pid} for role in ROLES}
        def send(request, filename):
            write_private(root / filename, request.as_dict())
            producer = launch(root, tools, "producer-send", [sys.executable, str(Path(__file__).resolve()),
                "--work-dir", str(root), "--role", "producer", "--message", filename])
            owned["producer-send"] = producer
            check(producer.process.wait(timeout=20) == 0, "PRODUCER_FAILED")
            producer.stop()
        first = command("BUS-CANARY-" + secrets.token_hex(16), tenant=TENANT)
        send(first, "first.json")
        await_receipts(root, first)
        records = asyncio.run(stream_records(config))
        wire = inspect_protected(records, first)
        report["protectedWire"] = wire
        report["checks"].append("real-encrypted-command-and-event-topics-with-two-observers")
        before = wire[0]["sealedSha256"]
        send(first, "retry.json")  # New producer process, same immutable command and SQLite cache.
        import sqlite3
        with sqlite3.connect((root / "producer/cache.db").as_uri() + "?mode=ro", uri=True) as db:
            sealed = db.execute("SELECT sealed FROM a2amq_sealed WHERE event_id=?",
                                (first.as_dict()["messageId"],)).fetchone()[0]
        check(digest(sealed.encode()) == before, "RESTART_CHANGED_CIPHERTEXT")
        publications = [json.loads(line) for line in
                        (root / "producer/publications.jsonl").read_text().splitlines()]
        retries = [row for row in publications if row["messageId"] == first.as_dict()["messageId"]]
        check(len(retries) == 2 and len({row["pid"] for row in retries}) == 2
              and all(row["sealedSha256"] == before for row in retries), "RETRY_WIRE_CHANGED")
        report["retryPublications"] = retries
        report["checks"].append("producer-process-restart-reuses-persisted-ciphertext")
        # Keep the already-created durable observer subscription, stop only its owned processes.
        owned["observer-b-dapr"].stop()
        owned["observer-b"].stop()
        second = command("OFFLINE-CANARY-" + secrets.token_hex(16), tenant=TENANT)
        send(second, "second.json")
        await_receipts(root, second, roles=("worker", "observer-a"))
        check(not any(r["correlationId"] == second.as_dict()["correlationId"]
                      for r in read_receipts(root, "observer-b")), "OFFLINE_OBSERVER_RECEIVED")
        start_role(root, tools, "observer-b", owned)
        report["restartedObserver"] = {"appPid": owned["observer-b"].process.pid,
                                      "sidecarPid": owned["observer-b-dapr"].process.pid}
        await_receipts(root, second)
        report["protectedWire"].extend(inspect_protected(asyncio.run(stream_records(config)), second))
        report["checks"].append("offline-independent-observer-receives-retained-encrypted-events")
        negatives = asyncio.run(negative_publications(root))
        deadline = time.monotonic() + 30
        rejected = {}
        while time.monotonic() < deadline:
            for row in asyncio.run(stream_records(config)):
                event = row["event"]
                if row["subject"] == DLQ:
                    original = json.loads(base64.b64decode(event["data"]["originalBase64"]))
                    if original.get("id") in negatives.values():
                        rejected[original["id"]] = event["data"]["reason"]
            if set(rejected) == set(negatives.values()):
                break
            time.sleep(.1)
        check(set(rejected) == set(negatives.values()), "NEGATIVE_NOT_QUARANTINED")
        entries = [json.loads(line) for line in (root / "worker/handler-entries.jsonl").read_text().splitlines()]
        check(not any(r["messageId"] in negatives.values() for r in entries), "INVALID_MESSAGE_EXECUTED")
        expected_reasons = {"plaintext": "INVALID_CLOUDEVENT", "tamper": "DECRYPTION_FAILED",
                            "wrong-receiver": "SEALED_SCOPE_MISMATCH", "wrong-key": "DECRYPTION_FAILED"}
        check(all(rejected[mid] == expected_reasons[case] for case, mid in negatives.items()),
              "NEGATIVE_REASON_MISMATCH")
        report["workerHandlerEntries"] = entries
        report["negativeCases"] = {case: {"quarantined": True, "handlerCalled": False,
                                          "reason": rejected[mid]} for case, mid in negatives.items()}
        report["checks"].append("plaintext-tamper-wrong-receiver-wrong-key-rejected-before-handler")
        report["receipts"] = {role: read_receipts(root, role) for role in ROLES}
        report["status"] = "PASS"
    finally:
        stop_owned(owned, report)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--work-dir", type=Path, required=True)
    parser.add_argument("--tools-bin", type=Path)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--base-port", type=int, default=24222)
    parser.add_argument("--role", choices=ROLES)
    parser.add_argument("--message")
    args = parser.parse_args()
    root = args.work_dir.resolve()
    if args.role:
        if args.role == "producer":
            asyncio.run(publish_command(root, args.message))
        else:
            import uvicorn
            uvicorn.run(make_app(root, args.role), host="127.0.0.1",
                        port=role_config(root, args.role)["appPort"], access_log=False, log_level="warning")
        return 0
    if not args.tools_bin or not args.output or root.exists() or args.output.exists():
        parser.error("tools/output required; work-dir and output must be new")
    if not 1024 <= args.base_port <= 65490:
        parser.error("base-port must be in 1024..65490")
    tools = args.tools_bin.resolve()
    native = _verified_bootstrap(tools.parent / "manifest.json", str(tools / "daprd"), str(tools / "nats-server"))
    report = {"status": "FAIL", "createdAt": datetime.now(timezone.utc).isoformat(), "checks": [],
              "scope": "native protected transport component integration, deterministic reference agents",
              "publicA2AApiExercised": False, "serviceApplicationExercised": False,
              "vaultExercised": False, "keyProvider": "LocalKeyRing development-only",
              "topics": {"command": COMMAND, "event": EVENT},
              "subscriberGroups": {"worker": "demo-worker", "observer-a": "demo-observer-a",
                                   "observer-b": "demo-observer-b"},
              "externalExactlyOnce": False, "productionCertified": False,
              "plaintextAtAuthorizedEndpoints": True, "sqlWorkflowEncryptionClaimed": False,
              "harnessSha256": digest(Path(__file__).read_bytes()),
              "runtimeFilesSha256": {str(p.relative_to(ROOT)): digest(p.read_bytes())
                                      for p in sorted([*(ROOT / "src/a2a_mq").glob("*.py"),
                                                       *(ROOT / "src/a2a_mq").glob("*.json"),
                                                       ROOT / "pyproject.toml",
                                                       ROOT / "scripts/portfolio_demo.py",
                                                       ROOT / "scripts/check_local_native_versions.py"])},
              "nativeBinarySha256": {k: v["binarySha256"] for k, v in native["binaries"].items()}}
    old = signal.signal(signal.SIGTERM, stop_signal)
    try:
        run(root, tools, args.base_port, report)
    except KeyboardInterrupt:
        report["status"] = "CANCELED"
    except Exception as error:
        report["status"] = "FAIL"
        report["failureType"] = type(error).__name__
        # Only our fixed assertion labels may be public; never raw third-party messages.
        if isinstance(error, RuntimeError) and str(error).replace("_", "").isupper():
            report["failureCode"] = str(error)
    finally:
        signal.signal(signal.SIGTERM, old)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("x") as stream:
        json.dump(report, stream, indent=2, sort_keys=True)
        stream.write("\n")
    print(json.dumps({"status": report["status"], "checks": report["checks"]}))
    return 0 if report["status"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
