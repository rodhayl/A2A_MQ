#!/usr/bin/env python3
"""A bounded, real Local-lite failure demo; creates only a fresh disposable profile."""
from __future__ import annotations

import argparse
import asyncio
import base64
from contextlib import closing
from datetime import datetime, timezone
import hashlib
import importlib.metadata
import json
import math
import os
from pathlib import Path
import platform
import signal
import socket
import sqlite3
import statistics
import subprocess
import sys
import time

import httpx

from a2a_mq.local_runtime import LOCAL_LITE_PORTS, prepare

BASE = "http://127.0.0.1:18000"
FAULT_NAMES = (
    "A2AMQ_FAULT_INJECTION_ENABLE", "A2AMQ_FAULT_INJECTION_MODE",
    "A2AMQ_FAULT_INJECTION_TARGET", "A2AMQ_FAULT_RECORD_FILE", "A2AMQ_FAULT_SYNC_DIR",
)


def check(condition, code):
    if not condition:
        raise RuntimeError(code)


def request(message_id, text="synthetic invoice awaiting review"):
    return {"message": {"messageId": message_id, "role": "ROLE_USER", "parts": [{"text": text}]},
            "configuration": {"returnImmediately": True}, "metadata": {"a2amq": {"transform": "upper"}}}


def percentile(values, fraction):
    return sorted(values)[max(0, math.ceil(len(values) * fraction) - 1)]


def source_digest(root):
    """Content identity of runtime, demo and packaging, without generated/private files."""
    paths = sorted([*root.joinpath("src").rglob("*.py"), *root.joinpath("src").rglob("*.json"),
                    root / "pyproject.toml", root / "scripts/portfolio_demo.py"])
    digest = hashlib.sha256()
    for path in paths:
        digest.update(path.relative_to(root).as_posix().encode() + b"\0" + path.read_bytes() + b"\0")
    return digest.hexdigest()


class OwnedStack:
    """A fresh process group; no global pkill, port owner lookup or unrelated PID kill."""

    def __init__(self, profile, tools):
        self.profile, self.tools = profile, tools
        self.process = self.log = None

    def stop(self):
        if self.process is not None:
            group = self.process.pid  # start_new_session makes this our own session/group.
            try:
                os.killpg(group, signal.SIGTERM)
            except ProcessLookupError:
                pass
            try:
                self.process.wait(timeout=8)
            except subprocess.TimeoutExpired:
                pass
            # App may already be SIGKILLed by the test hook, leaving its own sidecars.
            try:
                os.killpg(group, signal.SIGKILL)
            except ProcessLookupError:
                pass
            self.process.wait(timeout=5)
            self.process = None
        if self.log is not None:
            self.log.close()
            self.log = None
        time.sleep(0.3)

    def start(self, fault=False):
        self.stop()
        for port in LOCAL_LITE_PORTS:
            with socket.socket() as sock:
                sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                sock.bind(("127.0.0.1", port))
        env = {k: v for k, v in os.environ.items() if k not in FAULT_NAMES}
        env["PATH"] = str(self.tools) + os.pathsep + env.get("PATH", "")
        env.update(A2AMQ_FAULT_INJECTION_ENABLE="local-validation-v1",
                   A2AMQ_FAULT_RECORD_FILE=str(self.profile / "fault-events.jsonl"))
        if fault:
            env.update(A2AMQ_FAULT_INJECTION_MODE="unacked_delivery",
                       A2AMQ_FAULT_INJECTION_TARGET="portfolio-recovery")
        self.log = (self.profile / "runtime.log").open("a", encoding="utf-8")
        self.process = subprocess.Popen(
            [sys.executable, "-m", "a2a_mq.local_runtime", "run", str(self.profile)],
            env=env, stdout=self.log, stderr=subprocess.STDOUT, start_new_session=True,
        )
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            check(self.process.poll() is None, "RUNTIME_EXITED_BEFORE_READY")
            try:
                response = httpx.get(BASE + "/health/ready", timeout=0.5, trust_env=False)
                if response.status_code == 200 and response.json().get("status") == "ready":
                    return
            except httpx.HTTPError:
                pass
            time.sleep(0.1)
        raise RuntimeError("READINESS_TIMEOUT")


def rpc(client, method, params):
    response = client.post("/rpc", json={"jsonrpc": "2.0", "id": "portfolio", "method": method,
                                         "params": params})
    response.raise_for_status()
    value = response.json()
    check("error" not in value, method + "_FAILED")
    return value["result"]


def completed(client, task_id):
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        value = rpc(client, "GetTask", {"id": task_id})
        state = value["status"]["state"]
        if state == "TASK_STATE_COMPLETED":
            return value
        check(state not in {"TASK_STATE_CANCELED", "TASK_STATE_FAILED", "TASK_STATE_REJECTED"},
              "UNEXPECTED_TERMINAL_STATE")
        time.sleep(0.05)
    raise RuntimeError("COMPLETION_TIMEOUT")


def sql(profile, query, params=()):
    # Read-only connection: the demo never forges acceptance evidence in the ledger.
    with closing(sqlite3.connect((profile / "state/a2amq.db").as_uri() + "?mode=ro", uri=True)) as db:
        return db.execute(query, params).fetchall()


def physical_deliveries(profile, task_id, message_id=None, after=0):
    rows = [json.loads(line) for line in (profile / "fault-events.jsonl").read_text().splitlines()]
    return [row for row in rows if row.get("event") == "broker_delivery"
            and row.get("taskId") == task_id and row.get("timestamp", 0) > after
            and (message_id is None or row.get("messageId") == message_id)]


def acknowledged_consumer(consumers, route, sequence):
    """A broker ACK floor, not an app log or publication marker, closes the original delivery."""
    matches = [c for c in consumers if c.get("config", {}).get("filter_subject") == route]
    check(len(matches) == 1, "COMMAND_CONSUMER_NOT_UNIQUE")
    consumer = matches[0]
    if (consumer.get("num_ack_pending") == 0
            and consumer.get("ack_floor", {}).get("stream_seq", 0) >= sequence):
        return consumer
    return None


async def original_ack_checkpoint(profile, task_id, message_id, expected_copies=1):
    """Read only the fresh owned broker; never add a consumer or change its dedup window."""
    import nats
    nc = await nats.connect(servers=["nats://127.0.0.1:4222"],
                            token=(profile / "nats-token").read_text().strip(),
                            connect_timeout=5, max_reconnect_attempts=0, allow_reconnect=False)
    try:
        async def query(subject, data=None):
            reply = await nc.request("$JS.API." + subject, json.dumps(data or {}).encode(), timeout=5)
            value = json.loads(reply.data)
            check("error" not in value, "BROKER_OBSERVATION_FAILED")
            return value
        info = await query("STREAM.INFO.A2A_MQ")
        window = info["config"]["duplicate_window"] / 1_000_000_000
        check(0 < window <= 300, "UNEXPECTED_DEDUP_WINDOW")
        state = info["state"]
        check(state["last_seq"] - state["first_seq"] < 200, "UNEXPECTED_FIXTURE_STREAM_SIZE")
        originals = []
        for sequence in range(state["first_seq"], state["last_seq"] + 1):
            message = (await query("STREAM.MSG.GET.A2A_MQ", {"seq": sequence}))["message"]
            event = json.loads(base64.b64decode(message["data"], validate=True))
            envelope = event.get("data", {})
            if event.get("id") == message_id and envelope.get("taskId") == task_id:
                check(envelope.get("messageId") == message_id, "ORIGINAL_ID_MISMATCH")
                originals.append((sequence, message["subject"]))
        check(len(originals) == expected_copies, "ORIGINAL_BROKER_MESSAGE_NOT_UNIQUE")
        sequence, route = originals[-1]
        deadline = time.monotonic() + 15
        while time.monotonic() < deadline:
            listing = await query("CONSUMER.LIST.A2A_MQ")
            check(listing["total"] == len(listing.get("consumers", [])), "CONSUMER_LIST_TRUNCATED")
            consumer = acknowledged_consumer(listing["consumers"], route, sequence)
            if consumer is not None:
                return {"observedAt": time.time(), "messageId": message_id, "taskId": task_id,
                        "streamSequence": sequence, "consumer": consumer["name"],
                        "ackFloorStreamSequence": consumer["ack_floor"]["stream_seq"],
                        "ackPending": consumer["num_ack_pending"], "dedupWindowSeconds": window}
            await asyncio.sleep(.1)
        raise RuntimeError("ORIGINAL_BROKER_ACK_TIMEOUT")
    finally:
        await nc.close()


async def official_client(token):
    from a2a.client.client import ClientConfig
    from a2a.client.client_factory import ClientFactory
    from a2a.types import a2a_pb2 as pb
    from google.protobuf.json_format import ParseDict

    headers = {"Authorization": "Bearer " + token, "A2A-Version": "1.0"}
    async with httpx.AsyncClient(base_url=BASE, headers=headers, timeout=10, trust_env=False) as http:
        response = await http.get("/.well-known/agent-card.json")
        response.raise_for_status()
        card = ParseDict(response.json(), pb.AgentCard())
        client = ClientFactory(ClientConfig(streaming=False, polling=True, httpx_client=http,
                                            supported_protocol_bindings=["JSONRPC"],
                                            use_client_preference=True)).create(card)
        events = [event async for event in client.send_message(ParseDict(request("portfolio-sdk"),
                                                                         pb.SendMessageRequest()))]
        check(bool(events), "SDK_SEND_EMPTY")
        task_id = events[0].task.id
        for _ in range(200):
            task = await client.get_task(pb.GetTaskRequest(id=task_id))
            if task.status.state == pb.TASK_STATE_COMPLETED:
                return {"status": "PASS", "transport": "JSONRPC", "operations": ["SendMessage", "GetTask"]}
            await asyncio.sleep(0.05)
    raise RuntimeError("SDK_COMPLETION_TIMEOUT")


def demo(profile, tools, sample_count):
    prepare(profile)
    # Only this freshly generated fixture uses a shorter acknowledgement wait.
    component = profile / "components/bus.yaml"
    text = component.read_text()
    check("ackWait\n      value: 60s" in text, "ACK_WAIT_FIXTURE_DRIFT")
    component.write_text(text.replace("ackWait\n      value: 60s", "ackWait\n      value: 4s"))
    stack = OwnedStack(profile, tools)
    result = {"status": "FAIL", "scope": "Local-lite synthetic Uppercase over real Dapr/JetStream",
              "scenarios": {}, "externalExactlyOnce": False}
    try:
        stack.start(fault=True)
        token = (profile / "client.jwt").read_text().strip()
        headers = {"Authorization": "Bearer " + token, "A2A-Version": "1.0"}
        with httpx.Client(base_url=BASE, headers=headers, timeout=10, trust_env=False) as client:
            task_id = rpc(client, "SendMessage", request("portfolio-recovery"))["task"]["id"]
            check(stack.process.wait(timeout=15) == -signal.SIGKILL, "EXPECTED_INJECTED_SIGKILL")
            print("Observed: process killed after broker admission, before acknowledgement", flush=True)
            stack.start()
            task = completed(client, task_id)
            check(task["artifacts"][0]["parts"][0]["text"] == "SYNTHETIC INVOICE AWAITING REVIEW",
                  "RESULT_MISMATCH")
            duplicate = rpc(client, "SendMessage", request("portfolio-recovery"))["task"]
            check(duplicate["id"] == task_id, "DUPLICATE_CREATED_TASK")
            conflict = client.post("/rpc", json={"jsonrpc": "2.0", "id": "conflict", "method": "SendMessage",
                                                 "params": request("portfolio-recovery", "changed")})
            check(conflict.status_code == 200 and conflict.json().get("error", {}).get("code") == -32602,
                  "CONFLICT_NOT_REJECTED")
            deadline = time.monotonic() + 15
            deliveries = 0
            while deliveries < 2 and time.monotonic() < deadline:
                rows = [json.loads(line) for line in (profile / "fault-events.jsonl").read_text().splitlines()]
                deliveries = sum(row.get("event") == "broker_delivery" and row.get("taskId") == task_id
                                 for row in rows)
                time.sleep(0.1)
            check(deliveries >= 2, "BROKER_REDELIVERY_NOT_OBSERVED")
            original_deliveries = physical_deliveries(profile, task_id)
            message_ids = {row["messageId"] for row in original_deliveries}
            check(len(message_ids) == 1, "ORIGINAL_DELIVERY_ID_NOT_UNIQUE")
            message_id = message_ids.pop()
            ack = asyncio.run(original_ack_checkpoint(profile, task_id, message_id))
            ack_observed_monotonic = time.monotonic()
            check(sql(profile, "SELECT COUNT(*) FROM a2amq_runtime_end WHERE task_id=?", (task_id,))[0][0] == 1,
                  "TERMINAL_RECORD_NOT_UNIQUE")
            result["scenarios"]["crashDuplicateConflict"] = {
                "status": "PASS", "physicalDeliveries": deliveries, "terminalRecords": 1,
                "conflictErrorCode": -32602, "faultBoundary": "after admission before Dapr ACK"}
            print("PASS: real redelivery, one terminal record, duplicate reuse and content-conflict rejection", flush=True)
            client.post("/lab/control/agent", json={"paused": True}).raise_for_status()
            queued = rpc(client, "SendMessage", request("portfolio-cancel"))["task"]["id"]
            canceled = rpc(client, "CancelTask", {"id": queued})
            check(canceled["status"]["state"] == "TASK_STATE_CANCELED", "CANCEL_FAILED")
            client.post("/lab/control/agent", json={"paused": False}).raise_for_status()
            result["scenarios"]["pauseCancel"] = {"status": "PASS"}
            client.post("/lab/control/deadletter/" + task_id).raise_for_status()
            deadline = time.monotonic() + 15
            records = []
            while not records and time.monotonic() < deadline:
                response = client.get("/operations/deadletters")
                response.raise_for_status()
                records = response.json()["records"]
                time.sleep(0.1)
            check(bool(records), "DLQ_ARCHIVE_TIMEOUT")
            # The real broker reports its effective window. Wait past it without
            # changing stream config, replacing IDs, or forging ledger records.
            remaining = ack["dedupWindowSeconds"] + 1 - (time.monotonic() - ack_observed_monotonic)
            if remaining > 0:
                print(f"Waiting {remaining:.1f}s past the broker's real deduplication window", flush=True)
                time.sleep(remaining)
            from a2a_mq.deadletters import original
            archived = sql(profile, "SELECT record_id,wire FROM a2amq_delivery_dlq")
            matching = [record_id for record_id, wire in archived
                        if original(json.loads(wire)).get("id") == message_id
                        and original(json.loads(wire)).get("data", {}).get("taskId") == task_id]
            check(len(matching) == 1, "REPLAY_ARCHIVE_ID_NOT_UNIQUE")
            checkpoint = time.time()
            before_replay = len(physical_deliveries(profile, task_id, message_id))
            replay = client.post("/operations/deadletters/" + matching[0] + "/replay",
                                 json={"requestId": "portfolio-replay"})
            check(replay.status_code == 202, "REPLAY_NOT_ACCEPTED")
            replay_id = replay.json()["replayId"]
            deadline = time.monotonic() + 15
            while time.monotonic() < deadline:
                rows = sql(profile, "SELECT published_at FROM a2amq_delivery_replays WHERE replay_id=?", (replay_id,))
                if rows and rows[0][0] is not None:
                    break
                time.sleep(0.1)
            check(bool(rows) and rows[0][0] is not None, "REPLAY_PUBLISH_TIMEOUT")
            deadline = time.monotonic() + 15
            receipts = []
            while time.monotonic() < deadline:
                receipts = physical_deliveries(profile, task_id, message_id, after=checkpoint)
                if receipts:
                    break
                time.sleep(.1)
            check(bool(receipts), "REPLAY_PHYSICAL_RECEIPT_TIMEOUT")
            check(len(physical_deliveries(profile, task_id, message_id)) > before_replay,
                  "REPLAY_DELIVERY_COUNT_NOT_INCREASED")
            replay_ack = asyncio.run(original_ack_checkpoint(profile, task_id, message_id, expected_copies=2))
            check(replay_ack["streamSequence"] > ack["streamSequence"], "REPLAY_BROKER_SEQUENCE_NOT_NEW")
            check(completed(client, task_id)["artifacts"] == task["artifacts"], "REPLAY_CHANGED_RESULT")
            check(sql(profile, "SELECT COUNT(*) FROM a2amq_runtime_end WHERE task_id=?", (task_id,))[0][0] == 1,
                  "REPLAY_DUPLICATED_TERMINAL_RECORD")
            result["scenarios"]["demoDlqReplay"] = {
                "status": "PASS", "injection": "Queue Lab explicit quarantine through real Dapr DLQ",
                "replayPublished": True, "terminalSnapshotUnchangedAfterPublication": True,
                "consumerReplayEvidence": "PHYSICAL_RECEIPT_VERIFIED",
                "originalBrokerAck": ack, "observationCheckpoint": checkpoint,
                "replayBrokerAck": replay_ack,
                "replayRequestedAfterDedupSeconds": time.monotonic() - ack_observed_monotonic,
                "physicalDeliveriesBeforeReplay": before_replay,
                "replayReceipt": {key: receipts[0][key] for key in ("timestamp", "messageId", "taskId")},
                "terminalRecordsAfterReplay": 1, "terminalGuardBranch": "NOT_VERIFIED"}
            result["scenarios"]["officialSdkClient"] = asyncio.run(official_client(token))
            samples = []
            for index in range(sample_count):
                started = time.perf_counter()
                current = rpc(client, "SendMessage", request(f"portfolio-bench-{index}"))["task"]["id"]
                completed(client, current)
                samples.append((time.perf_counter() - started) * 1000)
            result["measurement"] = {
                "scope": "HTTP admission to observed completion, 50 ms polling, warm sequential synthetic tasks",
                "samples": len(samples), "concurrency": 1, "payloadTextBytes": len(request("x")["message"]["parts"][0]["text"].encode()),
                "initialPendingActions": "not isolated from earlier demo scenarios",
                "milliseconds": {"min": min(samples), "median": statistics.median(samples),
                                 "p95NearestRank": percentile(samples, .95), "max": max(samples)},
                "capacityBenchmark": False, "sla": False,
            }
            result["status"] = "PASS"
            print("PASS: pause/cancel, identity-preserving DLQ replay receipt/ACK, official SDK send/get; measurements recorded", flush=True)
    finally:
        stack.stop()
    return result


def stop_signal(signum, frame):
    raise KeyboardInterrupt


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--work-dir", type=Path, required=True, help="must not already exist; contains private fixture keys")
    parser.add_argument("--tools-bin", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True, help="new sanitized JSON report")
    parser.add_argument("--samples", type=int, default=20)
    args = parser.parse_args()
    if not 1 <= args.samples <= 100:
        parser.error("--samples must be 1..100")
    if args.work_dir.exists() or args.output.exists():
        parser.error("work directory and report must be new; existing files are never overwritten")
    tools = args.tools_bin.resolve()
    if not all((tools / name).is_file() and os.access(tools / name, os.X_OK)
               for name in ("daprd", "nats-server")):
        parser.error("--tools-bin must contain verified daprd and nats-server binaries")
    from check_local_native_versions import _verified_bootstrap
    native = _verified_bootstrap(tools.parent / "manifest.json", str(tools / "daprd"),
                                 str(tools / "nats-server"))
    root = Path(__file__).resolve().parents[1]
    report = {"createdAt": datetime.now(timezone.utc).isoformat(), "sourceContentSha256": source_digest(root),
              "environment": {"python": platform.python_version(), "system": platform.system(),
                              "architecture": platform.machine(),
                              "sdk": importlib.metadata.version("a2a-sdk")},
              "status": "FAIL", "nativeBinarySha256": {
                  name: item["binarySha256"] for name, item in native["binaries"].items()}}
    previous_sigterm = signal.signal(signal.SIGTERM, stop_signal)
    try:
        report.update(demo(args.work_dir.resolve(), tools, args.samples))
    except KeyboardInterrupt:
        report["status"] = "CANCELED"
        print("CANCELED: stopped owned processes", file=sys.stderr)
    except Exception as error:
        # Raw exceptions can contain headers or payloads: keep only the type in public evidence.
        report["failureType"] = type(error).__name__
        print("FAIL: " + type(error).__name__ + "; inspect the private local runtime log", file=sys.stderr)
    finally:
        signal.signal(signal.SIGTERM, previous_sigterm)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("x", encoding="utf-8") as stream:
        json.dump(report, stream, indent=2, sort_keys=True)
        stream.write("\n")
    return 0 if report["status"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())

