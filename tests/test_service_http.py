"""Real HTTP and spawned service processes; the sidecar is a local HTTP fixture."""
from contextlib import contextmanager, closing
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path
import socket
import sqlite3
import subprocess
import sys
import threading
import time

import httpx

from a2a_mq.domain import Envelope
from a2a_mq.transport import cloud_event
from test_service import TOKEN, callback
from test_storage import request


@contextmanager
def sidecar_fixture():
    records = []
    status = [503]
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass
        def do_GET(self):
            self.send_response(status[0])
            self.send_header("Content-Length", "0")
            self.end_headers()
        def do_POST(self):
            raw = self.rfile.read(int(self.headers["Content-Length"]))
            code = status[0]
            records.append((raw, code, self.path, self.headers.get("Content-Type")))
            self.send_response(code)
            self.send_header("Content-Length", "0")
            self.end_headers()
    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=lambda: server.serve_forever(poll_interval=0.02), daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}", records, status
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)
        assert not thread.is_alive()


def wait_for(predicate, timeout=15):
    until = time.monotonic() + timeout
    while not predicate():
        if time.monotonic() >= until:
            raise AssertionError("local service condition timed out")
        time.sleep(0.02)


@contextmanager
def service_process(database, sidecar, log, consumer="workers", role="worker"):
    # Pass an already-bound socket; there is no close-and-rebind port race.
    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    sock.listen(128)
    port = sock.getsockname()[1]
    script = '''
import socket, sys, uvicorn
from a2a_mq.service import from_env
sock = socket.socket(fileno=int(sys.argv[1]))
config = uvicorn.Config(from_env(), log_level="warning", access_log=False, lifespan="on")
uvicorn.Server(config).run(sockets=[sock])
'''
    env = {**os.environ, "PYTHONPATH": str(Path(__file__).resolve().parents[1] / "src"),
           "PYTHONDONTWRITEBYTECODE": "1", "A2AMQ_NO_LLM_TOKENS": "1", "APP_API_TOKEN": TOKEN,
           "A2A_SQLITE_PATH": str(database), "A2A_TENANT": "demo", "A2A_CONSUMER": consumer,
           "A2A_ROLE": role, "A2A_TARGET_KIND": "skill", "A2A_TARGET": "echo",
           "DAPR_HTTP_ENDPOINT": sidecar}
    env.pop("A2A_SOURCE", None)
    env.pop("DAPR_API_TOKEN", None)
    process = None
    try:
        with log.open("wb") as output:
            process = subprocess.Popen([sys.executable, "-c", script, str(sock.fileno())],
                                       pass_fds=(sock.fileno(),), env=env, stdout=output,
                                       stderr=subprocess.STDOUT)
        with httpx.Client(base_url=f"http://127.0.0.1:{port}", timeout=2,
                          headers={"dapr-api-token": TOKEN}, trust_env=False) as client:
            def alive():
                assert process.poll() is None, log.read_text()
                try:
                    return client.get("/health/live").status_code == 200
                except httpx.HTTPError:
                    return False
            wait_for(alive)
            yield client
    finally:
        sock.close()
        if process is not None:
            if process.poll() is None:
                process.terminate()
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=5)
                raise AssertionError("service did not drain within the shutdown budget")


def pending_count(database):
    with closing(sqlite3.connect(database)) as db:
        return db.execute("SELECT count(*) FROM a2amq_outbox WHERE published_at IS NULL").fetchone()[0]


def test_real_http_restart_outbox_and_two_observers(tmp_path):
    path = tmp_path / "state.db"
    req = request()
    with sidecar_fixture() as (sidecar, records, status):
        with service_process(path, sidecar, tmp_path / "worker-first.log") as client:
            assert client.post("/events", json=callback(req)).json() == {"status": "SUCCESS"}
            wait_for(lambda: any(code == 503 for _, code, _, _ in records))
            assert pending_count(path) == 1
        # The worker is gone; the outbox survives without an in-memory queue.
        assert pending_count(path) == 1
        status[0] = 204
        with service_process(path, sidecar, tmp_path / "worker-restart.log") as client:
            wait_for(lambda: pending_count(path) == 0)
            assert client.post("/events", json=callback(req)).json() == {"status": "SUCCESS"}
            tasks = client.get("/internal/tasks").json()["tasks"]
            assert len(tasks) == 1 and tasks[0]["status"]["state"] == "TASK_STATE_COMPLETED"
        wire_events = [raw for raw, _, _, _ in records]
        assert len(wire_events) >= 2 and all(raw == wire_events[0] for raw in wire_events)
        event = Envelope.from_dict(json.loads(wire_events[-1])["data"])
        assert json.loads(wire_events[-1]) == cloud_event(event)
        assert all(route == "/v1.0/publish/bus/a2a.v1.demo.event.task" for _, _, route, _ in records)
        assert all(mime == "application/cloudevents+json" for _, _, _, mime in records)
        for name in ("observer-a", "observer-b"):
            with service_process(path, sidecar, tmp_path / f"{name}.log", consumer=name, role="observer") as client:
                assert client.get("/internal/tasks").json()["tasks"] == []
                assert client.post("/events", json=callback(event)).json() == {"status": "SUCCESS"}
                assert client.post("/events", json=callback(event)).json() == {"status": "SUCCESS"}
                assert client.get("/internal/tasks").json()["tasks"] == tasks
        with closing(sqlite3.connect(path)) as db:
            assert db.execute("SELECT count(*) FROM a2amq_effects").fetchone()[0] == 1
            assert db.execute("SELECT count(*) FROM a2amq_tasks").fetchone()[0] == 3
            assert db.execute("SELECT count(*) FROM a2amq_inbox").fetchone()[0] == 3
