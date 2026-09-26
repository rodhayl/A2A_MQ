"""Real loopback HTTP and subprocess CLI tests, not a real Dapr broker."""
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import subprocess
import sys
from threading import Thread


def test_cli_saves_before_publication_and_retries_identical_bytes(tmp_path):
    received = []
    saved = tmp_path / "command.json"
    class Handler(BaseHTTPRequestHandler):
        def do_POST(self):
            assert saved.exists()
            received.append(self.rfile.read(int(self.headers["Content-Length"])))
            self.send_response(204)
            self.end_headers()
        def log_message(self, *args):
            pass
    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        args = [sys.executable, "-m", "a2a_mq", "--dapr-url", f"http://127.0.0.1:{server.server_port}"]
        first = subprocess.run([*args, "--text", "hello", "--save", str(saved)], capture_output=True, text=True, timeout=10)
        assert first.returncode == 0, first.stderr
        second = subprocess.run([*args, "--retry-file", str(saved)], capture_output=True, text=True, timeout=10)
        assert second.returncode == 0, second.stderr
        assert len(received) == 2 and received[0] == received[1]
        assert json.loads(received[0])["data"] == json.loads(saved.read_text())
        collision = subprocess.run([*args, "--text", "new", "--save", str(saved)], capture_output=True, text=True, timeout=10)
        assert collision.returncode != 0 and len(received) == 2
    finally:
        server.shutdown()
        thread.join(timeout=5)
        server.server_close()


def test_cli_requires_saved_identity():
    result = subprocess.run([sys.executable, "-m", "a2a_mq", "--text", "hello"], capture_output=True, text=True, timeout=10)
    assert result.returncode == 2
    assert "requires --save" in result.stderr
