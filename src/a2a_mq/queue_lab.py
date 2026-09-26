"""Local browser-based Queue Lab for manual A2A-MQ testing.

The lab is intentionally a thin client over the real A2A and operations HTTP
surfaces. It never receives server-side credentials and does not bypass normal JWT
authorization. Local profiles are loopback-only, so the page can be served publicly
while every state-changing/query operation still uses the user-supplied bearer token.
"""
from __future__ import annotations

from starlette.applications import Starlette
from starlette.responses import HTMLResponse, JSONResponse, Response
from starlette.routing import Mount, Route

from .domain import PermanentError, RetryableError, require
from .http_boundary import BoundedJSON
from .identity import BearerIdentity, current_principal


LAB_HTML = """<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width,initial-scale=1">
  <title>A2A-MQ Queue Lab</title>
  <link rel="stylesheet" href="/lab/app.css">
</head>
<body>
  <header class="topbar">
    <div>
      <div class="eyebrow">A2A-MQ</div>
      <h1>Queue Lab</h1>
      <p>Manual, model-free testing over the real local A2A-MQ runtime.</p>
    </div>
    <div class="top-actions">
      <span id="profile-badge" class="badge">loading…</span>
      <span id="health-live" class="status neutral">live</span>
      <span id="health-ready" class="status neutral">ready</span>
      <button id="toggle-agent" class="ghost" hidden>Pause simulated agent</button>
    </div>
  </header>

  <main class="layout">
    <section class="card auth-card">
      <div class="card-title">
        <div>
          <span class="kicker">Connection</span>
          <h2>Local client JWT</h2>
        </div>
        <button id="clear-token" class="ghost">Clear</button>
      </div>
      <div class="auth-row">
        <input id="token" type="password" autocomplete="off" spellcheck="false"
               placeholder="Paste client.jwt contents">
        <label class="file-button">
          Load file
          <input id="token-file" type="file" accept=".jwt,text/plain">
        </label>
        <button id="connect">Connect</button>
      </div>
      <p class="hint">The token stays in this browser tab (sessionStorage) and is sent only as a Bearer credential to authenticated A2A-MQ endpoints and local Lab controls. Queue Lab never writes it to server-side storage.</p>
      <div id="connection-info" class="connection-info"></div>
    </section>

    <section class="metric-grid">
      <article class="metric"><span>Active tasks</span><strong id="m-active">–</strong></article>
      <article class="metric"><span>Queued / submitted</span><strong id="m-submitted">–</strong></article>
      <article class="metric"><span>Task records</span><strong id="m-records">–</strong></article>
      <article class="metric"><span>Pending actions</span><strong id="m-actions">–</strong></article>
      <article class="metric"><span>Pending publications</span><strong id="m-publications">–</strong></article>
      <article class="metric"><span>Oldest publication</span><strong id="m-oldest">–</strong></article>
      <article class="metric"><span>DLQ</span><strong id="m-dlq">–</strong></article>
    </section>

    <section class="card composer">
      <div class="card-title">
        <div>
          <span class="kicker">Producer</span>
          <h2>Enqueue a task</h2>
        </div>
        <div class="route" id="route-label">agent / …</div>
      </div>

      <div class="form-grid">
        <label>
          Scenario
          <select id="preset">
            <option value="echo">Echo</option>
            <option value="upper">Uppercase</option>
            <option value="burst">Burst / queue view</option>
            <option value="input">Workflow: input required</option>
            <option value="auth">Workflow: approval required</option>
            <option value="delay">Workflow: delayed task</option>
          </select>
        </label>
        <label>
          Simulated agent behavior
          <select id="transform">
            <option value="echo">Echo</option>
            <option value="upper">Uppercase</option>
          </select>
        </label>
        <label>
          Batch count
          <input id="batch" type="number" min="1" max="25" value="1">
        </label>
        <label>
          Return immediately
          <select id="return-immediately">
            <option value="true">Yes — show queued state</option>
            <option value="false">No — wait for task state</option>
          </select>
        </label>
      </div>

      <label>
        Message
        <textarea id="message" rows="4">Hello A2A-MQ!</textarea>
      </label>

      <div class="form-grid">
        <label>
          Message ID
          <input id="message-id" placeholder="auto-generated">
        </label>
        <label>
          Context ID
          <input id="context-id" placeholder="optional">
        </label>
        <label class="workflow-field">
          Delay seconds
          <input id="delay" type="number" min="0" max="86400" step="0.1" value="0">
        </label>
        <label class="check workflow-field">
          <input id="input-required" type="checkbox">
          Require input
        </label>
        <label class="check workflow-field">
          <input id="auth-required" type="checkbox">
          Require approval
        </label>
      </div>

      <details>
        <summary>Advanced metadata</summary>
        <label>
          Additional A2A metadata JSON
          <textarea id="metadata" rows="3">{}</textarea>
        </label>
      </details>

      <div id="workflow-note" class="notice" hidden></div>

      <div class="actions">
        <button id="send">Enqueue</button>
        <button id="send-and-follow" class="secondary">Enqueue + follow</button>
        <button id="send-stream" class="secondary">Enqueue + A2A stream</button>
        <button id="stop-stream" class="ghost" disabled>Stop stream</button>
        <button id="refresh" class="ghost">Refresh queue</button>
      </div>
    </section>

    <section class="card queue-card">
      <div class="card-title">
        <div>
          <span class="kicker">Consumer view</span>
          <h2>Tasks</h2>
        </div>
        <div class="toolbar">
          <select id="state-filter">
            <option value="">All states</option>
            <option>SUBMITTED</option>
            <option>WORKING</option>
            <option>INPUT_REQUIRED</option>
            <option>AUTH_REQUIRED</option>
            <option>COMPLETED</option>
            <option>CANCELED</option>
            <option>FAILED</option>
            <option>REJECTED</option>
          </select>
          <label class="check compact"><input id="auto-refresh" type="checkbox" checked> auto</label>
        </div>
      </div>
      <div class="table-wrap">
        <table>
          <thead>
            <tr><th>State</th><th>Task</th><th>Context</th><th>Result</th><th>Actions</th></tr>
          </thead>
          <tbody id="tasks-body"></tbody>
        </table>
      </div>
    </section>

    <section class="card detail-card">
      <div class="card-title">
        <div>
          <span class="kicker">Inspector</span>
          <h2>Selected task</h2>
        </div>
        <span id="selected-state" class="badge muted">none</span>
      </div>
      <div class="task-buttons">
        <input id="continuation-text" class="continuation-input" value="continue"
               placeholder="Continuation input" disabled>
        <button id="task-follow" class="secondary" disabled>Follow</button>
        <button id="task-subscribe" class="secondary" disabled>Subscribe (SSE)</button>
        <button id="task-continue" class="ghost" disabled>Continue input</button>
        <button id="task-approve" class="ghost" disabled>Approve</button>
        <button id="task-reject" class="ghost danger" disabled>Reject</button>
        <button id="task-cancel" class="ghost danger" disabled>Cancel</button>
      </div>
      <pre id="task-json" class="json-view">Select a task.</pre>
    </section>

    <section class="card dlq-card">
      <div class="card-title">
        <div>
          <span class="kicker">Recovery</span>
          <h2>Dead letters</h2>
        </div>
        <button id="refresh-dlq" class="ghost">Refresh</button>
      </div>
      <div id="dlq-list" class="stack-list"><span class="hint">No data loaded.</span></div>
    </section>

    <section class="card raw-card">
      <div class="card-title">
        <div>
          <span class="kicker">Advanced</span>
          <h2>Raw JSON-RPC console</h2>
        </div>
        <select id="raw-preset">
          <option value="list">ListTasks</option>
          <option value="get">GetTask</option>
          <option value="cancel">CancelTask</option>
          <option value="subscribe">SubscribeToTask</option>
        </select>
      </div>
      <textarea id="raw-request" rows="9" spellcheck="false"></textarea>
      <div class="actions">
        <button id="run-raw">Run request</button>
      </div>
      <pre id="raw-response" class="json-view">Ready.</pre>
    </section>

    <section class="card log-card">
      <div class="card-title">
        <div>
          <span class="kicker">Live trace</span>
          <h2>Activity</h2>
        </div>
        <button id="clear-log" class="ghost">Clear</button>
      </div>
      <div id="activity-log" class="activity"></div>
    </section>
  </main>

  <footer>
    Queue Lab uses the real A2A JSON-RPC, operations API, Dapr Pub/Sub and SQLite state.
    It is a manual test console, not a second queue implementation.
  </footer>
  <script src="/lab/app.js"></script>
</body>
</html>
"""


LAB_CSS = """
:root {
  color-scheme: dark;
  --bg: #090d16;
  --panel: #111827;
  --panel2: #0d1422;
  --line: #243044;
  --text: #e7edf8;
  --muted: #8fa0b8;
  --accent: #55e6a5;
  --accent2: #55a7ff;
  --warn: #ffcc66;
  --bad: #ff718a;
  --shadow: 0 18px 50px rgba(0,0,0,.28);
}
* { box-sizing: border-box; }
body {
  margin: 0;
  min-height: 100vh;
  background:
    radial-gradient(circle at 10% -10%, rgba(85,167,255,.14), transparent 28rem),
    radial-gradient(circle at 90% 0%, rgba(85,230,165,.11), transparent 30rem),
    var(--bg);
  color: var(--text);
  font: 14px/1.45 Inter, ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
}
button, input, textarea, select { font: inherit; }
button {
  border: 0;
  border-radius: 10px;
  background: var(--accent);
  color: #05120c;
  font-weight: 700;
  padding: 10px 14px;
  cursor: pointer;
}
button:hover { filter: brightness(1.08); }
button:disabled { opacity: .42; cursor: default; filter: none; }
button.secondary { background: var(--accent2); color: #06101e; }
button.ghost { background: #172235; color: var(--text); border: 1px solid var(--line); }
button.danger { color: #ff9bac; }
.topbar {
  max-width: 1500px;
  margin: 0 auto;
  padding: 34px 28px 18px;
  display: flex;
  align-items: flex-end;
  justify-content: space-between;
  gap: 20px;
}
.eyebrow, .kicker {
  text-transform: uppercase;
  letter-spacing: .16em;
  font-size: 11px;
  font-weight: 800;
  color: var(--accent);
}
h1 { margin: 3px 0; font-size: clamp(30px, 5vw, 52px); line-height: 1; }
h2 { margin: 2px 0 0; font-size: 19px; }
p { color: var(--muted); }
.top-actions { display: flex; gap: 8px; align-items: center; flex-wrap: wrap; justify-content: flex-end; }
.badge, .status {
  display: inline-flex;
  align-items: center;
  min-height: 28px;
  padding: 4px 9px;
  border-radius: 999px;
  background: #172235;
  border: 1px solid var(--line);
  font-size: 12px;
  font-weight: 700;
}
.status.good { color: var(--accent); border-color: rgba(85,230,165,.4); }
.status.bad { color: var(--bad); border-color: rgba(255,113,138,.45); }
.status.neutral { color: var(--muted); }
.badge.muted { color: var(--muted); }
.layout {
  max-width: 1500px;
  margin: 0 auto;
  padding: 12px 28px 44px;
  display: grid;
  grid-template-columns: repeat(12, 1fr);
  gap: 16px;
}
.card, .metric {
  background: linear-gradient(180deg, rgba(17,24,39,.96), rgba(13,20,34,.96));
  border: 1px solid var(--line);
  border-radius: 16px;
  box-shadow: var(--shadow);
}
.card { padding: 18px; }
.auth-card, .composer, .queue-card, .raw-card, .log-card { grid-column: span 12; }
.detail-card, .dlq-card { grid-column: span 6; }
.metric-grid {
  grid-column: span 12;
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(140px, 1fr));
  gap: 10px;
}
.metric { padding: 14px; box-shadow: none; }
.metric span { display: block; color: var(--muted); font-size: 12px; margin-bottom: 5px; }
.metric strong { font-size: 24px; }
.card-title, .toolbar, .actions, .auth-row, .task-buttons {
  display: flex;
  gap: 10px;
  align-items: center;
}
.card-title { justify-content: space-between; margin-bottom: 16px; }
.auth-row { align-items: stretch; }
.auth-row input[type=password] { flex: 1; min-width: 180px; }
.file-button {
  display: inline-flex;
  align-items: center;
  padding: 10px 13px;
  border: 1px solid var(--line);
  border-radius: 10px;
  background: #172235;
  cursor: pointer;
}
.file-button input { display: none; }
input, textarea, select {
  width: 100%;
  border: 1px solid var(--line);
  border-radius: 10px;
  background: #090f1b;
  color: var(--text);
  padding: 10px 11px;
  outline: none;
}
input:focus, textarea:focus, select:focus { border-color: var(--accent2); }
textarea { resize: vertical; }
label { color: var(--muted); display: grid; gap: 6px; }
.check { display: flex; flex-direction: row; align-items: center; gap: 8px; color: var(--text); }
.check input { width: auto; }
.check.compact { font-size: 12px; }
.form-grid {
  display: grid;
  grid-template-columns: repeat(5, minmax(130px, 1fr));
  gap: 12px;
  margin-bottom: 12px;
}
.hint { font-size: 12px; margin: 8px 0 0; }
.connection-info { margin-top: 10px; color: var(--muted); font-size: 12px; }
.route { color: var(--accent2); font-family: ui-monospace, SFMono-Regular, Menlo, monospace; font-size: 12px; }
.notice {
  margin-top: 12px;
  border: 1px solid rgba(255,204,102,.35);
  background: rgba(255,204,102,.07);
  color: var(--warn);
  border-radius: 10px;
  padding: 10px 12px;
}
.notice[hidden] { display: none; }
.workflow-field.disabled-feature { opacity: .48; }
.actions { margin-top: 14px; flex-wrap: wrap; }
details { margin-top: 12px; }
summary { cursor: pointer; color: var(--muted); margin-bottom: 9px; }
.table-wrap { overflow: auto; }
table { width: 100%; border-collapse: collapse; min-width: 850px; }
th, td { text-align: left; padding: 10px 8px; border-bottom: 1px solid #202b3d; vertical-align: top; }
th { color: var(--muted); font-size: 11px; text-transform: uppercase; letter-spacing: .08em; }
td code { color: #b8c9df; }
.task-actions { display: flex; gap: 6px; flex-wrap: wrap; }
.continuation-input { width: min(320px, 100%); }
.task-actions button { padding: 6px 8px; font-size: 11px; }
.state-pill {
  display: inline-flex;
  border-radius: 999px;
  padding: 4px 8px;
  background: #182338;
  border: 1px solid var(--line);
  font-size: 11px;
  font-weight: 800;
}
.state-COMPLETED { color: var(--accent); }
.state-FAILED, .state-REJECTED { color: var(--bad); }
.state-INPUT_REQUIRED, .state-AUTH_REQUIRED { color: var(--warn); }
.state-WORKING { color: var(--accent2); }
.json-view {
  background: #070c14;
  border: 1px solid #202b3d;
  border-radius: 12px;
  padding: 13px;
  margin: 12px 0 0;
  max-height: 430px;
  overflow: auto;
  color: #c9d7e9;
  font: 12px/1.5 ui-monospace, SFMono-Regular, Menlo, Consolas, monospace;
  white-space: pre-wrap;
  word-break: break-word;
}
.activity {
  display: flex;
  flex-direction: column-reverse;
  max-height: 360px;
  overflow: auto;
  gap: 7px;
}
.log-item {
  padding: 9px 10px;
  border-radius: 9px;
  background: #0a111d;
  border-left: 3px solid var(--accent2);
  font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace;
  font-size: 12px;
}
.log-item.error { border-left-color: var(--bad); color: #ffbdc8; }
.log-item.ok { border-left-color: var(--accent); }
.stack-list { display: grid; gap: 8px; }
.stack-item {
  background: #0a111d;
  border: 1px solid #202b3d;
  border-radius: 10px;
  padding: 10px;
  display: flex;
  justify-content: space-between;
  gap: 10px;
  align-items: center;
}
footer {
  max-width: 1500px;
  margin: 0 auto;
  padding: 0 28px 30px;
  color: var(--muted);
  font-size: 12px;
}
@media (max-width: 1000px) {
  .metric-grid { grid-template-columns: repeat(3, 1fr); }
  .detail-card, .dlq-card { grid-column: span 12; }
  .form-grid { grid-template-columns: repeat(2, 1fr); }
}
@media (max-width: 650px) {
  .topbar { align-items: flex-start; flex-direction: column; padding: 24px 16px 12px; }
  .layout { padding: 10px 16px 32px; }
  .metric-grid { grid-template-columns: repeat(2, 1fr); }
  .form-grid { grid-template-columns: 1fr; }
  .auth-row { flex-direction: column; }
  .task-buttons { flex-wrap: wrap; }
}
"""


LAB_JS = r"""
(function () {
  "use strict";

  const $ = function (selector) { return document.querySelector(selector); };
  const state = {
    token: sessionStorage.getItem("a2amq.queueLab.token") || "",
    authenticated: false,
    sessionEpoch: 0,
    config: null,
    tasks: [],
    selected: null,
    autoTimer: null,
    autoRefreshing: false,
    followTimers: new Map(),
    streamController: null
  };
  const terminal = new Set(["COMPLETED", "FAILED", "CANCELED", "REJECTED"]);
  const interrupted = new Set(["INPUT_REQUIRED", "AUTH_REQUIRED"]);

  function uid(prefix) {
    const value = (globalThis.crypto && crypto.randomUUID) ?
      crypto.randomUUID() :
      String(Date.now()) + "-" + Math.random().toString(16).slice(2);
    return (prefix || "lab") + "-" + value.toLowerCase();
  }

  function now() {
    return new Date().toLocaleTimeString();
  }

  function log(message, kind, data) {
    const item = document.createElement("div");
    item.className = "log-item " + (kind || "");
    item.textContent = "[" + now() + "] " + message +
      (data === undefined ? "" : "  " + JSON.stringify(data));
    $("#activity-log").prepend(item);
  }

  function token() {
    return ($("#token").value || "").trim();
  }

  function validateToken(value) {
    if (!value) throw new Error("Load a client JWT first.");
    if (value.length > 16384 || !/^[A-Za-z0-9._-]+$/.test(value)) {
      throw new Error("JWT has an invalid shape or exceeds the 16 KiB server limit.");
    }
    return value;
  }

  function authHeaders(extra) {
    const headers = Object.assign({
      "Authorization": "Bearer " + validateToken(token()),
      "A2A-Version": "1.0"
    }, extra || {});
    return headers;
  }

  async function fetchJSON(path, options, authenticated) {
    const opts = Object.assign({}, options || {});
    opts.headers = Object.assign({}, opts.headers || {});
    if (authenticated !== false) {
      if (!token()) throw new Error("Load a client JWT first.");
      Object.assign(opts.headers, authHeaders());
    }
    const response = await fetch(path, opts);
    const text = await response.text();
    let value = {};
    if (text) {
      try { value = JSON.parse(text); }
      catch (_) { value = {raw: text}; }
    }
    if (!response.ok) {
      if (authenticated !== false && response.status === 401) {
        state.authenticated = false;
        state.sessionEpoch += 1;
      }
      const detail = value.error || value.detail || value;
      const suffix = response.status === 401 && authenticated !== false ?
        " — reload the current client.jwt and reconnect" : "";
      throw new Error(response.status + " " + JSON.stringify(detail) + suffix);
    }
    return value;
  }

  async function rpc(method, params) {
    const body = {
      jsonrpc: "2.0",
      id: uid("rpc"),
      method: method,
      params: params || {}
    };
    const value = await fetchJSON("/rpc", {
      method: "POST",
      headers: {"Content-Type": "application/json"},
      body: JSON.stringify(body)
    });
    if (value.error) throw new Error(JSON.stringify(value.error));
    return value.result;
  }

  function taskFromResult(value) {
    return value && value.task ? value.task : value;
  }

  function stateName(task) {
    const raw = task && task.status && task.status.state ? task.status.state : "UNKNOWN";
    return raw.replace(/^TASK_STATE_/, "");
  }

  function resultText(task) {
    const artifacts = task && Array.isArray(task.artifacts) ? task.artifacts : [];
    const values = [];
    artifacts.forEach(function (artifact) {
      (artifact.parts || []).forEach(function (part) {
        if (typeof part.text === "string") values.push(part.text);
      });
    });
    return values.join(" · ");
  }

  function short(value, size) {
    const text = value || "";
    return text.length > size ? text.slice(0, size) + "…" : text;
  }

  async function loadConfig() {
    state.config = await fetchJSON("/lab/config", {}, false);
    $("#profile-badge").textContent = state.config.profile;
    $("#route-label").textContent = "agent / " + state.config.source;
    $("#connection-info").textContent =
      "tenant=" + state.config.tenant +
      " · command=" + state.config.commandRoute +
      " · event=" + state.config.eventRoute;

    const pauseButton = $("#toggle-agent");
    if (state.config.features.agentPause) {
      pauseButton.hidden = false;
      refreshAgentControl();
    } else {
      pauseButton.hidden = true;
    }

    const workflow = !!state.config.features.workflow;
    document.querySelectorAll(".workflow-field").forEach(function (node) {
      node.querySelectorAll("input,select").forEach(function (input) {
        input.disabled = !workflow;
      });
      node.classList.toggle("disabled-feature", !workflow);
    });
    const note = $("#workflow-note");
    note.hidden = workflow;
    if (!workflow) {
      note.textContent =
        "Local-lite executes immediate echo/upper tasks only. Input, approval and delay controls are real Workflow features; start a2amq-local-workflow to enable them.";
    }
  }

  async function refreshAgentControl() {
    if (!state.config || !state.config.features.agentPause || !state.authenticated) return;
    try {
      const value = await fetchJSON("/lab/control/agent");
      const button = $("#toggle-agent");
      button.dataset.paused = value.paused ? "true" : "false";
      button.textContent = value.paused ? "Resume simulated agent" : "Pause simulated agent";
      button.className = value.paused ? "secondary" : "ghost";
    } catch (error) {
      log("Agent control refresh failed: " + error.message, "error");
    }
  }

  function requireConnected() {
    if (state.authenticated) return true;
    log("Connect with a valid current client.jwt first.", "error");
    return false;
  }

  async function toggleAgent() {
    if (!requireConnected()) return;
    const button = $("#toggle-agent");
    const paused = button.dataset.paused !== "true";
    try {
      const value = await fetchJSON("/lab/control/agent", {
        method: "POST",
        headers: {"Content-Type": "application/json"},
        body: JSON.stringify({paused: paused})
      });
      button.dataset.paused = value.paused ? "true" : "false";
      button.textContent = value.paused ? "Resume simulated agent" : "Pause simulated agent";
      button.className = value.paused ? "secondary" : "ghost";
      log(value.paused ?
        "Simulated agent unavailable; short manual pauses hold delivery. Normal timeout/redelivery/DLQ policy still applies." :
        "Simulated agent resumed; queued/submitted tasks can drain.", "ok");
      await refreshAll();
    } catch (error) {
      log("Agent control failed: " + error.message, "error");
    }
  }

  function setHealth(id, ok, text) {
    const node = $(id);
    node.className = "status " + (ok ? "good" : "bad");
    node.textContent = text;
  }

  async function refreshHealth() {
    try {
      const live = await fetchJSON("/health/live", {}, false);
      setHealth("#health-live", live.status === "alive", "live: " + live.status);
    } catch (error) {
      setHealth("#health-live", false, "live: down");
    }
    try {
      const ready = await fetchJSON("/health/ready", {}, false);
      setHealth("#health-ready", ready.status === "ready", "ready: " + ready.status);
    } catch (error) {
      setHealth("#health-ready", false, "ready: down");
    }
  }

  async function refreshMetrics() {
    if (!state.authenticated) return;
    const epoch = state.sessionEpoch;
    try {
      const value = await fetchJSON("/operations/status");
      if (!state.authenticated || epoch !== state.sessionEpoch) return;
      $("#m-active").textContent = value.activeTasks ?? "–";
      $("#m-records").textContent = value.taskRecords ?? "–";
      $("#m-actions").textContent = value.pendingActions ?? "–";
      $("#m-publications").textContent = value.pendingPublications ?? "–";
      const age = Number(value.oldestPublicationAgeSeconds || 0);
      $("#m-oldest").textContent = age < 1 ? "0s" : age.toFixed(1) + "s";
    } catch (error) {
      log("operations/status failed: " + error.message, "error");
    }
  }

  async function refreshDLQ() {
    if (!state.authenticated) return;
    const epoch = state.sessionEpoch;
    try {
      const value = await fetchJSON("/operations/deadletters?limit=50");
      if (!state.authenticated || epoch !== state.sessionEpoch) return;
      const items = Array.isArray(value) ? value :
        (value.records || value.items || value.deadletters || []);
      $("#m-dlq").textContent = items.length;
      const root = $("#dlq-list");
      root.innerHTML = "";
      if (!items.length) {
        const empty = document.createElement("span");
        empty.className = "hint";
        empty.textContent = "No dead letters.";
        root.appendChild(empty);
        return;
      }
      items.forEach(function (item) {
        const row = document.createElement("div");
        row.className = "stack-item";
        const id = item.recordId || item.entryId || item.id;
        const label = document.createElement("div");
        label.innerHTML = "<strong></strong><br><span class=\"hint\"></span>";
        label.querySelector("strong").textContent = id || "DLQ item";
        label.querySelector(".hint").textContent = item.reason || item.status || "archived";
        const buttons = document.createElement("div");
        buttons.className = "task-actions";
        const inspect = document.createElement("button");
        inspect.className = "ghost";
        inspect.textContent = "Inspect";
        inspect.disabled = !id;
        inspect.addEventListener("click", function () { inspectDLQ(id); });
        const replay = document.createElement("button");
        replay.className = "ghost";
        replay.textContent = "Replay";
        replay.disabled = !id;
        replay.addEventListener("click", function () { replayDLQ(id); });
        buttons.append(inspect, replay);
        row.append(label, buttons);
        root.appendChild(row);
      });
    } catch (error) {
      $("#m-dlq").textContent = "!";
      log("DLQ refresh failed: " + error.message, "error");
    }
  }

  async function inspectDLQ(id) {
    if (!id) return;
    try {
      const value = await fetchJSON("/operations/deadletters/" + encodeURIComponent(id));
      $("#raw-response").textContent = JSON.stringify(value, null, 2);
      log("DLQ record loaded into Raw response", "ok", {id: id});
    } catch (error) {
      log("DLQ inspect failed: " + error.message, "error");
    }
  }

  async function replayDLQ(id) {
    if (!id) return;
    try {
      const value = await fetchJSON("/operations/deadletters/" + encodeURIComponent(id) + "/replay", {
        method: "POST",
        headers: {"Content-Type": "application/json"},
        body: JSON.stringify({requestId: uid("replay")})
      });
      log("DLQ replay queued", "ok", value);
      await refreshDLQ();
    } catch (error) {
      log("DLQ replay failed: " + error.message, "error");
    }
  }

  async function listTasks() {
    if (!state.authenticated) return;
    const epoch = state.sessionEpoch;
    try {
      const result = await rpc("ListTasks", {pageSize: 100, includeArtifacts: true});
      if (!state.authenticated || epoch !== state.sessionEpoch) return;
      const value = result && result.tasks ? result : (result && result.result ? result.result : {});
      state.tasks = Array.isArray(value.tasks) ? value.tasks : [];
      renderTasks();
    } catch (error) {
      log("ListTasks failed: " + error.message, "error");
    }
  }

  function renderTasks() {
    const body = $("#tasks-body");
    body.innerHTML = "";
    const filter = $("#state-filter").value;
    const submitted = state.tasks.filter(function (task) {
      return stateName(task) === "SUBMITTED";
    }).length;
    $("#m-submitted").textContent = String(submitted);
    const tasks = state.tasks.filter(function (task) {
      return !filter || stateName(task) === filter;
    });
    if (!tasks.length) {
      const row = document.createElement("tr");
      const cell = document.createElement("td");
      cell.colSpan = 5;
      cell.className = "hint";
      cell.textContent = "No matching tasks.";
      row.appendChild(cell);
      body.appendChild(row);
      return;
    }
    tasks.forEach(function (task) {
      const row = document.createElement("tr");
      const st = stateName(task);
      const s = document.createElement("td");
      const pill = document.createElement("span");
      pill.className = "state-pill state-" + st;
      pill.textContent = st;
      s.appendChild(pill);

      const id = document.createElement("td");
      const code = document.createElement("code");
      code.textContent = short(task.id, 28);
      code.title = task.id || "";
      id.appendChild(code);

      const context = document.createElement("td");
      context.textContent = short(task.contextId, 20);

      const result = document.createElement("td");
      result.textContent = short(resultText(task), 48) || "—";

      const actions = document.createElement("td");
      actions.className = "task-actions";
      ["Inspect", "Follow", "Subscribe"].forEach(function (name) {
        const button = document.createElement("button");
        button.className = "ghost";
        button.textContent = name;
        if (name === "Subscribe" && terminal.has(st)) button.disabled = true;
        button.addEventListener("click", function () {
          selectTask(task.id);
          if (name === "Follow") followTask(task.id);
          if (name === "Subscribe") subscribeTask(task.id);
        });
        actions.appendChild(button);
      });

      row.append(s, id, context, result, actions);
      body.appendChild(row);
    });
  }

  async function getTask(id) {
    return taskFromResult(await rpc("GetTask", {id: id, historyLength: 100}));
  }

  async function selectTask(id) {
    const epoch = state.sessionEpoch;
    try {
      const selected = await getTask(id);
      if (!state.authenticated || epoch !== state.sessionEpoch) return;
      state.selected = selected;
      renderSelected();
    } catch (error) {
      log("GetTask failed: " + error.message, "error");
    }
  }

  function renderSelected() {
    const task = state.selected;
    const buttons = ["#continuation-text", "#task-follow", "#task-subscribe", "#task-continue",
      "#task-approve", "#task-reject", "#task-cancel"];
    buttons.forEach(function (id) { $(id).disabled = !task; });
    if (!task) {
      $("#selected-state").textContent = "none";
      $("#task-json").textContent = "Select a task.";
      return;
    }
    const st = stateName(task);
    $("#selected-state").textContent = st;
    $("#task-json").textContent = JSON.stringify(task, null, 2);
    $("#continuation-text").disabled = st !== "INPUT_REQUIRED";
    $("#task-continue").disabled = st !== "INPUT_REQUIRED";
    $("#task-approve").disabled = st !== "AUTH_REQUIRED";
    $("#task-reject").disabled = st !== "AUTH_REQUIRED";
    $("#task-subscribe").disabled = terminal.has(st);
    $("#task-cancel").disabled = terminal.has(st);
  }

  async function refreshAll() {
    await refreshHealth();
    if (state.authenticated) {
      await Promise.all([refreshMetrics(), listTasks(), refreshDLQ(), refreshAgentControl()]);
      if (state.selected) await selectTask(state.selected.id);
    }
  }

  function readMetadata() {
    let extra;
    try {
      extra = JSON.parse($("#metadata").value || "{}");
    } catch (_) {
      throw new Error("Advanced metadata must be valid JSON.");
    }
    if (!extra || Array.isArray(extra) || typeof extra !== "object") {
      throw new Error("Advanced metadata must be a JSON object.");
    }
    if ("a2amq" in extra && (!extra.a2amq || Array.isArray(extra.a2amq)
        || typeof extra.a2amq !== "object")) {
      throw new Error("metadata.a2amq must be a JSON object.");
    }
    const a2amq = Object.assign({}, extra.a2amq || {}, {
      transform: $("#transform").value
    });
    if (state.config.features.workflow) {
      const delay = Number($("#delay").value || 0);
      if (!Number.isFinite(delay) || delay < 0 || delay > 86400) {
        throw new Error("Delay must be a finite number between 0 and 86400 seconds.");
      }
      a2amq.inputRequired = $("#input-required").checked;
      a2amq.authRequired = $("#auth-required").checked;
      a2amq.delaySeconds = delay;
    }
    return Object.assign({}, extra, {a2amq: a2amq});
  }

  function buildRequestParams(index) {
    const base = ($("#message-id").value || "").trim();
    const messageId = base ?
      (index === 0 ? base : base + "-" + (index + 1)) :
      uid("msg");
    const text = $("#message").value;
    if (new TextEncoder().encode(text).length > 16384) {
      throw new Error("Message text exceeds the 16 KiB local workflow input limit.");
    }
    const message = {
      messageId: messageId,
      role: "ROLE_USER",
      parts: [{text: text}]
    };
    const contextId = ($("#context-id").value || "").trim();
    if (contextId) message.contextId = contextId;
    return {
      message: message,
      configuration: {returnImmediately: $("#return-immediately").value === "true"},
      metadata: readMetadata()
    };
  }

  async function sendOne(index) {
    return taskFromResult(await rpc("SendMessage", buildRequestParams(index)));
  }

  async function enqueue(follow) {
    if (!requireConnected()) return;
    const rawCount = Number($("#batch").value || 1);
    if (!Number.isFinite(rawCount) || rawCount < 1) {
      log("Batch count must be a positive integer.", "error");
      return;
    }
    const count = Math.max(1, Math.min(25, Math.trunc(rawCount)));
    if ($("#toggle-agent").dataset.paused === "true"
        && $("#return-immediately").value !== "true") {
      log("Use Return immediately = Yes while the simulated agent is paused.", "error");
      return;
    }
    $("#send").disabled = true;
    $("#send-and-follow").disabled = true;
    $("#send-stream").disabled = true;
    try {
      let last = null;
      for (let i = 0; i < count; i += 1) {
        last = await sendOne(i);
        log("Task admitted " + last.id, "ok", {state: stateName(last)});
      }
      await refreshAll();
      if (last) {
        await selectTask(last.id);
        if (follow) followTask(last.id);
      }
    } catch (error) {
      log("Enqueue failed: " + error.message, "error");
    } finally {
      $("#send").disabled = false;
      $("#send-and-follow").disabled = false;
      $("#send-stream").disabled = false;
    }
  }

  function stopFollow(id) {
    const handle = state.followTimers.get(id);
    if (handle) {
      clearTimeout(handle);
      state.followTimers.delete(id);
    }
  }

  async function followTask(id) {
    stopFollow(id);
    const epoch = state.sessionEpoch;
    let previous = "";
    const tick = async function () {
      if (!state.authenticated || epoch !== state.sessionEpoch) return;
      try {
        const task = await getTask(id);
        if (!state.authenticated || epoch !== state.sessionEpoch) return;
        const wire = JSON.stringify(task);
        if (wire !== previous) {
          previous = wire;
          log("Follow " + id + " → " + stateName(task), "ok");
          if (state.selected && state.selected.id === id) {
            state.selected = task;
            renderSelected();
          }
          await listTasks();
        }
        const st = stateName(task);
        if (terminal.has(st) || interrupted.has(st)) {
          state.followTimers.delete(id);
          return;
        }
        state.followTimers.set(id, setTimeout(tick, 500));
      } catch (error) {
        log("Follow failed: " + error.message, "error");
        state.followTimers.delete(id);
      }
    };
    log("Following task " + id);
    await tick();
  }

  function beginStream(label) {
    if (state.streamController) {
      state.streamController.abort();
      log("Previous SSE stream stopped before opening " + label + ".");
    }
    const controller = new AbortController();
    state.streamController = controller;
    $("#stop-stream").disabled = false;
    return controller;
  }

  function endStream(controller) {
    if (state.streamController === controller) {
      state.streamController = null;
      $("#stop-stream").disabled = true;
    }
  }

  function stopStream() {
    if (!state.streamController) return;
    state.streamController.abort();
    state.streamController = null;
    $("#stop-stream").disabled = true;
    log("SSE stream stopped by user.");
  }

  async function consumeSSE(response, label) {
    const reader = response.body.getReader();
    const decoder = new TextDecoder();
    let buffer = "";
    let dataLines = [];
    let count = 0;

    function flushEvent() {
      if (!dataLines.length) return;
      const raw = dataLines.join("\n");
      dataLines = [];
      try {
        const value = JSON.parse(raw);
        count += 1;
        log(label, "ok", value);
      } catch (_) {
        log(label + " (non-JSON data) " + raw, "error");
      }
    }

    function line(value) {
      if (value === "") {
        flushEvent();
        return;
      }
      if (value.startsWith("data:")) {
        dataLines.push(value.slice(5).replace(/^ /, ""));
      }
    }

    while (true) {
      const next = await reader.read();
      if (next.done) break;
      buffer += decoder.decode(next.value, {stream: true});
      const lines = buffer.split(/\r?\n/);
      buffer = lines.pop() || "";
      lines.forEach(line);
    }
    buffer += decoder.decode();
    if (buffer) line(buffer);
    flushEvent();
    return count;
  }

  async function sendStreaming() {
    if (!requireConnected()) return;
    if ($("#toggle-agent").dataset.paused === "true") {
      log("Resume the simulated agent before SendStreamingMessage, or the stream may wait for normal delivery timeout.", "error");
      return;
    }
    $("#send").disabled = true;
    $("#send-and-follow").disabled = true;
    $("#send-stream").disabled = true;
    const controller = beginStream("SendStreamingMessage");
    try {
      const request = {
        jsonrpc: "2.0",
        id: uid("stream"),
        method: "SendStreamingMessage",
        params: buildRequestParams(0)
      };
      const response = await fetch("/rpc", {
        method: "POST",
        headers: authHeaders({
          "Content-Type": "application/json",
          "Accept": "text/event-stream"
        }),
        body: JSON.stringify(request),
        signal: controller.signal
      });
      const type = response.headers.get("content-type") || "";
      if (response.status === 401) {
        state.authenticated = false;
        state.sessionEpoch += 1;
      }
      if (!response.ok || !response.body || type.indexOf("text/event-stream") === -1) {
        const body = await response.text();
        throw new Error("SendStreamingMessage returned HTTP " + response.status + ": " + body);
      }
      const count = await consumeSSE(response, "SendStreamingMessage event");
      log("SendStreamingMessage stream closed", "ok", {events: count});
      await refreshAll();
    } catch (error) {
      if (error.name === "AbortError") log("SendStreamingMessage stream stopped.");
      else log("SendStreamingMessage failed: " + error.message, "error");
    } finally {
      endStream(controller);
      $("#send").disabled = false;
      $("#send-and-follow").disabled = false;
      $("#send-stream").disabled = false;
    }
  }

  async function subscribeTask(id, allowFallback) {
    if (!requireConnected()) return;
    const fallback = allowFallback !== false;
    log("Opening A2A SubscribeToTask stream for " + id);
    const request = {
      jsonrpc: "2.0",
      id: uid("subscribe"),
      method: "SubscribeToTask",
      params: {id: id}
    };
    const controller = beginStream("SubscribeToTask");
    try {
      const response = await fetch("/rpc", {
        method: "POST",
        headers: authHeaders({
          "Content-Type": "application/json",
          "Accept": "text/event-stream"
        }),
        body: JSON.stringify(request),
        signal: controller.signal
      });
      const type = response.headers.get("content-type") || "";
      if (response.status === 401) {
        state.authenticated = false;
        state.sessionEpoch += 1;
      }
      if (!response.ok || !response.body || type.indexOf("text/event-stream") === -1) {
        const body = await response.text();
        if (!fallback) {
          $("#raw-response").textContent = body || ("HTTP " + response.status);
          log("Raw SubscribeToTask returned a non-stream response.", "error",
              {status: response.status});
          return;
        }
        if (response.status === 401) {
          log("Subscribe authentication expired; reload client.jwt and reconnect.", "error");
          return;
        }
        log("Streaming subscription unavailable; falling back to follow. " + body, "error");
        followTask(id);
        return;
      }
      await consumeSSE(response, "SubscribeToTask event");
      log("Subscription closed for " + id);
      await selectTask(id);
      await listTasks();
    } catch (error) {
      if (error.name === "AbortError") {
        log("SubscribeToTask stream stopped.");
        return;
      }
      if (!fallback) {
        $("#raw-response").textContent = error.message;
        log("Raw SubscribeToTask failed: " + error.message, "error");
        return;
      }
      if (!state.authenticated) {
        log("Subscribe authentication is no longer valid; reconnect before retrying.", "error");
        return;
      }
      log("Subscribe failed; falling back to follow: " + error.message, "error");
      followTask(id);
    } finally {
      endStream(controller);
    }
  }

  async function cancelSelected() {
    if (!state.selected || !requireConnected()) return;
    try {
      const task = taskFromResult(await rpc("CancelTask", {id: state.selected.id}));
      log("Cancel accepted", "ok", {id: task.id, state: stateName(task)});
      state.selected = task;
      renderSelected();
      await refreshAll();
    } catch (error) {
      log("Cancel failed: " + error.message, "error");
    }
  }

  async function continueSelected() {
    if (!state.selected || !requireConnected()) return;
    const text = $("#continuation-text").value;
    if (new TextEncoder().encode(text).length > 16384) {
      log("Continuation text exceeds the 16 KiB input limit.", "error");
      return;
    }
    const task = state.selected;
    const params = {
      message: {
        messageId: uid("input"),
        role: "ROLE_USER",
        taskId: task.id,
        contextId: task.contextId,
        parts: [{text: text}]
      },
      configuration: {returnImmediately: true}
    };
    try {
      const value = taskFromResult(await rpc("SendMessage", params));
      log("Continuation queued", "ok", {id: value.id});
      followTask(task.id);
    } catch (error) {
      log("Continuation failed: " + error.message, "error");
    }
  }

  async function approveSelected(approved) {
    if (!state.selected || !requireConnected()) return;
    try {
      const value = await fetchJSON(
        "/operations/tasks/" + encodeURIComponent(state.selected.id) + "/approve",
        {
          method: "POST",
          headers: {"Content-Type": "application/json"},
          body: JSON.stringify({requestId: uid("approval"), approved: approved})
        }
      );
      log(approved ? "Approval queued" : "Rejection queued", "ok", value);
      followTask(state.selected.id);
    } catch (error) {
      log("Approval action failed: " + error.message, "error");
    }
  }

  function applyPreset() {
    const preset = $("#preset").value;
    $("#input-required").checked = false;
    $("#auth-required").checked = false;
    $("#delay").value = "0";
    $("#batch").value = "1";
    if (preset === "echo") $("#transform").value = "echo";
    if (preset === "upper") $("#transform").value = "upper";
    if (preset === "burst") {
      $("#transform").value = "echo";
      $("#batch").value = "10";
      $("#return-immediately").value = "true";
    }
    if (preset === "input") $("#input-required").checked = true;
    if (preset === "auth") $("#auth-required").checked = true;
    if (preset === "delay") $("#delay").value = "10";
  }

  function rawPreset() {
    const selected = $("#raw-preset").value;
    const id = state.selected ? state.selected.id : "TASK_ID";
    let request;
    if (selected === "get") {
      request = {jsonrpc: "2.0", id: "lab", method: "GetTask", params: {id: id}};
    } else if (selected === "cancel") {
      request = {jsonrpc: "2.0", id: "lab", method: "CancelTask", params: {id: id}};
    } else if (selected === "subscribe") {
      request = {jsonrpc: "2.0", id: "lab", method: "SubscribeToTask", params: {id: id}};
    } else {
      request = {jsonrpc: "2.0", id: "lab", method: "ListTasks",
        params: {pageSize: 20, includeArtifacts: true}};
    }
    $("#raw-request").value = JSON.stringify(request, null, 2);
  }

  async function runRaw() {
    if (!requireConnected()) return;
    try {
      const payload = JSON.parse($("#raw-request").value);
      if (payload.method === "SubscribeToTask") {
        const id = payload.params && payload.params.id;
        if (!id || id === "TASK_ID") throw new Error("Select a real task or provide params.id.");
        $("#raw-response").textContent =
          "Streaming subscription opened. Events are shown in Activity.";
        await subscribeTask(id, false);
        return;
      }
      const value = await fetchJSON("/rpc", {
        method: "POST",
        headers: {"Content-Type": "application/json"},
        body: JSON.stringify(payload)
      });
      $("#raw-response").textContent = JSON.stringify(value, null, 2);
      log("Raw JSON-RPC completed", "ok", {method: payload.method});
    } catch (error) {
      $("#raw-response").textContent = error.message;
      log("Raw JSON-RPC failed: " + error.message, "error");
    }
  }

  async function connect() {
    let value;
    try {
      value = validateToken(token());
    } catch (error) {
      log(error.message, "error");
      return;
    }
    state.sessionEpoch += 1;
    state.authenticated = false;
    try {
      const card = await fetchJSON("/.well-known/agent-card.json", {}, false);
      await rpc("ListTasks", {pageSize: 1});
      state.sessionEpoch += 1;
      state.authenticated = true;
      sessionStorage.setItem("a2amq.queueLab.token", value);
      log("Authenticated connection ready", "ok", {name: card.name, version: card.version});
      await refreshAll();
    } catch (error) {
      state.authenticated = false;
      sessionStorage.removeItem("a2amq.queueLab.token");
      log("Connection/authentication failed: " + error.message, "error");
    }
  }

  function scheduleAutoRefresh() {
    if (state.autoTimer) clearInterval(state.autoTimer);
    state.autoTimer = setInterval(async function () {
      if (state.autoRefreshing) return;
      state.autoRefreshing = true;
      try {
        await refreshHealth();
        if ($("#auto-refresh").checked && state.authenticated) {
          await Promise.all([refreshMetrics(), listTasks()]);
        }
      } finally {
        state.autoRefreshing = false;
      }
    }, 1200);
  }

  $("#token").value = state.token;
  $("#connect").addEventListener("click", connect);
  $("#clear-token").addEventListener("click", function () {
    stopStream();
    state.followTimers.forEach(function (handle) { clearTimeout(handle); });
    state.followTimers.clear();
    sessionStorage.removeItem("a2amq.queueLab.token");
    state.sessionEpoch += 1;
    state.authenticated = false;
    $("#token").value = "";
    $("#token-file").value = "";
    state.tasks = [];
    state.selected = null;
    renderTasks();
    renderSelected();
    ["#m-active", "#m-submitted", "#m-records", "#m-actions", "#m-publications", "#m-oldest", "#m-dlq"]
      .forEach(function (id) { $(id).textContent = "–"; });
    $("#dlq-list").textContent = "No data loaded.";
    $("#connection-info").textContent = "";
    $("#raw-response").textContent = "Ready.";
    rawPreset();
    $("#activity-log").textContent = "";
  });
  $("#token-file").addEventListener("change", async function (event) {
    const file = event.target.files && event.target.files[0];
    if (!file) return;
    if (file.size > 32768) {
      log("Selected JWT file is unexpectedly large.", "error");
      event.target.value = "";
      return;
    }
    $("#token").value = (await file.text()).trim();
    event.target.value = "";
    connect();
  });
  $("#preset").addEventListener("change", applyPreset);
  $("#send").addEventListener("click", function () { enqueue(false); });
  $("#send-and-follow").addEventListener("click", function () { enqueue(true); });
  $("#send-stream").addEventListener("click", sendStreaming);
  $("#stop-stream").addEventListener("click", stopStream);
  $("#refresh").addEventListener("click", refreshAll);
  $("#state-filter").addEventListener("change", renderTasks);
  $("#task-follow").addEventListener("click", function () {
    if (state.selected) followTask(state.selected.id);
  });
  $("#task-subscribe").addEventListener("click", function () {
    if (state.selected) subscribeTask(state.selected.id);
  });
  $("#task-continue").addEventListener("click", continueSelected);
  $("#task-approve").addEventListener("click", function () { approveSelected(true); });
  $("#task-reject").addEventListener("click", function () { approveSelected(false); });
  $("#task-cancel").addEventListener("click", cancelSelected);
  $("#refresh-dlq").addEventListener("click", refreshDLQ);
  $("#raw-preset").addEventListener("change", rawPreset);
  $("#run-raw").addEventListener("click", runRaw);
  $("#clear-log").addEventListener("click", function () { $("#activity-log").innerHTML = ""; });
  $("#toggle-agent").addEventListener("click", toggleAgent);

  rawPreset();
  applyPreset();
  loadConfig()
    .then(function () {
      scheduleAutoRefresh();
      refreshHealth();
      if (token()) connect();
    })
    .catch(function (error) {
      log("Queue Lab configuration failed: " + error.message, "error");
    });
}());
"""


SECURITY_HEADERS = {
    "Cache-Control": "no-store",
    "Content-Security-Policy": (
        "default-src 'self'; connect-src 'self'; img-src 'self' data:; "
        "style-src 'self'; script-src 'self'; object-src 'none'; base-uri 'none'; "
        "form-action 'none'; frame-ancestors 'none'"
    ),
    "Referrer-Policy": "no-referrer",
    "X-Content-Type-Options": "nosniff",
    "X-Frame-Options": "DENY",
}


def create_queue_lab(settings, *, profile: str, verifier=None, agent_pause=None) -> Starlette:
    """Return the loopback-only manual Queue Lab web application."""
    require(profile in {"local-lite", "local-workflow"}, "INVALID_RUNTIME_PROFILE")
    config = {
        "profile": profile,
        "tenant": settings.tenant,
        "source": settings.source,
        "publicUrl": settings.public_url,
        "commandRoute": settings.command_route,
        "eventRoute": settings.event_route,
        "features": {
            "workflow": profile == "local-workflow",
            "inputRequired": profile == "local-workflow",
            "authRequired": profile == "local-workflow",
            "delay": profile == "local-workflow",
            "streaming": True,
            "dlqReplay": True,
            "agentPause": profile == "local-lite" and agent_pause is not None,
        },
    }

    async def index(request):
        return HTMLResponse(LAB_HTML, headers=SECURITY_HEADERS)

    async def css(request):
        return Response(LAB_CSS, media_type="text/css", headers=SECURITY_HEADERS)

    async def js(request):
        return Response(LAB_JS, media_type="application/javascript", headers=SECURITY_HEADERS)

    async def lab_config(request):
        return JSONResponse(config, headers=SECURITY_HEADERS)

    routes = [
        Route("/", index, methods=["GET"]),
        Route("/app.css", css, methods=["GET"]),
        Route("/app.js", js, methods=["GET"]),
        Route("/config", lab_config, methods=["GET"]),
    ]

    if agent_pause is not None:
        require(verifier is not None, "INVALID_IDENTITY_CONFIG")

        async def agent_control(request):
            current_principal().authorize("operate", settings.tenant)
            if request.method == "POST":
                value = await request.json()
                require(isinstance(value, dict) and set(value) == {"paused"}
                        and type(value["paused"]) is bool, "INVALID_OPERATION")
                if value["paused"]:
                    agent_pause.set()
                else:
                    agent_pause.clear()
            return JSONResponse({"paused": agent_pause.is_set()}, headers=SECURITY_HEADERS)

        async def control_failure(request, error):
            code = error.code
            status = 503 if isinstance(error, RetryableError) else {
                "UNAUTHENTICATED": 401,
                "FORBIDDEN": 403,
            }.get(code, 422)
            return JSONResponse({"error": code}, status_code=status, headers=SECURITY_HEADERS)

        control_app = Starlette(
            routes=[Route("/agent", agent_control, methods=["GET", "POST"])],
            exception_handlers={PermanentError: control_failure, RetryableError: control_failure},
        )
        protected = BearerIdentity(BoundedJSON(control_app), verifier)
        routes.append(Mount("/control", protected))

    return Starlette(routes=routes)
