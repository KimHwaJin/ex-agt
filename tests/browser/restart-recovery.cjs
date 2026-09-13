/* Explicitly restarts ONLY the local Compose api, preserving PostgreSQL. */
const assert = require("node:assert/strict");
const { randomUUID } = require("node:crypto");
const { execFileSync } = require("node:child_process");
const { resolve } = require("node:path");

const base = process.env.CHATAPP_UI_TEST_URL;
if (process.env.CHATAPP_RESTART_TEST !== "1"
    || !base || new URL(base).hostname !== "127.0.0.1"
    || new URL(base).port !== "8020") {
  throw new Error("Requires CHATAPP_RESTART_TEST=1 and local Compose :8020");
}
const root = resolve(__dirname, "../..");
const prefix = base.replace(/\/$/, "") + "/api/v1";
const docker = process.env.DOCKER_EXECUTABLE || "docker";

async function api(path, options = {}) {
  const response = await fetch(prefix + path, options);
  if (!response.ok) {
    throw new Error(`${response.status}: ${await response.text()}`);
  }
  return response.json();
}

function compose(...args) {
  execFileSync(docker, ["compose", ...args], {
    cwd: root, stdio: "inherit", timeout: 60000,
  });
}

async function main() {
  const home = await api("/me", {
    method: "POST", headers: { "X-User-Id": "restart-" + randomUUID() },
  });
  const headers = { "X-User-UUID": home.user.user_uuid,
    "Content-Type": "application/json", "Idempotency-Key": randomUUID() };
  const session = await api("/sessions", { method: "POST", headers,
    body: JSON.stringify({ project_id: home.default_project.project_id }) });
  headers["Idempotency-Key"] = randomUUID();
  const body = JSON.stringify({ user_id: home.user.user_uuid,
    session_id: session.session_id,
    input: { type: "message", content: [{ type: "text", text: "복구 테스트" }] },
  });
  const receipt = await api("/agent/runs", { method: "POST", headers, body });
  try {
    compose("stop", "--timeout", "1", "api");
  } finally {
    compose("up", "--no-deps", "-d", "--wait", "api");
  }
  let detail;
  for (let attempt = 0; attempt < 40; attempt++) {
    detail = await api("/agent/runs/" + receipt.run_id, { headers });
    if (detail.status === "awaiting_input") break;
    await new Promise((resolve) => setTimeout(resolve, 250));
  }
  assert.equal(detail.status, "awaiting_input");
  const replay = await api("/agent/runs", { method: "POST", headers, body });
  assert.deepEqual(replay, receipt);
  const page = await api("/sessions/" + session.session_id + "/messages",
    { headers });
  assert.equal(page.items.length, 2);
  assert.equal(page.items[0].content[0].text.split("API 흐름").length, 2);
  const response = await fetch(prefix + "/agent/runs/" + receipt.run_id
    + "/stream", { headers });
  const stream = await response.text();
  assert.ok(stream.includes("run.interrupted"));
  assert.ok(stream.includes("message.delta"));
  const cancelled = await api("/agent/runs/" + receipt.run_id + "/cancel",
    { method: "POST", headers });
  assert.equal(cancelled.status, "cancelled");
  console.log(JSON.stringify({ result: "passed", run_id: receipt.run_id,
    checks: ["API process restart", "DB job continuation",
      "original admission replay", "no duplicated output", "event replay"],
  }, null, 2));
}

main().catch((error) => { console.error(error); process.exitCode = 1; });
