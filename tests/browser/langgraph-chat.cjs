/* Real model smoke test: synthetic messages only, local Compose only. */
const assert = require("node:assert/strict");
const { randomUUID } = require("node:crypto");
const { execFileSync } = require("node:child_process");
const { mkdtempSync } = require("node:fs");
const { tmpdir } = require("node:os");
const { join, resolve } = require("node:path");
const { chromium } = require("playwright");

const root = process.env.CHATAPP_UI_TEST_URL;
if (!root || new URL(root).hostname !== "127.0.0.1"
    || new URL(root).port !== "8020") {
  throw new Error("Use the isolated local Compose API on 127.0.0.1:8020");
}
const artifacts = mkdtempSync(join(tmpdir(), "chatapp-langgraph-"));

async function main() {
  const browser = await chromium.launch({ headless: true,
    executablePath: process.env.CHROME_EXECUTABLE_PATH || undefined });
  const page = await browser.newPage({
    viewport: { width: 1440, height: 1000 },
  });
  const errors = [];
  page.on("pageerror", (error) => errors.push(error.message));
  const employee = "graph-ui-" + randomUUID();
  const code = "ORION-" + randomUUID().slice(0, 8);
  async function login() {
    await page.goto(root + "/dev/");
    await page.locator("#employee-id").fill(employee);
    await page.locator('#me-form button[type="submit"]').click();
    await page.waitForFunction(() =>
      !document.querySelector("#app-screen").hidden);
  }
  async function ready() {
    await page.waitForFunction(() =>
      !document.querySelector("#send-message").disabled, null,
    { timeout: 60000 });
  }
  async function send(text) {
    await ready();
    await page.locator("#message-draft").fill(text);
    await page.locator("#send-message").click();
    await ready();
    assert.ok((await page.locator("#run-status").textContent())
      .startsWith("완료"));
  }
  try {
    const runtime = await page.request.get(root + "/dev/runtime");
    assert.equal((await runtime.json()).agent_backend, "langgraph");
    await login();
    await page.locator("#welcome-new-chat").click();
    await ready();
    const sessionId = await page.locator(
      '#session-list button[aria-pressed="true"]',
    ).getAttribute("data-id");
    await send(`프로젝트 코드는 ${code}야. 기억해 줘. 그리고 데이터 분석에서`
      + " 결측치를 확인하는 이유를 500자 정도 설명해 줘.");
    assert.equal(await page.locator(".chat-message").count(), 2);
    const owner = await page.locator("#user-uuid").inputValue();
    const headers = { "X-User-UUID": owner };
    const list = await page.request.get(root + "/api/v1/sessions/"
      + sessionId + "/runs?limit=1", { headers });
    const runId = (await list.json()).items[0].run_id;
    const stream = await page.request.get(root + "/api/v1/agent/runs/"
      + runId + "/stream", { headers });
    assert.ok((await stream.text()).includes("event: message.delta"));
    if (process.env.CHATAPP_RESTART_TEST === "1") {
      const docker = process.env.DOCKER_EXECUTABLE || "docker";
      const options = { cwd: resolve(__dirname, "../.."),
        stdio: "inherit", timeout: 60000 };
      try {
        execFileSync(docker, ["compose", "stop", "--timeout", "1", "api"],
          options);
      } finally {
        execFileSync(docker, ["compose", "up", "--no-deps", "-d", "--wait",
          "api"], options);
      }
    }
    await login();
    await page.locator(`#session-list button[data-id="${sessionId}"]`).click();
    await ready();
    assert.equal(await page.locator(".chat-message").count(), 2);
    await send("내가 앞에서 알려준 프로젝트 코드만 정확히 답해줘.");
    assert.equal(await page.locator(".chat-message").count(), 4);
    const last = await page.locator(".chat-message.assistant").last()
      .textContent();
    assert.ok(last.includes(code), last);
    await page.screenshot({ path: join(artifacts, "conversation.png"),
      fullPage: true });
    assert.deepEqual(errors, []);
    console.log(JSON.stringify({ result: "passed", artifacts,
      restarted: process.env.CHATAPP_RESTART_TEST === "1",
      checks: ["real LLM", "token stream", "persisted messages",
        "session checkpoint memory", "UI restoration"] }, null, 2));
  } finally {
    await browser.close();
  }
}

main().catch((error) => { console.error(error); process.exitCode = 1; });
