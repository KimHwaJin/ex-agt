/* Real model + public UI smoke: only synthetic inputs and local test API. */
const assert = require("node:assert/strict");
const { randomUUID } = require("node:crypto");
const { execFileSync } = require("node:child_process");
const { mkdtempSync } = require("node:fs");
const { tmpdir } = require("node:os");
const { join } = require("node:path");
const { chromium } = require("playwright");

const root = process.env.CHATAPP_UI_TEST_URL;
if (!root || new URL(root).hostname !== "127.0.0.1"
    || !["8020", "8021"].includes(new URL(root).port)) {
  throw new Error("Use an isolated local API on 127.0.0.1:8020 or 8021");
}
const artifacts = mkdtempSync(join(tmpdir(), "chatapp-hitl-"));

async function main() {
  const browser = await chromium.launch({ headless: true,
    executablePath: process.env.CHROME_EXECUTABLE_PATH || undefined });
  const page = await browser.newPage({ viewport: { width: 1440, height: 1000 } });
  const errors = [];
  page.on("pageerror", (error) => errors.push(error.message));
  const employee = "hitl-ui-" + randomUUID();
  let owner;
  let sessionId;
  async function waitForApi() {
    for (let attempt = 0; attempt < 60; attempt++) {
      try {
        const response = await page.request.get(root + "/health/ready",
          { timeout: 1000 });
        if (response.ok()) return;
      } catch (_) { /* API process may still be starting. */ }
      await new Promise((resolve) => setTimeout(resolve, 250));
    }
    throw new Error("Test API did not become ready");
  }
  async function login() {
    await waitForApi();
    await page.goto(root + "/dev/");
    await page.locator("#employee-id").fill(employee);
    await page.locator('#me-form button[type="submit"]').click();
    await page.waitForFunction(() =>
      !document.querySelector("#app-screen").hidden);
    owner = await page.locator("#user-uuid").inputValue();
  }
  async function ready() {
    await page.waitForFunction(() =>
      !document.querySelector("#send-message").disabled, null,
    { timeout: 180000 });
  }
  async function submit(text) {
    await ready();
    await page.locator("#message-draft").fill(text);
    await page.locator("#send-message").click();
  }
  async function review(version) {
    await page.waitForFunction((v) =>
      (document.querySelector(`#hitl-panel [data-version="${v}"]`)
      && !document.querySelector('#hitl-panel button').disabled)
      || document.querySelector("#run-status").textContent.startsWith("실패"),
    version, { timeout: 180000 });
    const current = await detail(sessionId);
    assert.equal(current.status, "awaiting_input", JSON.stringify(current.error));
  }
  async function detail(sessionId) {
    const headers = { "X-User-UUID": owner };
    const list = await page.request.get(root + "/api/v1/sessions/"
      + sessionId + "/runs?limit=1", { headers });
    const runId = (await list.json()).items[0].run_id;
    return (await page.request.get(root + "/api/v1/agent/runs/"
      + runId, { headers })).json();
  }
  try {
    await login();
    await page.locator("#new-chat").click();
    await ready();
    sessionId = await page.locator(
      '#session-list button[aria-pressed="true"]',
    ).getAttribute("data-id");
    await submit("안녕! 한 문장으로 인사해줘.");
    await ready();
    assert.equal((await detail(sessionId)).status, "completed");
    await submit("EDA에서 결측치 비율을 확인하는 이유만 설명해줘. 실행 요청은 아니야.");
    await ready();
    assert.equal((await detail(sessionId)).status, "completed");
    await submit("샘플 매출 데이터 100행을 만들고 EDA와 시각화를 실제 실행해줘.");
    await review(1);
    const first = await detail(sessionId);
    assert.equal(first.pending_interrupts[0].payload.intent, "analysis_task");
    assert.equal(first.pending_interrupts[0].payload.executable, false);
    assert.equal(first.pending_interrupts[0].payload.code_prepared, true);
    assert.ok(first.pending_interrupts[0].payload.steps
      .every((step) => step.skill_id && step.tool_id && step.parameters));
    assert.ok(!JSON.stringify(first).includes("function_source"));
    assert.deepEqual(first.executions, []);
    assert.ok(!(await page.locator("#hitl-panel").textContent())
      .includes("[object Object]"));
    if (process.env.CHATAPP_RESTART_TEST === "1") {
      assert.equal(new URL(root).port, "8021");
      execFileSync(process.env.DOCKER_EXECUTABLE || "docker",
        ["restart", "ex-agent-hitl-test-api"], { timeout: 60000 });
      await waitForApi();
    }
    await login();
    await page.locator(`#session-list button[data-id="${sessionId}"]`).click();
    await review(1);
    assert.equal((await detail(sessionId)).pending_interrupts[0].interrupt_id,
      first.pending_interrupts[0].interrupt_id);
    await page.locator("#hitl-instruction").fill(
      "내부 스킬과 도메인 함수를 사용하지 말고 직접 코드를 작성해줘. "
      + "샘플은 50행으로 줄이고 막대그래프 1개만 그려줘.",
    );
    await page.locator('[data-decision="modify"]').click();
    await review(2);
    const revised = await detail(sessionId);
    assert.notEqual(revised.pending_interrupts[0].interrupt_id,
      first.pending_interrupts[0].interrupt_id);
    assert.equal(revised.pending_interrupts[0].payload.implementation,
      "generated_code");
    assert.equal(revised.pending_interrupts[0].payload.code_prepared, true);
    assert.ok(revised.pending_interrupts[0].payload.steps
      .every((step) => !step.skill_id && !step.tool_id));
    await page.screenshot({ path: join(artifacts, "revised-plan.png"),
      fullPage: true });
    await page.locator('[data-decision="reject"]').click();
    await ready();
    assert.equal((await detail(sessionId)).status, "rejected");
    await submit("파이썬으로 1부터 10까지 합을 구하는 코드를 실제 실행해줘.");
    await review(1);
    const code = await detail(sessionId);
    assert.equal(code.pending_interrupts[0].payload.intent, "code_task");
    await page.locator('[data-decision="approve"]').click();
    await ready();
    const approved = await detail(sessionId);
    assert.equal(approved.status, "failed");
    assert.equal(approved.error.code, "EXECUTOR_NOT_CONFIGURED");
    assert.deepEqual(approved.executions, []);
    await page.screenshot({ path: join(artifacts, "approved-unavailable.png"),
      fullPage: true });
    assert.deepEqual(errors, []);
    console.log(JSON.stringify({ result: "passed", artifacts,
      checks: ["real model routing", "catalog/code plan", "restore review",
        "modify", "reject", "approve without false execution"] }, null, 2));
  } finally {
    if (sessionId && owner) {
      try {
        const latest = await detail(sessionId);
        if (!["completed", "rejected", "failed", "cancelled"]
          .includes(latest.status)) {
          await page.request.post(root + "/api/v1/agent/runs/"
            + latest.run_id + "/cancel", {
            headers: { "X-User-UUID": owner },
          });
        }
      } catch (_) { /* Never affect users outside this synthetic session. */ }
    }
    await browser.close();
  }
}
main().catch((error) => { console.error(error); process.exitCode = 1; });
