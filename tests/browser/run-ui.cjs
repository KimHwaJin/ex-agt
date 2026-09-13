/* Run only against an isolated local development Compose API. */
const assert = require("node:assert/strict");
const { randomUUID } = require("node:crypto");
const { mkdtempSync } = require("node:fs");
const { tmpdir } = require("node:os");
const { join } = require("node:path");
const { chromium } = require("playwright");

const base = process.env.CHATAPP_UI_TEST_URL;
if (!base || !["localhost", "127.0.0.1"].includes(new URL(base).hostname)) {
  throw new Error("CHATAPP_UI_TEST_URL must be an isolated local API");
}
const root = base.replace(/\/$/, "");
const artifacts = mkdtempSync(join(tmpdir(), "chatapp-run-ui-"));

async function main() {
  const browser = await chromium.launch({
    headless: true,
    executablePath: process.env.CHROME_EXECUTABLE_PATH || undefined,
  });
  const page = await browser.newPage({
    viewport: { width: 1440, height: 1000 },
  });
  const errors = [];
  page.on("pageerror", (error) => errors.push(error.message));
  const employee = "run-ui-" + randomUUID();
  let owner;

  async function login() {
    await page.goto(root + "/dev/");
    await page.locator("#employee-id").fill(employee);
    await page.locator('#me-form button[type="submit"]').click();
    await page.waitForFunction(() =>
      !document.querySelector("#app-screen").hidden);
    owner = await page.locator("#user-uuid").inputValue();
  }

  async function ready() {
    await page.waitForFunction(() =>
      !document.querySelector("#send-message").disabled);
  }

  async function waiting() {
    await page.locator('[data-decision="approve"]').waitFor();
    await page.waitForFunction(() =>
      !document.querySelector('[data-decision="approve"]').disabled);
  }

  async function details(sessionId) {
    const headers = { "X-User-UUID": owner };
    const runs = await page.request.get(root + "/api/v1/sessions/"
      + sessionId + "/runs?limit=1", { headers });
    const run = (await runs.json()).items[0];
    const response = await page.request.get(root + "/api/v1/agent/runs/"
      + run.run_id, { headers });
    assert.equal(response.status(), 200);
    return response.json();
  }

  try {
    await login();
    await page.locator("#welcome-new-chat").click();
    await ready();
    const sessionId = await page.locator(
      '#session-list button[aria-pressed="true"]',
    ).getAttribute("data-id");
    assert.ok(sessionId);
    // End observation after awaiting_input, before the actual HITL payload.
    // The client must reconnect to recover the missing approval card.
    await page.route("**/api/v1/agent/runs", async (route) => {
      const response = await route.fetch();
      const full = await response.text();
      const interrupt = full.indexOf("event: run.interrupted");
      assert.ok(interrupt > 0);
      const cut = full.lastIndexOf("id: ", interrupt);
      await route.fulfill({ response, body: full.slice(0, cut) });
    }, { times: 1 });
    await page.locator("#message-draft").fill("샘플 데이터 분석해 줘");
    await page.locator("#send-message").click();
    await waiting();
    assert.equal(await page.locator(".chat-message").count(), 2);
    assert.equal(await page.locator("#send-message").isDisabled(), true);
    const first = await details(sessionId);
    assert.equal(first.status, "awaiting_input");
    assert.equal(first.executions.length, 0);
    await page.locator("#hitl-instruction").fill("그래프는 제외해 줘");
    await page.locator('[data-decision="modify"]').click();
    await page.locator('#hitl-panel [data-version="2"]').waitFor();
    await waiting();
    const revised = await details(sessionId);
    assert.equal(first.run_id, revised.run_id);
    assert.notEqual(first.pending_interrupts[0].interrupt_id,
      revised.pending_interrupts[0].interrupt_id);

    // Page observation can disappear without losing the pending approval.
    await page.reload();
    await login();
    await page.locator(`#session-list button[data-id="${sessionId}"]`).click();
    await waiting();
    assert.equal(await page.locator(".chat-message").count(), 2);
    assert.equal((await page.locator(".message-content").allTextContents())
      .join(" ").split("API 흐름 검증용 응답입니다.").length, 2);
    await page.screenshot({
      path: join(artifacts, "approval-restored.png"), fullPage: true,
    });
    await page.locator('[data-decision="approve"]').click();
    await ready();
    assert.equal((await details(sessionId)).status, "completed");
    assert.equal(await page.locator(".chat-message").count(), 3);
    const completed = await details(sessionId);
    assert.equal(completed.executions.length, 1);
    assert.equal(completed.executions[0].simulated, true);
    const text = await page.locator("#chat-messages").textContent();
    assert.ok(text.includes("실제 데이터·노트북·분석 리포트는"));
    await page.screenshot({
      path: join(artifacts, "completed.png"), fullPage: true,
    });

    await page.locator("#message-draft").fill("취소 흐름 테스트");
    await page.locator("#send-message").click();
    await waiting();
    await page.locator('[data-decision="approve"]').click();
    await page.waitForFunction(() => document.querySelector("#run-status")
      .textContent.includes("실행 결과 대기"));
    page.once("dialog", (dialog) => dialog.accept());
    await page.locator("#cancel-run").click();
    await ready();
    const cancelled = await details(sessionId);
    assert.equal(cancelled.status, "cancelled");
    assert.equal(await page.locator(".chat-message").count(), 5);
    const session = await page.request.get(root + "/api/v1/sessions/"
      + sessionId, { headers: { "X-User-UUID": owner } });
    const saved = await session.json();
    assert.equal(saved.is_locked, false);
    assert.equal(saved.active_run_id, null);
    await page.setViewportSize({ width: 390, height: 844 });
    await page.screenshot({
      path: join(artifacts, "mobile.png"), fullPage: true,
    });
    assert.equal(await page.evaluate(() =>
      document.documentElement.scrollWidth <= window.innerWidth), true);
    assert.deepEqual(errors, []);
    console.log(JSON.stringify({ result: "passed", artifacts, checks: [
      "streaming messages", "modify and same-run resume", "reload approval",
      "recover interrupted HITL stream",
      "no replay duplication", "success", "execution cancellation",
      "session unlock", "no cancelled-run report", "mobile layout",
    ] }, null, 2));
  } catch (error) {
    await page.screenshot({ path: join(artifacts, "failure.png"),
      fullPage: true }).catch(() => {});
    console.error("Browser artifacts:", artifacts);
    throw error;
  } finally {
    await browser.close();
  }
}

main().catch((error) => { console.error(error); process.exitCode = 1; });
