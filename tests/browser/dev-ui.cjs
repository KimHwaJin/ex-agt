/* Optional browser regression. Requires an isolated local Compose API. */
const assert = require("node:assert/strict");
const { randomUUID } = require("node:crypto");
const { mkdtempSync } = require("node:fs");
const { tmpdir } = require("node:os");
const { join } = require("node:path");
const { chromium } = require("playwright");

const base = process.env.CHATAPP_UI_TEST_URL;
if (!base || !["127.0.0.1", "localhost"].includes(new URL(base).hostname)) {
  throw new Error("Set CHATAPP_UI_TEST_URL to an isolated local Compose API");
}
const prefix = base.replace(/\/$/, "");
const apiBase = prefix + "/api/v1";
const artifacts = mkdtempSync(join(tmpdir(), "chatapp-dev-ui-"));

async function main() {
  const browser = await chromium.launch({
    headless: true,
    executablePath: process.env.CHROME_EXECUTABLE_PATH || undefined,
  });
  const page = await browser.newPage({
    viewport: { width: 1440, height: 1080 },
  });
  const errors = [];
  const requests = [];
  page.on("pageerror", (error) => errors.push(error.message));
  page.on("request", (request) => {
    if (request.url().startsWith(apiBase)) {
      requests.push({
        method: request.method(),
        path: new URL(request.url()).pathname,
        headers: request.headers(),
      });
    }
  });

  async function done(message) {
    await page.waitForFunction((expected) => {
      const notice = document.getElementById("notice");
      const busy = document.getElementById("workspace")
        .getAttribute("aria-busy");
      return busy === "false" && notice.textContent.includes(expected);
    }, message, { timeout: 10000 });
  }

  async function submit(form, message) {
    await page.locator("#" + form + ' button[type="submit"]').click();
    await done(message);
  }

  async function selectProject(id) {
    await page.locator(`#project-list button[data-id="${id}"]`).click();
    await done("프로젝트를 조회");
  }

  async function closePanels() {
    while (await page.locator("dialog[open]").count()) {
      await page.keyboard.press("Escape");
    }
  }

  async function panel(kind) {
    const mapping = {
      projectCreate: ["project-create-dialog", "welcome-new-project"],
      sessionCreate: ["session-create-dialog", "open-session-create"],
      project: ["project-dialog", "open-project-edit"],
      session: ["session-dialog", "open-session-edit"],
      inspector: ["inspector-dialog", "open-inspector"],
    };
    const [id, opener] = mapping[kind];
    if (await page.locator("#" + id).getAttribute("open") !== null) return;
    await closePanels();
    await page.locator("#" + opener).click();
    assert.ok(await page.locator("#" + id).isVisible());
  }

  try {
    await page.goto(prefix + "/dev");
    await page.waitForFunction(
      () => document.querySelector("#project-list p"),
    );
    assert.ok(page.url().endsWith("/dev/"));
    assert.equal(await page.locator("#login-screen").isVisible(), true);
    assert.equal(await page.locator("#app-screen").isVisible(), false);
    assert.equal(
      await page.locator("#project-create button").isDisabled(), true,
    );
    await page.screenshot({
      path: join(artifacts, "login.png"), fullPage: true,
    });

    // Failed identification must not reveal a partially initialized shell.
    await page.route("**/api/v1/me", (route) => route.fulfill({
      status: 503,
      contentType: "application/json",
      body: JSON.stringify({ message: "연결 실패 검증" }),
    }));
    await page.locator("#employee-id").fill("unavailable-user");
    await submit("me-form", "HTTP 503");
    assert.equal(await page.locator("#login-screen").isVisible(), true);
    assert.equal(await page.locator("#app-screen").isVisible(), false);
    assert.ok(await page.locator("#login-notice").textContent()
      .then((text) => text.includes("HTTP 503")));
    await page.unroute("**/api/v1/me");

    const employee = "ui-" + randomUUID();
    await page.locator("#employee-id").fill(employee);
    await submit("me-form", "사용자를 연결");
    assert.equal(await page.locator("#login-screen").isVisible(), false);
    assert.equal(await page.locator("#app-screen").isVisible(), true);
    assert.equal(await page.locator("#message-draft").isDisabled(), true);
    assert.equal(await page.locator("#send-message").isDisabled(), true);
    await page.screenshot({
      path: join(artifacts, "workspace.png"), fullPage: true,
    });
    const owner = await page.locator("#user-uuid").inputValue();
    const headers = { "X-User-UUID": owner };
    assert.ok(owner);
    const me = await page.evaluate(async (userUuid) => {
      const response = await fetch("../api/v1/me", {
        headers: { "X-User-UUID": userUuid },
      });
      return { status: response.status, body: await response.json() };
    }, owner);
    assert.equal(me.status, 200);
    assert.equal(me.body.user.user_uuid, owner);
    assert.equal(me.body.user.user_id, employee);
    assert.equal(me.body.default_project.is_default, true);
    assert.equal(await page.locator("#project-delete").isDisabled(), true);
    const defaultId = await page.locator("#project-selection").textContent();

    const name = "매출 데이터 분석 " + randomUUID().slice(0, 6);
    await panel("projectCreate");
    await page.locator("#new-project-name").fill(name);
    await page.locator("#new-project-description").fill("개발 화면 검증");
    await submit("project-create", "프로젝트를 생성");
    const projectId = await page.locator("#project-selection").textContent();

    await panel("sessionCreate");
    await submit("session-create", "세션을 생성");
    const sessionId = await page.locator("#session-selection").textContent();
    assert.equal(await page.locator("#session-title").inputValue(), "새 대화");
    await panel("session");
    await page.locator("#session-title").fill("기초 통계 확인");
    await submit("session-edit", "세션을 수정");
    await panel("session");
    await page.locator("#session-refresh").click();
    await done("세션을 조회");
    await closePanels();

    // A concurrent API edit must not overwrite the user's draft on conflict.
    const concurrent = await page.request.patch(
      apiBase + "/projects/" + projectId,
      { headers, data: { version: 1, name: "외부에서 수정" } },
    );
    assert.equal(concurrent.status(), 200);
    await panel("project");
    await page.locator("#project-name").fill("보존할 수정안");
    await submit("project-edit", "HTTP 409");
    assert.equal(await page.locator("#project-dialog").isVisible(), true);
    assert.ok(await page.locator("#project-dialog .dialog-notice")
      .textContent().then((text) => text.includes("HTTP 409")));
    assert.equal(
      await page.locator("#project-name").inputValue(), "보존할 수정안",
    );
    await page.locator("#project-refresh").click();
    await done("프로젝트를 조회");
    assert.equal(
      await page.locator("#project-name").inputValue(), "외부에서 수정",
    );
    await page.locator("#project-name").fill(name);
    await page.locator("#project-description").fill("");
    await submit("project-edit", "프로젝트를 수정");
    const detail = JSON.parse(
      await page.locator("#project-detail").textContent(),
    );
    assert.equal(detail.description, null);
    assert.equal(detail.version, 3);

    // API-provided text must never be inserted as HTML.
    const hostile = '<img src=x onerror="window.__xss=1">';
    await panel("project");
    await page.locator("#project-name").fill(hostile);
    await submit("project-edit", "프로젝트를 수정");
    assert.equal(await page.locator("#project-list img").count(), 0);
    assert.equal(await page.evaluate(() => window.__xss), undefined);
    await panel("project");
    await page.locator("#project-name").fill(name);
    await submit("project-edit", "프로젝트를 수정");

    // Seed only this test user's records to exercise both cursor lists.
    for (let i = 0; i < 5; i++) {
      let response = await page.request.post(apiBase + "/projects", {
        headers: { ...headers, "Idempotency-Key": randomUUID() },
        data: { name: "페이지 확인 " + i },
      });
      assert.equal(response.status(), 201);
      response = await page.request.post(apiBase + "/sessions", {
        headers: { ...headers, "Idempotency-Key": randomUUID() },
        data: { project_id: projectId, title: "세션 페이지 " + i },
      });
      assert.equal(response.status(), 201);
    }
    await panel("inspector");
    await page.locator(".connection-details summary").click();
    await page.locator("#page-size").selectOption("5");
    await done("조회했습니다");
    await closePanels();
    assert.equal(await page.locator("#project-list button").count(), 5);
    assert.equal(await page.locator("#session-list button").count(), 5);
    await page.locator("#project-next").click();
    await done("조회했습니다");
    const projectIds = await page.locator("#project-list button")
      .evaluateAll((nodes) => nodes.map((node) => node.dataset.id));
    assert.equal(projectIds.length, 7);
    assert.equal(new Set(projectIds).size, 7);
    await page.locator("#session-next").click();
    await done("조회했습니다");
    assert.equal(await page.locator("#session-list button").count(), 6);

    await selectProject(defaultId);
    assert.equal(await page.locator("#session-list button").count(), 0);
    assert.equal(await page.locator("#session-title").inputValue(), "");
    assert.equal(await page.locator("#session-next").isDisabled(), true);
    await selectProject(projectId);
    page.once("dialog", (dialog) => dialog.dismiss());
    const deletes = requests.filter((r) => r.method === "DELETE").length;
    await panel("project");
    await page.locator("#project-delete").click();
    await done("삭제를 취소");
    assert.equal(
      requests.filter((r) => r.method === "DELETE").length, deletes,
    );
    await closePanels();

    // Drop a response AFTER the server commits, then submit the same body.
    const retryName = "응답 유실 검증 " + randomUUID().slice(0, 6);
    const keys = [];
    await page.route("**/api/v1/projects", async (route) => {
      const request = route.request();
      if (request.method() !== "POST"
          || request.postDataJSON().name !== retryName) {
        await route.continue();
        return;
      }
      keys.push(request.headers()["idempotency-key"]);
      if (keys.length === 1) {
        assert.equal((await route.fetch()).status(), 201);
        await route.abort("failed");
      } else {
        await route.continue();
      }
    });
    await panel("projectCreate");
    await page.locator("#new-project-name").fill(retryName);
    await submit("project-create", "네트워크");
    await submit("project-create", "프로젝트를 생성");
    assert.equal(keys.length, 2);
    assert.ok(keys[0]);
    assert.equal(keys[0], keys[1]);
    await page.unroute("**/api/v1/projects");
    const all = await page.request.get(apiBase + "/projects?limit=100", {
      headers,
    });
    const matching = (await all.json()).items.filter(
      (item) => item.name === retryName,
    );
    assert.equal(matching.length, 1);

    await panel("project");
    page.once("dialog", (dialog) => dialog.accept());
    await page.locator("#project-delete").click();
    await done("프로젝트를 삭제");
    await page.locator("#project-next").click();
    await done("조회했습니다");
    await selectProject(projectId);
    await page.locator("#session-next").click();
    await done("조회했습니다");
    await page.locator(
      `#session-list button[data-id="${sessionId}"]`,
    ).click();
    await done("세션을 조회");

    // Capture the actual packaged UI before destructive checks clear it.
    await page.screenshot({
      path: join(artifacts, "desktop.png"), fullPage: true,
    });
    await panel("inspector");
    await page.screenshot({
      path: join(artifacts, "inspector.png"), fullPage: true,
    });
    await closePanels();
    await page.setViewportSize({ width: 390, height: 844 });
    await page.waitForFunction(() => {
      return document.getElementById("workspace").dataset.sidebarOpen
        === "false";
    });
    assert.ok(await page.evaluate(
      () => document.documentElement.scrollWidth <= window.innerWidth,
    ));
    await page.screenshot({
      path: join(artifacts, "mobile.png"), fullPage: true,
    });
    await page.locator("#sidebar-toggle").click();
    assert.equal(await page.locator("#sidebar").isVisible(), true);
    await page.screenshot({
      path: join(artifacts, "mobile-sidebar.png"), fullPage: true,
    });
    await page.keyboard.press("Escape");
    assert.equal(await page.locator("#sidebar").isVisible(), false);

    await panel("session");
    page.once("dialog", (dialog) => dialog.accept());
    await page.locator("#session-delete").click();
    await done("세션을 삭제");
    const deleted = await page.request.get(
      apiBase + "/sessions/" + sessionId, { headers },
    );
    assert.equal(deleted.status(), 404);
    await panel("project");
    page.once("dialog", (dialog) => dialog.accept());
    await page.locator("#project-delete").click();
    await done("프로젝트를 삭제");
    assert.equal(await page.locator("#session-list button").count(), 0);

    await page.locator("#sidebar-toggle").click();
    await page.locator("#switch-user").click();
    assert.equal(await page.locator("#login-screen").isVisible(), true);
    assert.equal(await page.locator("#app-screen").isVisible(), false);
    assert.equal(await page.locator("#user-uuid").inputValue(), "");
    await page.locator("#employee-id").fill("ui-other-" + randomUUID());
    await submit("me-form", "사용자를 연결");
    assert.notEqual(await page.locator("#user-uuid").inputValue(), owner);
    assert.equal(await page.locator("#project-list button").count(), 1);
    assert.equal(await page.locator("#session-list button").count(), 0);
    assert.equal(await page.locator("#history-select option").count(), 4);
    assert.ok(!await page.locator("#request-json").textContent()
      .then((text) => text.includes(owner)));
    await page.locator("#welcome-new-chat").click();
    await done("세션을 생성");
    assert.equal(await page.locator("#view-title").textContent(), "새 대화");
    assert.equal(await page.locator("#send-message").isDisabled(), true);
    await page.reload();
    await page.waitForFunction(
      () => document.querySelector("#project-list p"),
    );
    assert.equal(await page.locator("#login-screen").isVisible(), true);
    assert.equal(await page.locator("#app-screen").isVisible(), false);

    const operations = new Set(requests.map((request) => {
      const path = request.path.replace(
        /\/(projects|sessions)\/[^/]+$/, "/$1/:id",
      );
      return request.method + " " + path;
    }));
    assert.equal(operations.size, 12);
    assert.deepEqual(errors, []);
    console.log(JSON.stringify({
      result: "passed",
      operations: operations.size,
      checks: [
        "login transition/rejection/reset", "CRUD", "cursor pagination",
        "version conflict in dialog", "disabled chat composer",
        "idempotency after lost response", "XSS-safe text",
        "identity/project switching", "delete confirmation", "mobile layout",
      ],
      artifacts,
    }, null, 2));
  } catch (error) {
    await page.screenshot({
      path: join(artifacts, "failure.png"), fullPage: true,
    }).catch(() => {});
    console.error("Browser artifacts:", artifacts);
    throw error;
  } finally {
    await browser.close();
  }
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
