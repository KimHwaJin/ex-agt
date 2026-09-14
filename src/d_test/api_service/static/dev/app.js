import { installChat } from "./chat.js";

const $ = (id) => document.getElementById(id);
const apiBase = new URL("../api/v1/", window.location.href);
const mobile = window.matchMedia("(max-width: 760px)");
const state = {
  screen: "login",
  sidebarOpen: !mobile.matches,
  user: null,
  project: null,
  session: null,
  projects: [],
  sessions: [],
  projectCursor: null,
  sessionCursor: null,
  busy: false,
  history: [],
  sequence: 0,
  pendingCreates: new Map(),
  identityEpoch: 0,
};

function notice(message, kind = "") {
  $("notice").textContent = message;
  $("notice").dataset.kind = kind;
  $("login-notice").textContent = message;
  $("login-notice").dataset.kind = kind;
  document.querySelectorAll("dialog[open] .dialog-notice").forEach((node) => {
    node.textContent = message;
    node.dataset.kind = kind;
  });
}

function setSidebar(open) {
  state.sidebarOpen = open;
  $("workspace").dataset.sidebarOpen = String(open);
  $("sidebar-toggle").setAttribute("aria-expanded", String(open));
  $("sidebar-backdrop").hidden = !open || !mobile.matches;
  $("sidebar").inert = !open;
  document.querySelector(".main-pane").inert = mobile.matches && open;
}

function closeMobileSidebar() {
  if (mobile.matches) setSidebar(false);
}

function openDialog(id) {
  if (state.busy || !state.user) return;
  const dialog = $(id);
  if (dialog.open) return;
  const feedback = dialog.querySelector(".dialog-notice");
  if (feedback) feedback.textContent = "";
  dialog.showModal();
  const input = dialog.querySelector("input:not([readonly])");
  if (input) input.focus();
}

function closeDialog(id) {
  if ($(id).open) $(id).close();
}

function renderShell() {
  $("account-name").textContent = state.user?.user_id || "사용자";
  $("account-name").title = state.user?.user_id || "";
  $("account-avatar").textContent =
    (state.user?.user_id || "U").slice(0, 2).toUpperCase();
  $("main-project-name").textContent = state.project?.name || "작업 공간";
  $("main-session-name").textContent = state.session?.title || "새 대화";
  $("view-context").textContent = state.project?.name || "YOUR WORKSPACE";
  $("view-title").textContent = state.session?.title || "무엇을 분석해 볼까요?";
  $("view-description").textContent = state.session
    ? "대화 공간이 준비됐어요. 에이전트가 연결되면 이곳에서 분석을 시작합니다."
    : state.project
      ? "이 프로젝트에서 새로운 대화를 시작하고 작업을 이어가세요."
      : "왼쪽에서 프로젝트를 선택하거나 새 프로젝트를 만들어 보세요.";
}

function json(value) {
  const result = JSON.stringify(value, null, 2) ?? "—";
  return result.length > 24000
    ? result.slice(0, 24000) + "\n… 화면 표시를 24,000자로 제한합니다."
    : result;
}

function syncControls() {
  const entering = $("app-screen").hidden && state.screen === "workspace";
  $("login-screen").hidden = state.screen === "workspace";
  $("app-screen").hidden = state.screen !== "workspace";
  $("identity-fields").disabled = state.busy;
  $("workspace").disabled = state.busy || !state.user;
  $("workspace").setAttribute("aria-busy", String(state.busy));
  $("project-fields").disabled = state.busy || !state.project;
  $("session-create-fields").disabled = state.busy || !state.project;
  $("session-fields").disabled = state.busy || !state.session;
  $("project-next").disabled = state.busy || !state.projectCursor;
  $("session-next").disabled = state.busy || !state.sessionCursor;
  $("session-reload").disabled = state.busy || !state.project;
  $("project-delete").disabled = state.busy || !!state.project?.is_default;
  for (const id of [
    "new-chat", "welcome-new-chat", "open-session-create", "open-project-edit",
  ]) $(id).disabled = state.busy || !state.project;
  $("open-session-edit").hidden = !state.session;
  $("page-size").disabled = state.busy || !state.user;
  document.querySelectorAll(".create-fields").forEach((node) => {
    node.disabled = state.busy || !state.user;
  });
  document.querySelectorAll("[data-close-dialog]").forEach((node) => {
    node.disabled = state.busy;
  });
  setSidebar(state.sidebarOpen);
  if (entering) $("view-title").focus({ preventScroll: true });
  window.dispatchEvent(new Event("chat:controls"));
}

async function run(action) {
  if (state.busy) return;
  state.busy = true;
  syncControls();
  notice("API 요청을 처리하고 있습니다…");
  try {
    notice((await action()) || "조회했습니다.", "success");
  } catch (error) {
    if (!state.user) {
      $("identity-summary").textContent = "연결된 사용자가 없습니다.";
    }
    notice(error.message || "요청을 처리하지 못했습니다.", "error");
  } finally {
    state.busy = false;
    syncControls();
  }
}

function showHistory(id) {
  const entry = state.history.find((item) => item.id === id);
  $("request-json").textContent = entry ? json(entry.request) : "—";
  $("response-json").textContent = entry ? json(entry.response) : "—";
  $("response-meta").textContent = entry
    ? `HTTP ${entry.response.status} · ${entry.elapsed} ms · request_id: `
      + (entry.response.requestId || "없음")
    : "HTTP 상태와 request_id가 표시됩니다.";
}

function renderHistory() {
  $("history-count").textContent = String(state.history.length);
  const select = $("history-select");
  select.replaceChildren();
  if (!state.history.length) {
    select.add(new Option("아직 호출 기록이 없습니다.", ""));
  }
  for (const entry of state.history) {
    const path = new URL(entry.request.url).pathname;
    const label = `#${entry.id} · ${entry.response.status} · `
      + `${entry.request.method} ${path}`;
    select.add(new Option(label, entry.id));
  }
  showHistory(select.value);
}

async function api(method, path, options = {}) {
  const identityEpoch = state.identityEpoch;
  const url = new URL(path, apiBase);
  const headers = { Accept: "application/json", ...options.headers };
  if (state.user) headers["X-User-UUID"] = state.user.user_uuid;
  if (options.body !== undefined) {
    headers["Content-Type"] = "application/json";
  }
  if (options.key) headers["Idempotency-Key"] = options.key;
  const entry = {
    id: String(++state.sequence),
    request: { method, url: url.href, headers, body: options.body ?? null },
    response: { status: "NETWORK_ERROR", requestId: null, body: null },
  };
  const started = performance.now();
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), 30000);
  try {
    const response = await fetch(url, {
      method,
      headers,
      body: options.body === undefined
        ? undefined : JSON.stringify(options.body),
      credentials: "same-origin",
      cache: "no-store",
      signal: controller.signal,
    });
    const text = await response.text();
    let body = null;
    if (text) {
      try { body = JSON.parse(text); } catch { body = text; }
    }
    entry.response = {
      status: response.status,
      requestId: response.headers.get("X-Request-Id"),
      body,
    };
    if (!response.ok) {
      let message = body?.message || response.statusText || "요청 실패";
      if (body?.code === "VERSION_CONFLICT") {
        message += " ‘선택 다시 조회’ 후 변경 내용을 확인하고 수정하세요.";
      }
      throw new Error(`HTTP ${response.status} · ${message}`);
    }
    return body;
  } catch (error) {
    if (entry.response.status === "NETWORK_ERROR") {
      const message = controller.signal.aborted
        ? "30초 내 응답을 받지 못했습니다."
        : "네트워크 연결 또는 서버 상태를 확인하세요.";
      entry.response.body = { message };
      throw new Error(
        message + " 생성 요청이었다면 목록에서 반영 여부를 확인하세요. "
        + "같은 본문으로 재시도하면 기존 Idempotency-Key를 사용합니다.",
      );
    }
    throw error;
  } finally {
    clearTimeout(timer);
    entry.elapsed = Math.round(performance.now() - started);
    if (identityEpoch === state.identityEpoch) {
      state.history.unshift(entry);
      state.history = state.history.slice(0, 20);
      renderHistory();
    }
  }
}

function requestKey() {
  const bytes = crypto.getRandomValues(new Uint8Array(16));
  return "dev-ui-" + Array.from(
    bytes, (value) => value.toString(16).padStart(2, "0"),
  ).join("");
}

async function createResource(kind, body) {
  const signature = JSON.stringify([state.user.user_uuid, kind, body]);
  // Keep keys after ambiguous failures; never silently retry a mutation.
  let key = state.pendingCreates.get(signature);
  if (!key) {
    key = requestKey();
    state.pendingCreates.set(signature, key);
  }
  const resource = await api("POST", kind, { body, key });
  state.pendingCreates.delete(signature);
  return resource;
}

function resourcePath(kind, id) {
  return kind + "/" + encodeURIComponent(id);
}

function mergePage(previous, items, key) {
  return Array.from(new Map(
    [...previous, ...items].map((item) => [item[key], item]),
  ).values());
}

function drawList(kind) {
  const project = kind === "project";
  const items = project ? state.projects : state.sessions;
  const selected = project ? state.project : state.session;
  const idKey = project ? "project_id" : "session_id";
  const container = $(kind + "-list");
  container.replaceChildren();
  if (!items.length) {
    const empty = document.createElement("p");
    empty.className = "empty";
    empty.textContent = !state.user
      ? "사용자를 먼저 불러오세요."
      : !project && !state.project
        ? "프로젝트를 먼저 선택하세요." : "표시할 항목이 없습니다.";
    container.append(empty);
  }
  for (const item of items) {
    const button = document.createElement("button");
    button.type = "button";
    button.className = "resource";
    button.dataset.id = item[idKey];
    button.title = project ? item.name : item.title;
    button.setAttribute(
      "aria-pressed", String(selected?.[idKey] === item[idKey]),
    );
    const label = document.createElement("span");
    label.className = "resource-name";
    label.textContent = project ? item.name : item.title;
    const icon = document.createElement("span");
    icon.className = "resource-icon";
    icon.setAttribute("aria-hidden", "true");
    icon.textContent = project ? "▱" : "◌";
    button.append(icon, label);
    if (project && item.is_default) {
      const badge = document.createElement("span");
      badge.className = "badge";
      badge.textContent = "기본";
      button.append(badge);
    }
    button.addEventListener("click", () => run(
      () => project ? chooseProject(item[idKey]) : chooseSession(item[idKey]),
    ));
    container.append(button);
  }
  $(kind + "-count").textContent = `${items.length}개 표시`;
}

function drawProject() {
  const item = state.project;
  $("project-selection").textContent = item
    ? item.project_id : "선택되지 않았습니다.";
  $("project-name").value = item?.name || "";
  $("project-description").value = item?.description || "";
  $("project-detail").textContent = item
    ? json(item) : "선택한 프로젝트가 없습니다.";
  $("project-version").textContent = item
    ? `version ${item.version} · `
      + (item.is_default ? "기본 프로젝트는 삭제할 수 없습니다."
        : "수정 시 이 version을 자동 전송합니다.")
    : "수정 시 조회한 version을 자동으로 전송합니다.";
  $("session-parent").textContent = item
    ? item.name : "프로젝트를 먼저 선택하세요.";
  drawList("project");
  renderShell();
}

function drawSession() {
  const item = state.session;
  $("session-selection").textContent = item
    ? item.session_id : "선택되지 않았습니다.";
  $("session-title").value = item?.title || "";
  $("session-detail").textContent = item
    ? json(item) : "선택한 세션이 없습니다.";
  $("session-version").textContent = item
    ? `version ${item.version} · 수정 시 이 version을 자동 전송합니다.`
    : "메시지 및 에이전트 실행은 이 화면의 범위에 포함되지 않습니다.";
  drawList("session");
  renderShell();
  window.dispatchEvent(new Event("chat:context"));
}

function clearSessions() {
  state.session = null;
  state.sessions = [];
  state.sessionCursor = null;
  $("session-create").reset();
  drawSession();
}

async function loadProjects(append = false) {
  const params = new URLSearchParams({ limit: $("page-size").value });
  if (append && state.projectCursor) {
    params.set("cursor", state.projectCursor);
  }
  const page = await api("GET", "projects?" + params);
  state.projects = mergePage(
    append ? state.projects : [], page.items, "project_id",
  );
  state.projectCursor = page.next_cursor;
  drawList("project");
}

async function loadSessions(append = false) {
  if (!state.project) return;
  const params = new URLSearchParams({
    project_id: state.project.project_id,
    limit: $("page-size").value,
  });
  if (append && state.sessionCursor) {
    params.set("cursor", state.sessionCursor);
  }
  const page = await api("GET", "sessions?" + params);
  state.sessions = mergePage(
    append ? state.sessions : [], page.items, "session_id",
  );
  state.sessionCursor = page.next_cursor;
  drawList("session");
}

async function chooseProject(id) {
  const item = await api("GET", resourcePath("projects", id));
  state.project = item;
  clearSessions();
  drawProject();
  await loadSessions();
  closeMobileSidebar();
  return "프로젝트를 조회했습니다.";
}

async function chooseSession(id) {
  state.session = await api("GET", resourcePath("sessions", id));
  drawSession();
  closeMobileSidebar();
  return "세션을 조회했습니다.";
}

function form(id, action) {
  $(id).addEventListener("submit", (event) => {
    event.preventDefault();
    run(action);
  });
}

function click(id, action) {
  $(id).addEventListener("click", () => run(action));
}

function resetWorkspace() {
  state.identityEpoch += 1;
  document.querySelectorAll("dialog[open]").forEach((node) => node.close());
  state.screen = "login";
  state.user = null;
  state.project = null;
  state.projects = [];
  state.projectCursor = null;
  state.pendingCreates.clear();
  state.history = [];
  state.sequence = 0;
  $("project-create").reset();
  $("user-uuid").value = "";
  $("identity-summary").textContent = "연결된 사용자가 없습니다.";
  clearSessions();
  drawProject();
  renderHistory();
}

form("me-form", async () => {
  const employee = $("employee-id").value.trim();
  if (!employee) throw new Error("사번을 입력해 주세요.");
  resetWorkspace();
  $("identity-summary").textContent = "사용자를 불러오고 있습니다.";
  const home = await api("POST", "me", {
    headers: { "X-User-Id": employee },
  });
  state.user = home.user;
  $("user-uuid").value = home.user.user_uuid;
  $("identity-summary").textContent =
    `사번 ${home.user.user_id} · 상태 ${home.user.status}`;
  await loadProjects();
  await chooseProject(home.default_project.project_id);
  state.screen = "workspace";
  return "사용자를 연결하고 기본 프로젝트를 선택했습니다.";
});

form("project-create", async () => {
  const item = await createResource("projects", {
    name: $("new-project-name").value.trim(),
    description: $("new-project-description").value.trim() || null,
  });
  $("project-create").reset();
  closeDialog("project-create-dialog");
  state.project = item;
  clearSessions();
  drawProject();
  await loadProjects();
  await loadSessions();
  closeMobileSidebar();
  return "프로젝트를 생성했습니다.";
});

form("project-edit", async () => {
  state.project = await api(
    "PATCH", resourcePath("projects", state.project.project_id),
    { body: {
      version: state.project.version,
      name: $("project-name").value.trim(),
      description: $("project-description").value.trim() || null,
    } },
  );
  drawProject();
  await loadProjects();
  closeDialog("project-dialog");
  return "프로젝트를 수정했습니다.";
});

async function startSession(title = "") {
  const body = { project_id: state.project.project_id };
  if (title) body.title = title;
  state.session = await createResource("sessions", body);
  $("session-create").reset();
  closeDialog("session-create-dialog");
  drawSession();
  await loadSessions();
  closeMobileSidebar();
  return "세션을 생성했습니다.";
}

form("session-create", () => startSession(
  $("new-session-title").value.trim(),
));
click("new-chat", () => startSession());
click("welcome-new-chat", () => startSession());

form("session-edit", async () => {
  state.session = await api(
    "PATCH", resourcePath("sessions", state.session.session_id),
    { body: {
      version: state.session.version,
      title: $("session-title").value.trim(),
    } },
  );
  drawSession();
  await loadSessions();
  closeDialog("session-dialog");
  return "세션을 수정했습니다.";
});

click("project-reload", () => loadProjects());
click("session-reload", () => loadSessions());
click("project-next", () => loadProjects(true));
click("session-next", () => loadSessions(true));
click("project-refresh", () => chooseProject(state.project.project_id));
click("session-refresh", async () => {
  const message = await chooseSession(state.session.session_id);
  window.dispatchEvent(new Event("chat:refresh"));
  return message;
});

click("project-delete", async () => {
  if (!confirm(
    `‘${state.project.name}’ 프로젝트를 삭제할까요?\n`
    + "하위 세션도 접근할 수 없게 됩니다. 데이터는 논리 삭제됩니다.",
  )) return "삭제를 취소했습니다.";
  await api("DELETE", resourcePath("projects", state.project.project_id));
  closeDialog("project-dialog");
  state.project = null;
  clearSessions();
  drawProject();
  await loadProjects();
  return "프로젝트를 삭제했습니다.";
});

click("session-delete", async () => {
  if (!confirm(`‘${state.session.title}’ 세션을 삭제할까요?`)) {
    return "삭제를 취소했습니다.";
  }
  await api("DELETE", resourcePath("sessions", state.session.session_id));
  closeDialog("session-dialog");
  state.session = null;
  drawSession();
  await loadSessions();
  return "세션을 삭제했습니다.";
});

$("page-size").addEventListener("change", () => run(async () => {
  await loadProjects();
  await loadSessions();
}));
$("history-select").addEventListener("change", (event) => {
  showHistory(event.target.value);
});
$("clear-history").addEventListener("click", () => {
  state.history = [];
  renderHistory();
});

for (const id of ["open-project-create", "welcome-new-project"]) {
  $(id).addEventListener("click", () => openDialog("project-create-dialog"));
}
$("open-session-create").addEventListener("click", () => {
  openDialog("session-create-dialog");
});
$("open-project-edit").addEventListener("click", () => {
  drawProject();
  openDialog("project-dialog");
});
$("open-session-edit").addEventListener("click", () => {
  drawSession();
  openDialog("session-dialog");
});
$("open-inspector").addEventListener("click", () => {
  openDialog("inspector-dialog");
});
document.querySelectorAll("[data-close-dialog]").forEach((button) => {
  button.addEventListener("click", () => button.closest("dialog").close());
});
document.querySelectorAll("dialog").forEach((dialog) => {
  dialog.addEventListener("cancel", (event) => {
    if (state.busy) event.preventDefault();
  });
});
$("sidebar-toggle").addEventListener("click", () => {
  setSidebar(!state.sidebarOpen);
  if (state.sidebarOpen && mobile.matches) $("new-chat").focus();
});
$("sidebar-backdrop").addEventListener("click", () => {
  setSidebar(false);
  $("sidebar-toggle").focus();
});
document.addEventListener("keydown", (event) => {
  if (event.key === "Escape" && mobile.matches && state.sidebarOpen
      && !document.querySelector("dialog[open]")) {
    setSidebar(false);
    $("sidebar-toggle").focus();
  }
});
mobile.addEventListener("change", () => setSidebar(!mobile.matches));
$("switch-user").addEventListener("click", () => {
  if (state.busy) return;
  resetWorkspace();
  notice("다른 사번을 입력해 작업 공간을 열 수 있습니다.");
  $("employee-id").value = "";
  syncControls();
  $("employee-id").focus();
});

installChat({ $, state, api, apiBase, requestKey, notice });
drawProject();
drawSession();
syncControls();
