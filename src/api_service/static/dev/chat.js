/* Development-only chat client. No raw HTML from agent output is rendered. */
export function installChat({ $, state, api, apiBase, requestKey, notice }) {
  const terminal = new Set(["completed", "rejected", "failed", "cancelled"]);
  const labels = {
    queued: "접수 대기", running: "처리 중", awaiting_input: "사용자 응답 대기",
    waiting_execution: "실행 결과 대기", cancelling: "취소 확인 중",
    completed: "완료", rejected: "계획 거절", failed: "실패", cancelled: "취소 완료",
  };
  let backend = "disabled";
  let contextKey = "";
  let epoch = 0;
  let messages = new Map();
  let messageCursor = null;
  let current = null;
  let pending = null;
  let controller = null;
  let eventId = null;
  let busy = false;
  let restoring = false;
  let retryTimer = null;
  let retryCount = 0;
  let failedSubmission = null;

  function controls() {
    const active = current ? !terminal.has(current.status)
      : !!state.session?.active_run_id;
    const disabled = backend === "disabled" || !state.session || state.busy
      || busy || restoring || active;
    $("message-draft").disabled = !!disabled;
    $("send-message").disabled = !!disabled;
    $("message-draft").placeholder = backend === "demo"
      ? "[DEMO] 흐름 테스트 메시지 · 실제 분석은 실행되지 않습니다"
      : backend === "langgraph" ? "메시지를 입력하세요"
      : "실행기가 연결되지 않았습니다";
    $("agent-mode").textContent = backend === "demo"
      ? "DEMO · 실제 LLM/Executor 미연결"
      : backend === "langgraph" ? "Agent · 대화·계획 검토 / 실행 미연결"
      : "에이전트 미연결";
    $("composer-note").textContent = backend === "demo"
      ? "테스트 실행기입니다. 승인·스트림·취소를 검증하며 실제 분석은 하지 않습니다."
      : backend === "langgraph"
      ? "질문에 답변하고 작업 요청의 계획과 셀 코드를 준비합니다. 실제 실행은 아직 미연결입니다."
      : "메시지 실행 기능을 사용하려면 실행기를 설정해 주세요.";
    $("run-panel").hidden = !current;
    $("run-status").textContent = current
      ? `${labels[current.status] || current.status} · Run ${current.run_id}`
      : "";
    $("cancel-run").hidden = !active;
    $("cancel-run").disabled = busy || state.busy
      || current?.status === "cancelling";
    $("observe-run").hidden = !active || !!controller
      || (current?.status === "awaiting_input" && !!pending);
    $("older-messages").hidden = !messageCursor;
    $("older-messages").disabled = busy || restoring;
    document.querySelectorAll("#hitl-panel button").forEach((button) => {
      button.disabled = busy || state.busy;
    });
  }

  function renderMessages() {
    const items = Array.from(messages.values()).sort((a, b) =>
      a.created_at.localeCompare(b.created_at)
      || a.message_id.localeCompare(b.message_id));
    $("chat-messages").replaceChildren();
    for (const item of items) {
      const article = document.createElement("article");
      article.className = "chat-message " + item.role;
      article.dataset.messageId = item.message_id;
      const caption = document.createElement("p");
      caption.className = "message-caption";
      caption.textContent = `${item.role === "user" ? "나" : "에이전트"}`
        + ` · ${item.status}`;
      article.append(caption);
      for (const block of item.content) {
        const text = document.createElement("div");
        text.className = "message-content";
        text.textContent = block.type === "text"
          ? block.text : "지원되지 않는 메시지 블록";
        article.append(text);
      }
      $("chat-messages").append(article);
    }
    $("chat-history").hidden = !items.length;
    document.querySelector(".welcome").hidden = !!items.length;
  }

  function renderPending() {
    $("hitl-panel").replaceChildren();
    if (!pending || current?.status !== "awaiting_input") return;
    const title = document.createElement("p");
    title.textContent = pending.payload.title;
    const plan = document.createElement("div");
    plan.dataset.version = pending.payload.plan_version;
    const version = document.createElement("p");
    version.textContent = `계획 버전 ${pending.payload.plan_version}`;
    const steps = document.createElement("ol");
    for (const step of pending.payload.steps) {
      const item = document.createElement("li");
      item.textContent = typeof step === "string" ? step
        : `${step.description} — 이유: ${step.reason}`
          + ` / 예상 산출물: ${step.expected_result}`;
      if (step.tool_id) {
        const tool = document.createElement("p");
        tool.textContent = `Skill: ${step.skill_id}@${step.skill_version}`
          + ` / Tool: ${step.tool_id}@${step.tool_version}`;
        item.append(tool);
      }
      if (step.parameters) {
        const parameters = document.createElement("p");
        parameters.textContent = "파라미터: " + JSON.stringify(step.parameters)
          + (step.input_step ? ` / 입력: ${step.input_step}번 셀 결과` : "");
        item.append(parameters);
      }
      steps.append(item);
    }
    plan.append(version, steps);
    if (pending.payload.summary) {
      const summary = document.createElement("p");
      summary.textContent = pending.payload.summary;
      plan.prepend(summary);
    }
    if (pending.payload.notice) {
      const warning = document.createElement("p");
      warning.textContent = pending.payload.notice;
      plan.prepend(warning);
    }
    if (pending.payload.implementation) {
      const method = document.createElement("p");
      method.textContent = "구현 방식: "
        + (pending.payload.implementation === "generated_code"
          ? "직접 코드 작성" : "Skill·Tool 카탈로그 조합");
      plan.append(method);
    }
    for (const text of pending.payload.generation_risk?.warnings ?? []) {
      const warning = document.createElement("p");
      warning.textContent = `생성 전 위험 검토: ${text}`;
      plan.append(warning);
    }
    if (pending.payload.instruction) {
      const change = document.createElement("p");
      change.textContent = `수정 요청: ${pending.payload.instruction}`;
      plan.append(change);
    }
    const instruction = document.createElement("textarea");
    instruction.id = "hitl-instruction";
    instruction.maxLength = 8000;
    instruction.placeholder = "수정 요청 시 변경할 내용을 입력하세요";
    instruction.setAttribute("aria-label", "실행계획 수정 지시");
    $("hitl-panel").append(title, plan, instruction);
    for (const [decision, label] of [
      ["approve", "승인"], ["modify", "수정 요청"], ["reject", "거절"],
    ]) {
      const button = document.createElement("button");
      button.type = "button";
      button.dataset.decision = decision;
      button.textContent = label;
      button.addEventListener("click", () => {
        const response = { type: "plan_review", decision, schema_version: 1 };
        if (decision === "modify") {
          if (!instruction.value.trim()) {
            notice("수정할 내용을 입력해 주세요.", "error");
            return;
          }
          response.instruction = instruction.value;
        }
        send({ type: "resume", run_id: current.run_id,
          interrupt_id: pending.interrupt_id, response });
      });
      $("hitl-panel").append(button);
    }
    controls();
  }

  function applyEvent(kind, envelope, id) {
    if (id) {
      const next = Number(id.split(":")[1]);
      if (eventId && next <= Number(eventId.split(":")[1])) return;
      eventId = id;
    }
    const data = envelope.data || {};
    const sequence = id ? Number(id.split(":")[1]) : 0;
    if (kind.startsWith("message.")) {
      const old = messages.get(data.message_id);
      if (!old || sequence > old.event_sequence) {
        if (kind === "message.delta") {
          if (old) {
            if (!old.content[data.block_index]) {
              old.content[data.block_index] = { type: "text", text: "" };
            }
            old.content[data.block_index].text += data.delta;
            old.event_sequence = sequence;
          }
        } else {
          messages.set(data.message_id, { ...data, event_sequence: sequence });
        }
        renderMessages();
      }
    }
    if (kind === "run.accepted") {
      current = { ...current, ...data };
      state.session.active_run_id = data.run_id;
    }
    if (kind === "run.status_changed" || terminal.has(kind.slice(4))) {
      current.status = data.status;
    }
    if (kind === "run.progress") $("run-progress").textContent = data.message;
    if (kind === "run.interrupted") {
      current.status = "awaiting_input";
      pending = data;
      renderPending();
    }
    if (current && terminal.has(current.status)) {
      state.session.active_run_id = null;
      state.session.is_locked = false;
      pending = null;
      renderPending();
      $("run-progress").textContent = data.error
        ? data.error.message : labels[current.status];
    }
    controls();
  }

  async function consume(response, token) {
    if (!response.ok) {
      const error = await response.json();
      throw new Error(`HTTP ${response.status} · ${error.message}`);
    }
    const reader = response.body.getReader();
    const decoder = new TextDecoder();
    let buffer = "";
    try {
      while (token === epoch) {
        const { value, done } = await reader.read();
        if (token !== epoch) return;
        if (done) break;
        buffer += decoder.decode(value, { stream: true });
        let boundary;
        while ((boundary = buffer.indexOf("\n\n")) >= 0) {
          const frame = buffer.slice(0, boundary);
          buffer = buffer.slice(boundary + 2);
          let kind = "message", id = null;
          const data = [];
          for (const line of frame.split("\n")) {
            if (line.startsWith("event:")) kind = line.slice(6).trim();
            if (line.startsWith("id:")) id = line.slice(3).trim();
            if (line.startsWith("data:")) data.push(line.slice(5).trim());
          }
          if (!data.length) {
            if (frame.startsWith(":")) retryCount = 0;
            continue;
          }
          const envelope = JSON.parse(data.join("\n"));
          if (kind === "stream.error") throw new Error(envelope.message);
          // An intentional connection rotation is not a failed retry.
          if (kind === "stream.closed") retryCount = 0;
          if (kind !== "stream.closed") {
            retryCount = 0;
            applyEvent(kind, envelope, id);
          }
        }
      }
    } finally {
      await reader.cancel().catch(() => {});
      reader.releaseLock();
    }
  }

  async function observe() {
    if (controller || !current || terminal.has(current.status)
        || (current.status === "awaiting_input" && pending)) return;
    const token = epoch;
    controller = new AbortController();
    controls();
    try {
      const headers = { "X-User-UUID": state.user.user_uuid };
      if (eventId) headers["Last-Event-ID"] = eventId;
      const response = await fetch(new URL(
        `agent/runs/${current.run_id}/stream`, apiBase,
      ), { headers, signal: controller.signal });
      await consume(response, token);
    } catch (error) {
      if (token === epoch && error.name !== "AbortError") {
        notice(error.message + " · 다시 연결하거나 세션을 새로 조회하세요.",
          "error");
      }
    } finally {
      if (token === epoch) {
        controller = null;
        controls();
        scheduleReconnect();
      }
    }
  }

  function scheduleReconnect() {
    if (current && !terminal.has(current.status)
        && !(current.status === "awaiting_input" && pending)
        && retryCount < 3) {
      clearTimeout(retryTimer);
      retryTimer = setTimeout(observe, 500 * (2 ** retryCount++));
    }
  }

  async function send(input) {
    if (busy || !state.session) return;
    busy = true;
    clearTimeout(retryTimer);
    controller?.abort();
    // Isolate the previous observer from a new resume stream.
    epoch += 1;
    const sendEpoch = epoch;
    controller = new AbortController();
    const body = { user_id: state.user.user_uuid,
      session_id: state.session.session_id, stream: true, input };
    const signature = JSON.stringify(body);
    if (failedSubmission?.signature !== signature) {
      failedSubmission = { signature, key: requestKey() };
    }
    controls();
    const submissionController = controller;
    let timedOut = false;
    const timeout = setTimeout(() => {
      timedOut = true;
      submissionController.abort();
    }, 30000);
    try {
      const response = await fetch(new URL("agent/runs", apiBase), {
        method: "POST", signal: controller.signal,
        headers: { "Content-Type": "application/json",
          "X-User-UUID": state.user.user_uuid,
          "Idempotency-Key": failedSubmission.key },
        body: JSON.stringify(body),
      });
      clearTimeout(timeout);
      if (sendEpoch !== epoch) return;
      if (response.ok) {
        current = {
          run_id: response.headers.get("X-Run-Id"), status: "queued",
        };
        if (input.type === "message") eventId = null;
        pending = null;
        $("run-progress").textContent = "";
        renderPending();
        busy = false;
        if (input.type === "message") $("message-draft").value = "";
        controls();
      }
      await consume(response, sendEpoch);
      failedSubmission = null;
      retryCount = 0;
    } catch (error) {
      if (sendEpoch === epoch && timedOut) {
        notice("접수 확인 시간이 초과됐습니다. 같은 입력으로 재시도하거나"
          + " 세션을 다시 조회해 주세요.", "error");
      }
      if (sendEpoch === epoch && error.name !== "AbortError") {
        notice(error.message + " · 같은 입력을 재시도하면 같은 키를 사용합니다.",
          "error");
      }
    } finally {
      clearTimeout(timeout);
      if (sendEpoch === epoch) {
        busy = false;
        controller = null;
        controls();
        scheduleReconnect();
      }
    }
  }

  async function loadMessages(append, token) {
    const params = new URLSearchParams({ limit: "20" });
    if (append && messageCursor) params.set("cursor", messageCursor);
    const page = await api("GET", `sessions/${state.session.session_id}`
      + `/messages?${params}`);
    if (token !== epoch) return;
    for (const item of page.items) {
      const old = messages.get(item.message_id);
      if (!old || old.event_sequence <= item.event_sequence) {
        messages.set(item.message_id, item);
      }
    }
    messageCursor = page.next_cursor;
    renderMessages();
  }

  async function restore(force = false) {
    const key = `${state.user?.user_uuid}:${state.session?.session_id}`;
    if (!force && key === contextKey) { controls(); return; }
    contextKey = key;
    epoch += 1;
    const token = epoch;
    controller?.abort();
    controller = null;
    clearTimeout(retryTimer);
    retryCount = 0;
    busy = false;
    current = null;
    pending = null;
    eventId = null;
    failedSubmission = null;
    messages = new Map();
    messageCursor = null;
    restoring = !!state.session;
    $("message-draft").value = "";
    $("run-progress").textContent = "";
    renderMessages();
    renderPending();
    controls();
    if (!state.session) return;
    try {
      const runs = await api("GET", `sessions/${state.session.session_id}`
        + "/runs?limit=1");
      if (token !== epoch) return;
      if (runs.items.length) {
        const detail = await api("GET", `agent/runs/${runs.items[0].run_id}`);
        if (token !== epoch) return;
        current = detail;
        eventId = current.last_event_id;
        pending = current.pending_interrupts[0] || null;
      }
      // Read messages AFTER the event watermark; message-specific sequence
      // prevents older replay deltas from duplicating already saved text.
      await loadMessages(false, token);
      if (token !== epoch) return;
      renderPending();
      observe();
    } catch (error) {
      if (token === epoch) notice(error.message, "error");
    } finally {
      if (token === epoch) { restoring = false; controls(); }
    }
  }

  $("send-message").addEventListener("click", () => {
    const text = $("message-draft").value;
    if (!text.trim()) return notice("메시지를 입력해 주세요.", "error");
    send({ type: "message", content: [{ type: "text", text }] });
  });
  $("observe-run").addEventListener("click", () => {
    restore(true);
  });
  $("older-messages").addEventListener("click", async () => {
    restoring = true;
    const token = epoch;
    controls();
    try { await loadMessages(true, token); }
    catch (error) { if (token === epoch) notice(error.message, "error"); }
    finally { if (token === epoch) { restoring = false; controls(); } }
  });
  $("cancel-run").addEventListener("click", async () => {
    if (!current || busy || !confirm("실제 작업 취소를 요청할까요?")) return;
    const token = epoch;
    busy = true;
    controls();
    try {
      const receipt = await api("POST", `agent/runs/${current.run_id}/cancel`);
      if (token !== epoch) return;
      current.status = receipt.status;
      pending = null;
      renderPending();
      if (receipt.status === "cancelled") state.session.active_run_id = null;
      observe();
    } catch (error) { if (token === epoch) notice(error.message, "error"); }
    finally { if (token === epoch) { busy = false; controls(); } }
  });
  window.addEventListener("chat:context", () => restore());
  window.addEventListener("chat:refresh", () => restore(true));
  window.addEventListener("chat:controls", controls);
  fetch(new URL("runtime", window.location.href))
    .then((response) => response.json())
    .then((config) => { backend = config.agent_backend; controls(); })
    .catch(() => controls());
  restore();
}
