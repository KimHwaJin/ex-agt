# 세션 기반 Agent — 전환 2A/2B/2C

`build_session_graph()`는 기존 분석 업무 노드와 `src/worker`를 연결하는
실제 LangGraph 정의다. `session_id = thread_id`이며, 한 세션에서 여러 Task를
순차 수행한다. **아직 운영 진입점으로 전환한 단계는 아니다.**

## 소스 역할

| 파일 | 역할 |
|---|---|
| `graph/state.py` | 신뢰된 Task 입력과 세션/작업 상태 경계 |
| `graph/builder.py` | 기존 업무 경로 + 수락/등록/영수증 노드 구성 |
| `graph/nodes.py` | 작업 초기화, 업무 노드 연결, 실행 binding 등록 |
| `session.py` | 시작·사용자 승인 검증과 저수준 guard 호출 |
| `admission/` | API 요청 영속 접수·직접 invoke·중단 복구 루프 |
| `services.py` | 요청 복구를 적용한 세션 그래프 업무 서비스 |
| `effects/` | Executor 요청·응답 기록과 멱등 DB 반영 |
| `integrations/langgraph_adapter.py` | Worker 이벤트를 실행 대기 지점에 전달 |
| `integrations/worker_hooks.py` | 운영 그래프 factory와 이벤트별 연결 지점 |
| `worker_main.py` | Worker 프로세스 자원 생성·종료; 아직 factory 미연결 |

공통 소비기·Inbox·Outbox에는 Agent 전용 import나 상태 필드를 추가하지 않았다.
현재 업무 노드/서비스와 공통 topology는 `ex_agent`에서 재사용한다. 업무 모듈의
최종 이동이 끝나기 전에는 `ex_agent`를 삭제하면 안 된다.

## State 경계

- `workflow`: 현재 Task의 계획, 실행, 위험 판정, 결과, 리포트. 새 Task가 시작하면
  전체 객체를 새로 만든다. 이전 Task에만 있던 필드도 남지 않는다.
- `messages`: 세션 대화 기록. 메시지 ID로 중복 결과 추가를 방지한다.
- `task_requests`: 이미 시작한 Task의 입력 해시. 같은 Task의 입력 바꿔치기와
  과거 Task 재시작을 거부한다. 현재 Task의 같은 시작 요청은 상태 조회로 처리한다.
- `ew_pending`, `ew_receipts`, `ew_sequences`: 워커 수락 정보, 처리 영수증,
  Execution별 순번. 다음 Task에서도 영수증과 순번을 보존한다.
- `api_receipts`, `invocation_owner`: API 입력 수락 증거와 미완료 호출의 소유자.
  이전 승인 복구가 다음 승인이나 Worker 후속 실행을 건드리지 않게 한다.
- 최상위 `active_task_id`, `execution_id`: 어댑터가 현재 실행을 식별하는 필드.
  실제 업무 상세는 `workflow`에 둔다.

대화 기록 보존과 LLM에 과거 대화를 전달하는 정책은 다르다. 현재 재사용하는
기존 질의·분류 서비스는 현재 `user_message`를 사용한다. 과거 대화 컨텍스트 구성과
기록/영수증의 보존 기간·압축 정책은 후속 작업이다. 영수증을 임의로 삭제하지 않는다.

## 이벤트 처리 경계

```text
계획 승인 → submit_execution → register_execution → wait_external_signal
                                                        ↓ 이벤트 수락 저장
                                              reconcile_executor
                                                        ↓ 결과 저장
                                              record_event_receipt
                                                        ↓
                             다음 셀 / 재승인 / 종료 확인 / 성공 리포트
```

제출 결과를 먼저 checkpoint하므로 binding 등록 실패 시 등록 노드만 복구할 수 있다.
제출 응답이 checkpoint되기 전 장애는 `SessionWorkflowServices`의 요청 기록으로
동일 제출을 재시도할 수 있다. API 호출 중단은 2C의 요청 접수 기록과 복구 루프가
담당한다. 이벤트 수락 후 장애는 Worker가 같은 action을 확인하고 `ainvoke(None)`으로
미완료 노드를 이어 간다. 영수증 이후 리포트 등 후속 노드가 실패해도 같은 규칙이다.
이미 사용자 승인에 도달했다면 이전 이벤트를 다시 승인 응답으로 넣지 않는다.

`interrupt()` 재개 시 해당 노드가 다시 시작되는 점을 고려해 수락 노드와 외부 효과를
분리했다. 근거: [LangGraph interrupts](https://docs.langchain.com/oss/python/langgraph/interrupts).

## 연결 방식

API와 Worker가 동일한 factory를 사용하며 동일한 세션 checkpoint DB를 공유해야 한다.
아래는 자원이 이미 준비된 호스트에서의 연결 조각이다. 독립 실행 스크립트가 아니다.

```python
from agent.graph import build_session_graph, checkpoint_serializer
from agent.admission.service import AdmissionService
from agent.admission.store import RequestStore
from agent.integrations.langgraph_adapter import SessionGraphAdapter
from agent.services import SessionWorkflowServices

# saver는 checkpoint_serializer()를 사용해 생성한다.
# repository와 agent_session_factory는 같은 Agent DB를 가리킨다.
services = SessionWorkflowServices(
    settings,
    repository,
    executor_client,
    registry,
    sessions=agent_session_factory,
)
graph = build_session_graph(services, worker.bindings, checkpointer=saver)
api_calls = AdmissionService(
    graph, worker.guard, RequestStore(agent_session_factory)
)
event_handler = SessionGraphAdapter(graph)
```

`AsyncPostgresSaver.from_conn_string(url, serde=checkpoint_serializer())`로
생성한다. 스키마 setup은 배포 Job에서 수행한다. 위 모듈을 쓰려고 구 Task 기반
체크포인트에 session ID를 덮어쓰거나 기존 실행 중인 Worker와 새 Worker를
동시에 같은 업무에 연결하면 안 된다.

`AdmissionService`는 API가 인증·소유권을 검증한 `TaskTurn`과 request_id를 받는다.
START의 Task와 접수 기록을 함께 저장하므로 기존 create_task를 먼저 부르지 않는다.
사용자 응답에는 현재 interrupt ID도 필요하다. 직접 graph State를 HTTP 입력으로
받지 않는다. Dispatcher는 이미 guard를 잡으므로 핸들러에서 잠금을 중첩하지 않는다.
호스트의 복구 루프 실행과 상세 사용법은 [API 접수 안내](admission/README.md)에 있다.
`SessionCoordinator`는 저수준 검증용이며 이 내구성 있는 접수 경로를 대체하지 않는다.

## 배포 전 남은 필수 작업

1. 2B의 `SessionWorkflowServices`를 공통 운영 factory에 연결한다. submit/append/
   finalize/cancel/report의 고정 요청과 멱등 결과 반영은 구현했다.
   Agent DB에 `0007_executor_effects`를 적용해야 한다. 요청 복구 계약은
   [효과 모듈 안내](effects/README.md)를 참고한다.
2. 구현한 `admission/`을 API와 호스트 lifecycle에 연결하고 Agent DB에
   `0008_api_requests`를 적용한다. 접수·수락 영수증·재개 루프는 2C에 포함한다.
   재시도 한도 초과는 현재 BLOCKED로 유지한다. 최종 실패 보상 연결은 남아 있다.
3. 운영 `worker_hooks.create_graph` 자원 연결과 최종 실패 알림/실행 취소 보상을
   새 업무 서비스에 맞춘다. handler가 최종 실패했을 때의 업무 정리도 필요하다.
4. 장기 세션 채팅 금지는 업무 DB에서, 짧은 호출 상호배제는 SessionGuard에서
   담당한다. API·Worker 양쪽에서 이 정책을 유지하고 성공 리포트 저장까지 잠근다.
5. FastAPI/Agent Chat UI·배포 진입점을 전환하고 실제 Executor·모델을 검증한 뒤
   구 Worker와 임시 import를 제거한다. 그 전에 새 그래프의 비최종 상태와
   current_interrupt를 화면용 Task DB에 반영하는 호스트 연결도 필요하다.

2A는 대체 업무 서비스를, 2B/2C는 실제 외부 요청·업무 DB·접수 복구 구현을 검증한다.
LangGraph·PostgreSQL·Redis는 실제 구현이며 모델·Executor HTTP는 대역이다.
실제 LLM 생성 코드나 Executor/Jupyter 실행의 종단 검증으로 해석하지 않는다.

전환 전체 계획은 [프로젝트 전환 기록](../../docs/worker-centered-refactor.md),
공통 워커 기능은 [Worker 안내](../worker/README.md)를 참고한다.
