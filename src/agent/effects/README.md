# Agent 외부 요청 복구 — 전환 2B

이 모듈은 `SessionWorkflowServices`가 Executor에 보내는 요청을 고정하고,
같은 그래프 노드가 재실행돼도 같은 실행 결과로 수렴하게 한다.
공통 `worker`의 Inbox/Outbox와 별개인 **Agent 업무 기록**이다.
새 Redis 스트림·컨슈머·발행 단계는 추가하지 않는다.

## 필요한 이유와 복구 경계

LangGraph 노드의 외부 HTTP 호출, Agent 업무 DB, checkpoint는 한 트랜잭션이
아니다. Executor가 받아들인 요청의 응답을 잃거나, DB 반영 후 checkpoint 전에
중단되면 노드가 다시 실행될 수 있다.

| 중단 지점 | 같은 입력으로 재실행했을 때 |
|---|---|
| 요청 기록 전 | 준비를 다시 수행; 아직 Executor에 보내지 않음 |
| 요청 기록 후, 응답 기록 전 | 저장된 경로·본문·코드/Markdown·멱등 키로 HTTP 재요청 |
| 응답 기록 후, 업무 DB 반영 전 | 저장된 응답으로 업무 DB만 반영 |
| 업무 DB 반영 후, checkpoint 전 | 동일 DB 결과를 재사용; 순번/메시지/계획 중복 방지 |

HTTP가 정확히 한 번만 호출된다는 보장은 아니다. Executor의 동일 키·동일 입력
멱등 처리와 Agent의 기록을 결합한다. Executor의 영수증 보존 기간보다 오래된
불확실한 요청을 자동 재전송해서는 안 된다. 기존 키를 지우거나 새 키로 바꿔서
오류를 우회하지 않는다.

## 파일별 책임

| 파일 | 기능 |
|---|---|
| `models.py` | 요청·응답, 입력/요청 해시, Task FK, 생성·수정 시각/주체 |
| `store.py` | 첫 요청 고정, 키의 입력 불일치·기록 변조 검사, 응답 저장 |
| `journal.py` | 준비 → 저장 → HTTP → 응답 저장; DB 연결을 잡고 HTTP/LLM을 기다리지 않음 |
| `runner.py` | PATH 파일 복원, HTTP 전송, Execution/Operation 응답 검증 |
| `files.py` | 내용 해시 기반 입력 경로, 승인 코드 해시 확인, 저장된 원문 복원 |
| `plans.py` | 스레드로 컴파일/파일 IO 분리, 기대 revision 기반 중복 저장 방지 |
| `execution.py` | submit·MULTI append·finalize·cancel 요청 고정 및 결과 반영 |
| `reporting.py` | 성공 결과 검증, 최초 생성 Markdown 고정, 아티팩트 응답 재사용 |
| `projections.py` | 실행 binding·순번·최종 메시지·승격 안내의 멱등 DB 반영 |

`agent_executor_effects`는 `0007_executor_effects` 마이그레이션이 만든다.
새 ORM metadata는 기존 baseline의 동적 create_all과 분리했다. 빈 DB와 기존
Agent DB 모두 동일한 신규 revision을 적용한다. Worker만 이식하는 개발자는
이 테이블이나 Agent 효과 모듈을 가져갈 필요가 없다.

## 작업 식별

- 제출: Task + 승인 계획 revision 번호.
- 추가 셀: Task + 직전 Operation ID. DB의 가변 다음 셀 순번으로 키를 만들지 않는다.
- finalize/cancel/report: Task + 행위 종류. 같은 키에 다른 입력을 주면 거부한다.
- 준비 시점의 expected_version과 셀 sequence는 요청 안에 저장한다.
  결과 조회로 DB 버전이 올라가더라도 재요청 본문을 다시 만들지 않는다.
- 추가 셀의 새 계획 ID·revision·원문 계획을 서비스 응답에 넣고 checkpoint한다.
  사용자 재승인/수정 후에는 그래프의 현재 계획을 사용한다.
- 코드/Markdown의 PATH는 내용 해시를 포함한다. 같은 경로에 다른 생성 결과를
  덮어쓰지 않으며, 요청 준비 후 파일이 없어지면 기록된 원문으로 복구한다.

요청 기록에 코드·Markdown 원문과 파라미터가 포함된다. 일반 진행 로그에 원문을
출력하지 않는다. 보존/삭제 정책은 Task·checkpoint·Executor 영수증·공유 파일을
함께 고려해 후속 운영 기능으로 설계해야 한다.

## 서비스 연결

아래는 자원을 이미 준비한 API/Worker 호스트의 조립 예시다.
현재 운영 factory를 자동으로 활성화하는 스크립트는 아니다.

```python
from agent.graph import build_session_graph
from agent.services import SessionWorkflowServices

services = SessionWorkflowServices(
    settings,
    repository,
    executor_client,
    registry,
    sessions=agent_session_factory,
)
graph = build_session_graph(services, worker.bindings, checkpointer=saver)
```

`agent_session_factory`는 repository와 같은 Agent DB를 가리켜야 한다.
API와 Worker는 같은 session checkpoint DB와 SessionGuard를 사용한다.
예제의 services는 사용자 HTTP 바디를 직접 받는 공개 API가 아니다.
승인 확인·세션 잠금·이벤트 검증을 하는 그래프/호스트 경계를 유지한다.

## 이번 단계가 해결하지 않는 것

- 이 모듈 자체는 스케줄러가 아니다. API 장애 시 최초 제출을 재개하는 접수 기록과
  복구 루프는 2C의 [admission 모듈](../admission/README.md)에 있으며 호스트 연결은 남아 있다.
- HTTP 실패/409 등을 새 계획이나 새 키로 자동 변환하지 않는다. 확정 실패 시
  Executor 취소 확인·사용자 안내·장기 세션 잠금 정리는
  [실패 보상 모듈](../failure/README.md)이 담당한다.
- cancel HTTP 접수는 취소 완료가 아니다. 기존 그래프가 종료 이벤트/결과를
  확인한 뒤 사용자에게 취소 완료를 알리는 흐름을 유지한다.
- 보고서는 성공 Execution에 대해서만 생성한다. 실패/취소 보고서를 만들지 않는다.
- 2C에서 Q&A 최종 답변도 멱등 반영한다. 중간 상태 이벤트 등 모든 업무 쓰기를
  멱등화한 것은 아니다.
- 기존 `DefaultWorkflowServices` 진입점은 아직 이 서비스를 사용하지 않는다.
  `worker_hooks.create_graph`의 미연결 보호와 기존 운영 배포를 유지한다.

## 회귀 테스트

- `tests/agent/test_effects.py`: 요청 해시, 파일, 응답 검증, HTTP 재시도 경계.
- `tests/agent/test_effects_postgres.py`: 실제 PG 기반 중복·응답 유실·DB 장애.
- `tests/worker/test_agent_effect_recovery.py`: 실제 PG/Redis/체크포인트에서
  MULTI binding 반영 후 중단 및 최종 메시지 저장 후 중단 복구.

Executor HTTP는 동일 키 영수증을 구현한 대역, 모델은 결정적 출력 대역이다.
실제 Jupyter 코드 실행·LLM 품질·K8s 강제 종료 검증을 대신하지 않는다.
