# 공통 워커 기능 목록

확인 기준일: 2026-08-31. 이 문서는 현재 남아 있는 소스를 기준으로 작성한
인수인계용 기능 참조 문서다. 원본 `ex_agent` 서비스의 전체 기능 목록이 아니다.

워커는 이벤트 수신, DB 보관과 순서 정리, 내부 발행, 핸들러 실행, 복구와
모니터링을 제공한다. LangGraph 연결은 선택적으로 사용하는 참조 구현이다.

정식 소스 편입과 삭제된 운영 도구의 잔여 참조 정리를 완료했다.
실제 Agent 연결은 다음 단계이며, 현재 단계와 검증 범위는
[전환 계획](../worker-centered-refactor.md)을 확인한다.

## 1. Redis 메시지 소비

담당: [consumer.py](../../src/worker/consumer.py)

| 기능 이름 | 기능 설명 |
|---|---|
| 소비자 그룹 초기화 | Redis 소비자 그룹을 생성하고 기존 그룹은 유지한다. |
| 신규 메시지 수신 | Stream에서 새 메시지를 대기하고 읽는다. |
| 동시 처리 제한 | 설정한 슬롯 수만큼 비동기로 처리한다. |
| 미완료 메시지 회수 | ACK하지 못한 메시지를 idle 시간 기준으로 회수한다. |
| 커서 기반 pending 탐색 | 미완료 메시지를 나눠 검색하고 다음 범위를 이어서 탐색한다. |
| ACK 처리 | 핸들러가 완료를 반환하면 해당 그룹에서 처리 완료를 확인한다. |
| 처리 연기 DEFER | ACK 없이 대기하되 소비기 재시도 횟수는 증가시키지 않는다. |
| 메시지 재시도 RETRY | 실패 횟수를 기록하고 한도 미만이면 이후 회수·재처리한다. |
| DLQ 기록 | 영구 오류나 재시도 한도에 도달한 메시지와 원인을 별도 Stream에 기록한다. |
| 처리 중 heartbeat | 처리 중인 pending 메시지의 idle 시간을 주기적으로 갱신한다. |
| 선택적 처리 잠금 | 핸들러가 잠금 키를 제공하면 잠금 획득·갱신·해제를 수행한다. |
| 소비 루프 오류 복구 | Redis 읽기 등의 오류 발생 시 대기 시간을 늘려 재시도한다. |
| 오래된 소비자 정보 정리 | 조회 시 pending이 없고 오래 idle 상태인 소비자 등록 정보를 정리한다. |
| 안전한 종료 대기 | 신규 처리를 멈추고 진행 중 작업을 기다린 뒤 취소를 요청한다. |

- ACK는 해당 그룹의 pending 기록을 해제하는 것이며 Stream 원문 삭제가 아니다.
- RETRY와 DEFER는 즉시 핸들러를 다시 호출하지 않는다. pending 회수로 재실행한다.
- 재시도 한도에는 최초 처리에서 반환된 RETRY도 포함된다.
- 현재 Ingress와 Dispatcher는 소비기의 선택적 잠금을 요청하지 않는다.
  Dispatcher 내부의 SessionGuard가 세션 잠금을 담당한다.
- DLQ 기록 기능은 남아 있다. 삭제된 것은 DLQ 조회·재발행·폐기 관리 도구다.
- 취소는 협조적이다. 핸들러가 취소를 무시하면 종료 유예 시간은 강제 종료
  시한을 보장하지 않는다.

## 2. 이벤트 접수·실행 연결·순서 관리

담당: [contracts.py](../../src/worker/contracts.py),
[ingress.py](../../src/worker/ingress.py),
[store.py](../../src/worker/store.py)

| 기능 이름 | 기능 설명 |
|---|---|
| Executor 이벤트 검증 | 이벤트 ID, 실행 ID, 타입, 순번, 버전, payload 등의 형식을 검사한다. |
| Inbox 저장 | Executor 원본 이벤트를 PostgreSQL에 저장한 뒤 원본 ACK를 허용한다. |
| 수신 이벤트 중복 검사 | 동일 이벤트 재수신을 확인하고 ID·순번이 충돌하는 다른 데이터를 거부한다. |
| Execution 연결 등록 | execution_id를 session_id, task_id와 연결한다. |
| 연결 변경 방지 | 동일 Execution을 다른 세션·Task로 바꾸는 요청을 거부한다. |
| 연결 등록 전 수신 보관 | binding이 아직 없어도 원본 이벤트부터 Inbox에 저장한다. |
| Execution별 순번 관리 | 각 실행의 이벤트를 연속된 순번대로 내부 처리 대상으로 전환한다. |
| 누락 이벤트 보충 | 순번 누락이나 catch-up 필요 시 Executor REST API로 이력을 보충한다. |
| 보충 진행 상태 관리 | 이력 보충 요청·완료 상태를 DB에 기록해 다음 회차에 이어간다. |
| 이벤트 타입별 선별 | 등록된 타입은 Command 생성, 나머지는 IGNORED 기록 후 순번 진행한다. |
| 라우팅 오류 기록 | 순서 복구·이력 조회 오류를 DB에 남기고 이후 다시 시도한다. |
| 원자적 처리 대상 생성 | Inbox 처리 표시, Command와 Outbox 생성을 한 DB 트랜잭션으로 반영한다. |

Ingress는 원본 접수를 담당하고, EventRouter는 DB에 접수된 이벤트를 내부
처리 대상으로 전환한다. 실제 Redis 내부 발행은 Outbox가 담당한다.

순서 관리는 Execution별이다. 서로 다른 Execution 사이의 업무 선후 관계는
호스트 서비스가 관리한다. 한 Task에 여러 Execution을 연결할 수 있지만,
같은 Task의 Execution들이 같은 세션에 속하는지는 호스트가 보장해야 한다.

`EventContext.event`는 payload만이 아니라 Executor 원본 이벤트 전체다.
상세 데이터는 `EventContext.event.payload`에 있다. EventContext는 이 이벤트에
namespace, session_id, task_id, execution_id, command_id를 결합한 핸들러 입력이다.

## 3. Outbox 기반 내부 메시지 발행

담당: [outbox.py](../../src/worker/outbox.py),
[store.py](../../src/worker/store.py)

| 기능 이름 | 기능 설명 |
|---|---|
| 발행 대기 기록 보관 | 아직 Redis에 발행하지 못한 Command를 DB Outbox에 유지한다. |
| 발행 작업 선점 | DB 잠금과 SKIP LOCKED로 여러 워커가 발행 대상을 나눠 가져간다. |
| 선점 만료 복구 | 발행 담당 워커가 종료되면 선점 만료 후 다른 워커가 다시 가져간다. |
| 내부 Command 발행 | namespace, command_id, generation 등을 내부 Stream에 발행한다. |
| 묶음 발행 | Redis pipeline으로 여러 발행 요청의 통신 횟수를 줄인다. |
| 부분 성공 처리 | 묶음 중 성공·실패 항목을 구분해 DB 상태에 반영한다. |
| 불확실한 발행 재처리 | 결과를 확정하지 못하면 동일 Command ID로 재발행할 수 있다. |
| 선행 Command 완료 확인 | 같은 Execution의 앞 Command가 DONE·IGNORED인 경우 다음 것을 발행한다. |
| 발행 확정 소유권 검사 | 자신의 선점 토큰이 유효한 항목만 발행 확정 상태로 변경한다. |

Outbox 재시도는 메시지 발행 재시도다. 핸들러 실행 재시도는 pending 회수가
담당한다. 재발행으로 중복 전달될 수 있으므로 발행을 exactly-once로 보장하지 않는다.

내부 Redis 메시지는 DB 조회용 식별 정보를 담는다. Dispatcher는 command_id로
DB를 조회해 원본 이벤트와 실행 연결을 복원한다.

## 4. 핸들러 실행·중복 처리·세션 보호

담당: [dispatcher.py](../../src/worker/dispatcher.py),
[guard.py](../../src/worker/guard.py),
[store.py](../../src/worker/store.py)

| 기능 이름 | 기능 설명 |
|---|---|
| 내부 메시지 검증 | 메시지 버전, namespace, Command ID, generation을 검사한다. |
| DB 기반 처리 문맥 복원 | Command ID로 이벤트와 실행 연결을 조회해 EventContext를 만든다. |
| 이벤트별 핸들러 호출 | event_type에 등록된 비동기 함수를 실행한다. |
| 완료 Command 중복 방지 | DONE·IGNORED인 Command는 핸들러 재호출 없이 ACK한다. |
| 이전 generation 차단 | 수동 복구 이전 세대의 메시지가 늦게 도착해도 실행하지 않는다. |
| 핸들러 구성 불일치 대기 | 생성된 Command의 핸들러가 현재 워커에 없으면 대기한다. |
| 업무 처리 상태 저장 | READY, RUNNING, DONE, IGNORED, FAILED 상태를 관리한다. |
| 업무 실패 횟수 관리 | 실제 핸들러 실패 횟수를 PostgreSQL에 기록한다. |
| 일시적 대기와 실패 구분 | 잠금 충돌, DB·Redis 오류, DeferEvent 등을 실패 예산에서 제외한다. |
| 명시적 무시·거절 | IgnoreEvent는 처리 생략, RejectEvent는 최종 실패로 처리한다. |
| 실패 후 후속 처리 차단 | 최종 실패 Command가 남으면 같은 Execution의 뒤 Command 발행을 막는다. |
| 세션별 분산 실행 잠금 | 같은 세션의 API·워커 그래프 호출이 겹치지 않도록 공통 잠금을 제공한다. |
| 잠금 소유권 유지 | 소유자 토큰을 확인하며 TTL을 갱신하고 해제한다. |
| 잠금 상실 시 취소 요청 | 잠금을 유지하지 못하면 현재 작업에 취소를 전달한다. |

API도 동일 SessionGuard를 사용해야 API·워커 간 동시 실행을 보호할 수 있다.
핸들러 안에서 이미 잡은 세션 잠금을 다시 획득하지 않는다.

이 잠금은 짧은 그래프 호출 구간의 동시 실행 방지다. 코드 실행이 지속되는
며칠 동안 사용자 채팅을 금지하는 장기 잠금은 호스트 서비스의 별도 책임이다.

## 5. LangGraph 연결: 선택적 참조 구현

담당: [langgraph_adapter.py](../../src/agent/integrations/langgraph_adapter.py)

| 기능 이름 | 기능 설명 |
|---|---|
| 세션 기반 체크포인트 조회 | session_id를 thread_id로 사용해 그래프 상태를 조회한다. |
| Task·Execution 일치 검사 | 이벤트가 현재 그래프의 작업·실행에 속하는지 확인한다. |
| 체크포인트 준비 대기 | 이벤트가 API의 상태 저장보다 먼저 도착하면 처리 연기한다. |
| Executor 인터럽트 재개 | EXECUTOR_EVENT 대기 지점을 지정해 이벤트를 전달한다. |
| 사용자 승인 대기 보호 | Executor 이벤트를 사용자 승인 답변으로 잘못 전달하지 않는다. |
| 그래프 처리 기록 확인 | command_id → event_id 기록으로 이미 반영된 요청을 확인한다. |
| 중간 실패 복구 | 이벤트 수락 후 실패했다면 새 resume 대신 남은 노드를 이어서 실행한다. |
| 처리 기록 이후 복구 | 처리 기록 뒤 노드가 실패한 경우 동일 요청의 후속 노드를 복구한다. |
| 늦은 이벤트 무시 | 처리 이력이 있는 이전 실행이나 이미 반영한 순번의 이벤트를 무시한다. |
| 종료 그래프 보호 | 끝난 그래프를 늦게 도착한 이벤트로 다시 시작하지 않는다. |
| 재개 후 반영 확인 | 이벤트 처리 기록이 체크포인트에 남았는지 확인한다. |
| 단계별 저장 완료 대기 | durability="sync"로 다음 단계 전에 체크포인트 저장 완료를 기다린다. |

이 기능은 특정 State 필드와 단일 Executor 대기 인터럽트 계약을 따르는 그래프에
적용된다. 모든 LangGraph에 그대로 붙는 범용 어댑터가 아니다.

- 그래프 상태: active_task_id, execution_id, ew_pending, ew_receipts, ew_sequences.
- 대기 인터럽트: kind=EXECUTOR_EVENT와 task_id, execution_id를 제공한다.
- 그래프 노드: 수락한 요청과 처리 완료 기록을 직접 상태에 저장해야 한다.
- 어댑터: EventContext를 읽어 thread 설정과 resume 입력으로 변환한다.
- API·워커: 호환되는 그래프와 동일한 체크포인트 저장소를 사용한다.

EventContext와 State가 같은 구조일 필요는 없다. 개발자의 State 구조가 다르면
어댑터와 이벤트 수신·반영 노드를 함께 조정한다. 체크포인트 저장 전에 발생한
외부 부수 효과가 재실행될 수 있으므로 외부 호출은 command_id 등을 사용해
멱등하게 만들어야 한다.

## 6. 실행 환경·설정·모니터링

담당: [runtime.py](../../src/worker/runtime.py),
[config.py](../../src/worker/config.py),
[telemetry.py](../../src/worker/telemetry.py)

| 기능 이름 | 기능 설명 |
|---|---|
| 워커 구성 요소 조립 | 원본·내부 소비기, Router, Outbox, 잠금, 지표를 구성한다. |
| 백그라운드 루프 실행 | 소비와 라우팅·발행·지표 갱신 루프를 함께 실행한다. |
| 비동기 연결 재사용 | DB pool, Redis·HTTP client를 재사용하고 종료 시 정리한다. |
| 루프 대기 시간 조절 | 처리할 일이 없거나 오류가 발생하면 유지보수 루프의 대기를 조절한다. |
| 환경변수 설정 | EW_ 접두사로 접속 주소, 처리량, 시간 제한 등을 설정한다. |
| namespace 구분 | DB 행, Redis 그룹·키·내부 Stream을 서비스 단위로 구분한다. |
| 인스턴스 구분 | 소비자 이름에 instance ID를 사용해 워커 인스턴스를 구분한다. |
| Liveness 응답 | /health/live에서 워커 HTTP 응답 가능 여부를 확인한다. |
| Readiness 검사 | /health/ready에서 종료 상태, 소비 루프, DB·Redis 연결을 확인한다. |
| 처리 지표 수집 | 처리 결과, 활성 처리 수, 처리 시간 분포를 수집한다. |
| 대기량 지표 수집 | DB 상태별 건수와 Redis pending·lag를 조회한다. |
| Prometheus 지표 노출 | /metrics에서 수집한 지표를 제공한다. |
| 관찰자 오류 분리 | 소비기 Observer 오류가 메시지 처리에 직접 전파되지 않도록 분리한다. |

namespace는 같은 서비스를 처리하는 API·워커가 공유한다. 사용자나 Pod별로
바꾸는 값이 아니며 Kubernetes namespace, LangGraph checkpoint namespace와도
다르다. 권한을 강제하는 보안 경계나 체크포인트 격리를 자동 제공하지 않는다.

지표의 처리 시간은 소비기 메시지 처리 시간이다. Executor 코드 실행 전체
소요 시간이 아니다. Readiness 성공도 LLM·Executor 업무의 성공을 보장하지 않는다.

## 7. DB 초기화·감사 기록·내부 운영 함수

담당: [Alembic 가이드](../../worker_migrations/README.md),
[store.py](../../src/worker/store.py)

| 기능 이름 | 기능 설명 |
|---|---|
| Alembic 초기화 | binding, Inbox, Command, Outbox, audit 테이블과 제약·인덱스를 생성한다. |
| 독립 버전 관리 | ew_alembic_version으로 기존 서비스의 Alembic 버전과 분리한다. |
| 중복·동시 초기화 대응 | 적용 버전을 확인하고 DB advisory lock으로 동시 migration을 직렬화한다. |
| 초기화 SQL 출력 | DB에 적용하지 않고 실행 예정 SQL을 출력한다. |
| 파괴적 downgrade 보호 | 전체 워커 테이블 삭제는 명시적 옵션 없이는 거부한다. |
| 생성·수정 추적 필드 | created_at/by, updated_at/by를 기록한다. |
| Command 수동 재시도 함수 | FAILED Command를 READY로 바꾸고 Outbox 재발행을 설정한다. |
| Command 수동 건너뛰기 함수 | FAILED Command를 IGNORED로 처리한다. |
| 운영 조치 감사 기록 | 수동 재시도·건너뛰기의 수행자와 사유를 기록한다. |
| 상태별 건수 조회 | Store.counts()로 Inbox·Command·Outbox 상태별 건수를 조회한다. |

수동 복구 함수는 `Store.resolve_failed(retry=True/False)`로 남아 있다.
하지만 CLI는 삭제됐고 관리 API도 없다. 호출자는 동일한 세션 잠금을 잡고
수행자와 사유를 제공해야 한다. SKIP은 업무 성공이나 Executor 취소를 뜻하지 않는다.

새 DB는 Alembic으로 초기화한다. 기존 테이블이 있는 DB는 임의 stamp로 채택하지
않고 스키마 동등성을 확인한다. LangGraph checkpoint 초기화는 호스트가 별도로
수행한다. 워커 시작 시 자동 DDL은 실행하지 않는다.

## 8. 정식 진입점과 예제·배포 자료

| 이름 | 제공하는 내용 |
|---|---|
| src/agent/worker_main.py | 시작, 자원 수명 관리, SIGTERM/SIGINT 종료 |
| src/agent/integrations/worker_hooks.py | 그래프 factory와 이벤트 핸들러 연결 |
| examples/worker/session_graph.py | 이벤트 대기 → 수락 저장 → 반영 예제 |
| examples/worker/api_integration.py | 기존 Execution 연결과 대기 상태 생성 |
| examples/worker/failure_cleanup.py | 취소 접수 후 실제 종료 확인 예제 |
| Dockerfile | 루트의 런타임·테스트 이미지 |
| deploy/worker/deployment.yaml.example | 향후 API+Agent / Worker 배치 |
| deploy/worker/migrate-job.yaml.example | ew_* migration Job |
| deploy/worker/compose.test.yaml, tests/worker | 격리 DB·Redis 회귀 |

취소 예제는 자동 호출하지 않는다. 성공·실패·취소 업무 정책은 Agent가 결정한다.
실제 Agent factory는 아직 미구현이므로 새로운 main은 소비 전에 실패한다.
기존 ex-agent-api/ex-agent-worker 배포는 이 단계에서 변경하지 않았다.

## 9. 삭제 후 정리 사항

CLI, DLQ replay/discard, Stream trim 도구는 사용자가 삭제한 상태를 유지한다.
잔여 CLI 명령 등록, 해당 모듈에 의존하는 테스트 4개, 삭제된 schema.sql을
읽는 Store.migrate()는 제거했다. DB 초기화는 worker_migrations의 Alembic만 쓴다.

소비기의 DLQ 발행·ACK·재시도 검증과 Store의 수동 복구 함수는 유지한다.
기존 스키마의 무조건 채택 금지·검증 후 stamp 경로는 Alembic 테스트로 확인한다.
삭제 전·이동 전 기록은 [과거 검증 이력](validation-history.md)이고, 현재 결과는
[전환 계획](../worker-centered-refactor.md)에 기록한다.

## 10. 호스트 에이전트가 별도로 구현할 범위

- 사용자 채팅 API, 인증·권한, 세션·Task 발급 및 작업 접수 정책.
- 분석 계획, Skill·Tool 선택, LLM 호출, 코드 생성, 리포트 생성.
- Executor 실행 제출 및 제출 중 장애에 대비한 멱등 키·실행 연결 복원.
- 실제 이벤트 타입별 업무 처리와 프론트 진행 상황 전달.
- 장기 실행 중 채팅 잠금, 사용자 취소 정책, 결과·실패 안내.
- 그래프 State와 이벤트 대기·처리 기록 노드, 체크포인트 초기화.
- 외부 부수 효과의 멱등 처리, 장기 세션 처리 기록의 보존·정리.
- 삭제된 운영 도구를 대체할 관리 API나 별도 운영 절차.
- Redis 영속성·백업, Executor 이벤트 이력 보존 등 인프라 운영 정책.

이 워커는 전달·재처리 기반을 제공하며 전체 업무의 exactly-once 실행을
보장하지 않는다. Redis/DB 기록 보존과 호스트의 멱등 처리·복구 계약이 전제다.

다음으로 읽을 문서:

- [정식 시작·개발자 작성 가이드](agent-integration.md)
- [인수인계 README](README.md)
- [DB 초기화·마이그레이션](../../worker_migrations/README.md)
- [기존 검증 기록과 테스트 매핑](validation-history.md)
