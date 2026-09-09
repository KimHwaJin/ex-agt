# 실제 Executor 이벤트 동일성 보완 및 재검증

## 결과

2026-09-06 전달 패키지의 REST/Redis 이벤트 동일성 충돌을 수정했다.
이전에 실패했던 2개 실제 계약 테스트와 새 실제 Executor/Jupyter 실행의
Worker 재기동·Pending 회수·이력 보충까지 통과했다.
**이 결함으로 인한 전달 보류 사유는 해소됐다.**

| 검증 | 결과 |
| --- | --- |
| 격리 Redis 6.0.8 + PostgreSQL 회귀 | 73 passed, 2 skipped, 6.73초 |
| 격리 Redis 7.4 + PostgreSQL 회귀 | 73 passed, 2 skipped, 6.78초 |
| 기존 실제 Execution으로 계약 재검증 | 2 passed |
| 새 실제 Execution으로 계약 재검증 | 2 passed |
| Ruff lint/format, ty | 통과, Python 파일 36개 |
| 실제 SINGLE/MULTI, 프로세스 재기동, Pending 회수 | 통과 |

격리 회귀의 2개 skip은 실제 Executor URL/Execution ID를 명시해야 하는
opt-in 테스트다. 이를 실제 환경에서 별도 실행해 모두 통과했다.
테스트는 `uv sync --frozen --no-editable` 설치형 Docker 이미지로 수행했다.
소스 디렉터리를 PYTHONPATH로 주입하거나 설치된 Worker를 런타임에 패치하지 않았다.

## 수정 내용

런타임 변경은 다음 **두 파일**뿐이다.

- `src/worker/contracts.py`: `ExecutorEvent.identity_document()` 추가.
- `src/worker/store.py`: 저장된 이벤트와 새 이벤트 양쪽을 정규화하여 비교.

동일성 비교에는 스키마 1.0의 다음 7개 불변 필드를 사용한다.
`event_id`, `execution_id`, `event_type`, `event_sequence`, `schema_version`,
`occurred_at`, `payload`.

- `occurred_at`은 timezone이 명시된 시각을 UTC·microsecond 표현으로 비교한다.
  `Z`, `+00:00`, 같은 시각의 `+09:00`은 같은 이벤트로 판정한다.
- 날짜 형식이 잘못됐거나 timezone이 없는 값은 거절한다.
- REST 응답의 감사/전달 정보 등 추가 필드는 이벤트 동일성에서 제외한다.
  payload 내부 필드는 제외하지 않는다.
- 비교 전용 JSON 정규화로 객체 key 순서는 영향을 주지 않는다.
  숫자는 Decimal과 내부 tuple 표식으로 비교하여 JSONB의 `1e24` → 정수
  저장도 같은 수치로 판단하되 `true`, `1`, `[1]`은 서로 구분한다.
  내부 비교 표현은 이벤트/State/DB로 전달하지 않는다.
- ID, 실행 연결, 순번, 타입, 실제 발생 시각 또는 payload가 달라지면 기존처럼
  충돌로 거절한다. 다른 event_id가 같은 execution/sequence를 점유하는 것도
  DB unique constraint와 기존 충돌 검사로 거절한다.

**공개 EventContext/Event 모델, 저장 JSON, Agent State, resume payload는
바꾸지 않았다.** 기존 Inbox 행과 새 행을 비교할 때만 정규화한다.
REST로 먼저 저장된 행과 Redis로 먼저 저장된 행 모두 재수신할 수 있고,
기존 JSON을 덮어쓰거나 테이블 전체를 재작성하지 않는다.
이벤트 재수신 시 `catch_up_version` 증가도 유지한다.

## 실제 실행 증거

Executor `1f89ce7`, 공유 Redis 서버 **6.0.8**, 기존 Jupyter를 사용했다.
Worker DB와 PostgreSQL LangGraph checkpointer는 별도 테스트 DB로 격리했다.
수령자 역할의 테스트 그래프를 사용했고 LLM은 호출하지 않았다.

- SINGLE: `25c97a0e-36f3-482c-9305-203f7d370f3e`, `SUCCEEDED`.
  - 노트북 출력: `{'count': 4, 'mean': 25.0}`.
  - operation/execution 완료 이벤트 2개, receipt 2개, 그래프 종료.
- MULTI: `8de03924-cb50-4ad7-89a3-53ac39e30a4e`, `SUCCEEDED`.
  - 첫 operation의 `values`를 다음 operation에서 재사용해 평균 25.0 출력.
  - 첫 결과 이후 Worker 프로세스를 종료하고, 다음 operation의 실제 이벤트
    4건을 별도 consumer가 읽되 ACK하지 않은 상태로 남겼다.
  - 새 Worker 프로세스가 Pending을 회수하고 같은 session thread를 재개.
  - 최종 operation 2개 및 execution 완료 이벤트 1개 처리, 그래프 종료.
  - `last_sequence=10`, `catch_up_version=4`, `caught_up_version=4`,
    `last_error=NULL`. 이전 이력 보충 오류가 재발하지 않았다.
- 두 실행 합계 command `DONE=5`, outbox `SENT=5`.
- ingress/dispatch Pending 모두 0.
- 실제 REST 이력과 그래프 수신 event_id 순서 일치.
- notebook API로 실제 코드 출력 확인.

이 검증은 프로세스 정상 종료/재시작과 인위적으로 남긴 실제 PEL의 회수다.
Kubernetes 강제 kill/노드 장애, 장기간 부하, 실제 운영 ACL/TLS/Sentinel/Cluster
전체 검증으로 확대 해석하지 않는다.

## 재전달 방법

처음 전달한다면 **`executor_worker_handoff/` 디렉터리만** 전달한다.
이전 Redis 6.0.8 대응판을 이미 받은 경우:

1. `src/worker/contracts.py`, `src/worker/store.py`를 함께 교체한다.
   수령자가 Worker core를 바꾸지 않았다면 `src/worker/` 전체 교체도 가능하다.
2. 선택적으로 `tests/test_event_identity.py`,
   `tests/test_live_event_contract.py`와 본 문서를 함께 전달한다.
3. 설치형 배포는 다시 설치/이미지 재빌드 후 Worker를 재기동한다.

환경변수, `uv.lock`/의존성, DB migration, namespace/group 설정은 이번 보완으로
변경하지 않는다. DB/Stream/Pending을 초기화하지 않는다.
수령자가 만든 `src/agent`, `src/agent_worker`, 이벤트 핸들러는 덮어쓰지 않는다.
기존 불변성 검사 실패로 scan backoff 중인 건은 재기동 후 다음 scan에서 처리한다.
정말 payload/identity가 충돌하는 기존 오류까지 자동으로 무시하는 수정은 아니다.

수정 범위는 이 전달 패키지다. 저장소 루트의 별도 `src/worker`나 원본 Agent,
Executor 서비스 구현에 이 수정이 자동 반영되는 것은 아니다.

## 재현

회귀 테스트는 [Redis 호환 안내](redis-6.0.8.md)의 Compose 명령을 사용한다.
실제 계약 테스트는 [기존 검증 문서](verification-redis608-2026-09-06.md)의
환경변수/명령을 그대로 사용한다. 결과가 `2 failed`에서 `2 passed`로 바뀌었다.
예전 Executor 이벤트가 삭제됐다면 위 새 Execution ID로 실행한다.

노트북 JSON 등 로컬 증거는 `/private/tmp/handoff-identity-fix.Gb8NxR/`에 있다.
임시 디렉터리는 장기 보관물이 아니므로 주요 결과는 본 문서를 기준으로 삼는다.
테스트 DB는 백업 후 임시 컨테이너와 함께 정리했다. 이번 테스트 전용 consumer
group/command Stream도 Pending 0 확인 후 제거했다. Executor의 실제 이벤트,
실행 이력, 노트북은 보존했고, 기존 Jupyter 세션은 변경하지 않았다.
테스트 중 임시 증가시킨 실행 한도는 4에서 기존 값 1로 복구했다.
