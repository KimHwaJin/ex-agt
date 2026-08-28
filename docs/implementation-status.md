# V1 Implementation Status

상태: `FIRST_IMPLEMENTATION_COMPLETE`

## 구현됨

- FastAPI는 Task/resume/cancel을 PostgreSQL에 먼저 기록하고 Redis Stream으로
  전달한다.
- Background worker만 LangGraph를 invoke/resume한다.
- `create_agent(tools=[])` Planner와 Skill context, risk prerequisite, model
  audit, timeout, output-validation middleware가 연결되어 있다.
- 의미 기반 LLM intent 분기, 명시적 request/code risk node, HITL interrupt,
  SINGLE/MULTI 실행, 최대 3회 MULTI 보정, 취소, 성공 전용 report flow가
  Graph edge로 표현되어 있다.
- Plan/Revision/Step에는 사용자 공개 계획, 선택 이유, Skill/Tool version과
  hash, parameter, 컴파일 source hash/path가 저장된다.
- promoted Workflow는 pgvector cosine search로 상위 3개를 검색하며 서비스
  전체 공개를 기본값으로 하고 미래 권한 필드를 포함한다.
- Executor 제출/append/finalize/cancel/result/report REST 계약과
  `executor.events` consumer가 구현되어 있다.
- 실행 코드와 성공 리포트는 공유 입력 루트에 원자적으로 materialize한 뒤
  PATH와 SHA-256으로만 제출한다. Agent Executor 경계는 INLINE source를
  거절한다.
- Executor event는 `event_id`로 중복 제거하고 Execution별 순번을 DB에서
  원자적으로 전진시킨다. gap은 Executor event-history REST pagination으로
  복구한 뒤에만 Graph resume command를 만든다.
- SSE는 PostgreSQL event ID와 `Last-Event-ID`로 재연결할 수 있다.
- 승인 command와 Session lock은 같은 transaction으로 저장된다. 잠금은 성공
  report 완료, 실패 확인 또는 취소 완료 후 해제된다.
- uv, Ruff 79자, ty, Docker multi-stage build, Alembic, pgvector PostgreSQL,
  Redis Compose 통합 테스트가 구성되어 있다.
- 내부 vLLM `qwen38-27b-fp8`의 LangChain 일반 호출과 구조화 출력을 실제
  컨테이너에서 검증했다.
- 실제 임베딩 모델이 없는 개발 단계에는 결정적 `dummy-hash-v1` 1024차원
  임베딩으로 pgvector 인덱싱·검색 계약을 검증한다.
- 실제 Agent REST/HITL/worker/Executor/Jupyter/Redis event/report 전체를 잇는
  SINGLE 코드 실행 E2E에서 PATH source, checksum, stdout, Notebook Markdown,
  Task 감사 매핑을 검증했다.

## 다음 구현 범위

- 실제 Executor/Jupyter를 사용한 MULTI와 데이터 분석 Workflow 전체 E2E
- 실제 embedding 모델 확보 후 Workflow 의미 검색 품질 검증 및 재인덱싱
- Workflow 승격 command/API와 승격 권한 정책
- BFF 서명 또는 service-to-service 인증으로 `X-User-ID` 신뢰 경계 강화
- transient token delta를 위한 별도 ephemeral streaming channel
- report evidence의 원본 result manifest 선택 읽기와 reference validator 강화
- 다중 worker 장애 주입, 장기 실행, Redis/PostgreSQL outage recovery 테스트
