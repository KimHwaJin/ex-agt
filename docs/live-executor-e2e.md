# 실제 Agent · Executor 종단 검증

이 검증은 FastAPI 직접 admission부터 LangGraph interrupt, Executor/Jupyter,
Redis 이벤트 Worker resume, 성공 리포트와 노트북 projection까지 확인한다.
실제 모델과 외부 실행을 사용하므로 기본 pytest에서는 실행하지 않는다.

## 사전 조건

- Executor PostgreSQL, Redis, Executor API와 Jupyter가 healthy여야 한다.
- Agent `.env`가 Executor의 Redis와 `shared_dir`를 가리켜야 한다.
- Agent DB migration을 적용한 현재 API와 Worker가 healthy여야 한다.
- API와 Worker는 동일한 Agent DB와 LangGraph checkpoint DB를 사용해야 한다.

현재 저장소의 Compose 배포는 Executor/Jupyter를 재시작하지 않고 아래처럼
Agent API와 Worker만 갱신한다.

```bash
docker compose up -d --build --wait api worker
curl --fail http://127.0.0.1:8010/health/ready
curl --fail http://127.0.0.1:8011/health/ready
curl --fail http://127.0.0.1:8000/readyz
```

## 실행

명시적인 두 URL이 모두 있어야 테스트가 활성화된다. 별도 테스트 사용자,
프로젝트, Task와 Session UUID를 매번 생성하므로 기존 Task를 변경하지 않는다.

```bash
EX_AGENT_TEST_LIVE_EXECUTION_API_URL=http://127.0.0.1:8010 \
EX_AGENT_TEST_LIVE_EXECUTOR_URL=http://127.0.0.1:8000 \
uv run --no-sync python -m pytest \
  tests/test_agent_execution_live.py -q -s
```

테스트는 다음 증거를 모두 확인한다.

1. API가 START를 직접 LangGraph에 전달하고 PLAN_REVIEW에서 중단한다.
2. SINGLE 계획이 함수 정의와 호출을 담은 셀 하나를 가진다.
3. 승인 후 Executor Execution ID가 Task에 projection된다.
4. Worker가 Executor 완료 이벤트를 받아 같은 Session thread를 resume한다.
5. Task와 Executor가 모두 SUCCEEDED로 종료된다.
6. 전체 노트북 조회에서 stdout `55`를 확인한다.
7. NOTEBOOK·REPORT Artifact와 마지막 Markdown 리포트 셀을 확인한다.
8. MULTI 분석은 후보가 있을 때만 워크플로우 선택을 한 번 요청한다.
9. 최초 1셀 계획만 승인하고 이후 셀은 결과 기반으로 자동 계획한다.
10. 최종적으로 DATASET·PLOT·REPORT를 생성한다.

실패한 실행과 Task는 진단 이력으로 보존한다. 테스트 종료 시 실제 기록이나
Artifact를 자동 삭제하지 않는다. 이 smoke는 장기 실행, MULTI 재계획,
Worker 강제 종료와 K8s 롤링 전환 검증을 대신하지 않는다.

## 2026-09-01 최초 직접 admission 검증

- Task: `8a9d69ac-7a02-4df8-9a79-a73d5a823d1d`
- Execution: `b254c091-b7f3-4288-9cc8-77f6e2b43577`
- 흐름: `PLAN_REVIEW → EXECUTOR_EVENT → SUCCEEDED`
- Jupyter stdout: `55`
- Artifact: `execution.ipynb`, `analysis-report.md`
- 노트북: 코드 셀 1개와 마지막 Markdown 리포트 셀

이번 검증 과정에서 번호가 붙은 미추적 복사본이 Docker build context에
들어가 `ew_0001` Alembic revision을 중복시킨 문제도 발견했다. `.dockerignore`
에서 `파일명 2.py` 같은 번호 복사본을 제외하며 원본 로컬 파일은 삭제하지 않는다.

## 2026-09-01 최초 MULTI 분석 검증

- Task: `54704b24-00de-4a80-82fc-45c71018b4bd`
- Execution: `11658734-3d4a-4b91-a8da-ac46105ace16`
- 실행: 6개 Operation 모두 SUCCEEDED
- 분석: 500행 샘플 생성, 구조·결측치·요약·그룹 집계·분포 시각화
- Artifact: CSV DATASET, PNG PLOT, NOTEBOOK, Markdown REPORT
- 사용자 결정: 최초 PLAN_REVIEW 한 번, 중간 승인 없음

현재 DB에 검색 가능한 승격 워크플로우 후보가 없으면
`WORKFLOW_SELECTION`은 생략되고 바로 동적 계획으로 진행한다. 후보가 있으면
선택 interrupt는 최대 한 번 발생한다. 두 경우 모두 직접 분석은 MULTI다.

자동 테스트 검증 결과:

- SINGLE: `1 passed in 22.20s`
- MULTI: `1 passed in 117.57s`
- 최종 연속 실행: `2 passed in 144.20s`
- 환경 변수가 없는 기본 테스트: 2개 모두 skip
