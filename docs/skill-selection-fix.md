# 스킬 선택 이름·버전 계약 보완 — 2026-08-31

## 원인과 수정

초기 선택 프롬프트는 `data-inspection@0.1.0`처럼 이름과 버전을 합쳐
보여줬지만, 등록 여부 검증은 `data-inspection`만 허용했다.
반환 스키마가 임의 문자열 목록이어서 같은 표현을 반환하면 실패했다.

`middleware/skill_selection.py`에서 다음 계약을 적용한다.

- 카탈로그의 `name`, `version`, `description`을 별도로 전달한다.
- 반환 JSON Schema의 이름 항목을 해당 스냅샷의 등록 이름 enum으로 제한한다.
- 응답은 로컬에서도 검증한다. 정확한 `이름@등록버전`만 호환 별칭으로
  허용하며 다른 버전, 오타, 알 수 없는 이름은 거절한다.
- 중복은 최초 선택 순서를 유지하며 제거한다. 빈 선택은 허용하지 않는다.
- 출력/선택 검증 실패만 오류 피드백을 전달해 한 번 추가 요청한다.
  네트워크 오류와 취소는 이 루프에서 재시도하지 않는다.
- 자유 코드 계획은 기존처럼 스킬 선택을 우회한다.

미들웨어 순서는 위험 사전 확인 → 모델 감사 → 시간 제한 → 스킬 컨텍스트
→ 계획 출력 검증이다. 선택 호출도 감사/시간 제한 범위에 들어가며,
선택 이유를 `agent_model_call_audits.metadata.skill_selection_rationale`에
남긴다. 승인, Redis consumer, Executor 실행 계약은 변경하지 않는다.

선택 보정은 호출당 최대 두 번이다. 기존 Worker의 커맨드 재시도 정책과는
별도이며, 이 제한을 전체 작업의 총 모델 호출 제한으로 해석하면 안 된다.

참고: [LangChain custom middleware](https://docs.langchain.com/oss/python/langchain/middleware/custom),
[structured output](https://docs.langchain.com/oss/python/langchain/structured-output).

## 실제 실행 중 추가 확인한 MULTI 문제

스킬 선택 수정 후 첫 샘플 실행에서 데이터 생성은 성공했으나, 다음 셀의
`adapt_multi_plan`은 등록 카탈로그 없이 이전 계획/실행 결과만 모델에
전달하고 있었다. 실제로 다른 스킬 소속의 도구를 연결한 응답이 발생했다.

후속 MULTI 분석 요청에도 스킬 지침과 도구의 정확한 소속·버전·해시·파라미터를
전달하도록 보완했다. 함수 소스는 제외하고 기존 스킬/툴 조합 검증을 유지한다.
자유 코드에는 분석 카탈로그를 전달하지 않는다. 전체 JSON 컨텍스트는
`planner_context_max_chars` 제한을 적용한다.

이 변경은 후속 선택을 전체 등록 카탈로그로 안내한다. 큰 카탈로그에서
선택된 스킬만 로딩하는 최적화나 MULTI 의사결정 경로 전체를 미들웨어로
통합하는 구조 변경은 포함하지 않는다.

두 번째 smoke에서는 데이터 생성/검사가 성공한 뒤 실행 이력 저장에서
`'dict' object has no attribute 'model_dump'` 오류가 발생했다.
체크포인트에서 복원된 셀 dict를 `AgentRepository` 경계에서
`PlanStepDraft.model_validate`로 검증·복원한 뒤 저장하도록 보완했다.
직렬화된 셀과 모델 객체 입력, 불완전한 dict 거절을 회귀 테스트에 추가했다.

## 재현 및 회귀 검증

```bash
uv sync --group chat-ui --no-editable --reinstall-package ex-agent
uv run --no-sync python -m pytest tests/test_skill_selection.py \
  tests/test_multi_planning_context.py tests/test_planning_prompt.py
docker compose --profile test build test
docker compose --profile test run --rm test python -m pytest -q
docker compose --profile test run --rm --no-deps \
  -e EX_AGENT_TEST_LIVE_MODEL_URL=http://model.frodo.com/v1 \
  test python -m pytest tests/test_skill_selection_live.py -q -s
```

- 로컬/컨테이너 전체 테스트 결과는 아래 최종 검증 기록을 따른다.
- 실제 `qwen38-27b-fp8` 스킬 선택: 동일 EDA 요청 3회 모두 통과.
  다섯 스킬을 모두 정상 이름으로 선택했으며 데이터 준비/시각화가 포함됐다.
- Ruff check/format(line length 79), ty: 최종 변경 통과.
- 위 카운트의 skip은 별도 DB/Redis 또는 opt-in 실서비스 환경이 필요한
  테스트이며 공유 Executor DB/Redis로 통합 테스트를 우회하지 않는다.

## 실제 Task 추적

기존 사용자 실패 Task `429ffc92-442d-5f7e-910c-30d440872fe6`은 보존했다.
테스트는 `skill-selection-smoke-user` / `skill-selection-smoke-project`로
분리했으며 기존 실패 작업을 강제로 재개하지 않았다.

첫 smoke Task `75336a12-6a5f-477f-9f69-31fb7bd394c3`:

- 스킬 선택 → 계획 승인 → `fetch_dataset` 실행 성공.
- Executor: `a8985f1f-3ae6-4a68-bbc4-137d2c2f847d`.
- 추가 MULTI 소속 오류로 FAILED. 실패 보상에서 Executor 취소 확인.
- 성공 리포트를 만들지 않았고 실패 기록을 삭제하지 않았다.

두 번째 smoke Task `3cb2580f-ed66-4496-8d1a-e6c2e95361c2`:

- 데이터 생성과 `inspect_dataset` 실행 성공.
- Executor: `b412aa8c-fa5b-4c8d-b192-4fe2002e718d`.
- 성공 셀 이력 저장의 dict 타입 오류로 FAILED. Executor 취소 확인.
- 이 기록도 재개/삭제하지 않았다.

## 최종 검증 결과

- 파일 기반 로컬 전체 테스트: **211 passed, 43 skipped**.
- 전용 Compose DB/Redis 전체 테스트: **233 passed, 21 skipped**.
- 선택/미들웨어/후속 MULTI 컨텍스트 및 복원 경계 회귀 테스트를 포함한다.
- 최종 Task: `337ca737-a88b-4d51-9e9f-6b4e931c3f38`, `SUCCEEDED`.
- Executor: `7b2b78c2-c03e-460d-9c6c-c26835cfff01`.
- 실제 순서: `fetch_dataset` → `inspect_dataset` →
  `profile_missing_values` → `summarize_numeric_columns` →
  `plot_distribution` → finalize → 리포트 생성/반환.
- CSV 다운로드 확인: 500행/5열, revenue 결측치 20개.
- PNG 다운로드 확인: PNG 시그니처, 1152×720 크기.
- 노트북 전용 조회: 실행 카운트 1~5인 코드 셀 5개, 출력 오류 없음,
  마지막 Markdown 리포트 셀 1개 확인.
- 리포트 Artifact 본문과 Agent API의 terminal_message가 동일함을 확인.

Artifact ID:

| 종류 | ID |
| --- | --- |
| DATASET | `0a3e6e56-8025-41ed-b057-0b52afb6ca2c` |
| PLOT | `d68d2876-75b3-4bb6-a2d0-a5b246287912` |
| NOTEBOOK | `6ab2b0ee-87ff-4e62-9fae-4ae479533ee7` |
| REPORT | `15557b55-8284-e586-5664-657841ef0e64` |

기존 API와 Executor는 재시작하지 않았다. 수정한 Worker만 교체했으며,
로컬 파일 기반 패키지도 갱신했다. 기존 Chat UI dev 프로세스는 종료하지 않았다.

## 별도 후속 보완 — 이번 수정에 포함하지 않음

실행 완료는 확인했지만 다음 문제 때문에 전체 기능의 운영 준비 완료나
모든 추적 정보/리포트 품질의 정확성을 보장하는 결과는 아니다.

### 1. MULTI 계획 리비전 연결과 소스 파일 불변성

`append_operation` 이후 현재 계획 리비전/ID가 그래프 상태에 반영되지 않는다.
실제 DB에서 성공 셀 0~4의 `source_plan_revision_id`가 모두 리비전 1을
가리켰다. 리비전 2~5의 compiled_source_path도 동일한
`337ca737-a88b-4d51-9e9f-6b4e931c3f38/2/step-0000.py`로 기록됐다.

Executor는 각 operation의 소스/출력과 실제 노트북 셀을 별도로 보존하지만,
Agent 측 과거 계획 파일과 리비전 연결은 정확하지 않다. 후속 작업에서는
persisted plan 정보를 graph state로 돌려주고, 실제 리비전에 맞춘 불변 파일
경로와 재시도 멱등성을 함께 검증해야 한다.

### 2. 리포트 서술 품질

리포트 파일과 노트북 첨부는 실제 생성됐지만, 본문에는 모순되게
"최종 결과 리포트 작성 단계는 제외됨"이라고 서술했고 등록되지 않은
`reporting` 스킬을 후속 작업으로 제안했다. 현재 자신이 최종 리포트를
작성 중이라는 프롬프트와 사용 가능한 기능 근거, 출력 검증 보완이 필요하다.
수치/파일 존재 확인과 서술 품질 보증은 구분해야 한다.

### 3. Executor NOTEBOOK Artifact 다운로드

리포트 첨부 후 `/artifacts/6ab2b0ee-87ff-4e62-9fae-4ae479533ee7/content`
응답의 JSON이 도중에 잘려 파싱에 실패했다. NOTEBOOK 메타데이터의
size_bytes는 리포트 첨부 전 값 9761로 표시됐다. 메타데이터 갱신/다운로드
길이 처리가 원인인지 Executor 측 확인이 필요하며, 해당 저장소는 수정하지 않았다.

반면 `/executions/7b2b78c2-c03e-460d-9c6c-c26835cfff01/notebook` 전용
조회는 리포트를 포함한 6개 셀을 정상 반환한다. 노트북 자체 손상과
Artifact 다운로드 응답 문제를 구분해야 한다.
