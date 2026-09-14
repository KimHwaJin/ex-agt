# Skill·Tool 기반 셀 계획 준비

## 현재 범위

신규 Run은 `assistant-v3` 그래프를 사용합니다. 실제 LLM 요청 분류와
초안 생성 후 Skill·Tool 선택 또는 직접 코드 작성으로 셀을 준비합니다.
준비된 계획은 승인·수정·거절할 수 있지만 **Executor 제출은 아직 없습니다.**
승인 시 `EXECUTOR_NOT_CONFIGURED`로 종료하며 실제 실행 성공으로 표시하지 않습니다.

```text
classify → plan (초안 및 구현 방식 결정)
           → prepare_code
               → 생성 전 위험 검토 middleware
               → catalog 또는 generated_code 전용 create_agent
               → 식별자/인자/셀 구조 검증·조립
           → review_plan (interrupt)
               → 수정: plan → prepare_code → 새 버전 검토
               → 승인/거절: review_outcome → complete
```

LangChain middleware는 모델의 구조화 응답 검증과 생성 전 위험 검토를 담당합니다.
LangGraph는 계획 생성과 재실행 가능한 승인 경계를 분리하고 영속 상태를 관리합니다.
분석 함수 자체를 LangChain 실행 도구로 등록하지 않습니다. Agent 프로세스에서
분석 함수를 실행하는 것이 아니라 Jupyter에 보낼 셀 원문을 만드는 단계입니다.

## 파일 위치와 확장 방법

| 위치 | 책임 |
|---|---|
| `src/agent_service/catalog/assets/*.md` | Skill 설명, 사용 제약, 연결 Tool |
| `src/agent_service/catalog/assets/*.py` | Jupyter로 복사할 독립 함수 원문 |
| `src/agent_service/catalog/registry.py` | Skill/Tool ID·버전·파라미터 계약 |
| `src/agent_service/catalog/compiler.py` | 인자 검증, 이전 셀 참조, 셀 조립 |
| `src/agent_service/agents/code_planner.py` | 선택/직접 작성 Agent, 위험 검토 |
| `src/agent_service/graphs/assistant/preparation.py` | 공개 계획과 내부 셀 분리 |
| `src/agent_service/application/plan_snapshots.py` | 승인 카드와 원문 원자적 저장 |

새 도메인 함수를 받으면 `.md`와 독립 함수 파일을 추가하고 `registry.py`에
파라미터 모델 및 입출력 계약을 등록합니다. 함수/스킬을 변경할 때는 버전을 올립니다.
현재 예제들은 모두 0.1.0이며, 새 버전에서도 예전 승인 계획은 저장된 원문으로
추적할 수 있습니다. 동시에 여러 버전을 검색하는 카탈로그 UI는 아직 없습니다.

패키지 리소스는 `importlib.resources`로 읽고 프로세스에서 한 번 캐시합니다.
컨테이너는 editable 설치나 호스트 소스 마운트 없이 파일을 포함한 패키지를 사용합니다.
함수 파일을 Agent에서 동적으로 import/exec하지 않습니다.

## 기본 예제 네 개

| Skill | Tool | 역할 |
|---|---|---|
| sample-data | fetch_sample_data | 합성 매출 행 생성: date/category/revenue |
| data-inspection | inspect_data | 행 수, 열 이름, 결측치 수 |
| descriptive-analysis | describe_numeric | 숫자 열 개수·평균·최소·최대 |
| visualization | plot_category_totals | 범주별 합계 SVG 막대그래프 |

`fetch_sample_data(query, rows, seed)`는 다운로드 함수 자리를 대신할 **페이크**입니다.
query는 추적용 설명이며 SQL을 실행하거나 데이터 레이크에 접속하지 않습니다.
예제 데이터는 pandas가 아닌 `list[dict]`로 전달합니다. 기존 이미지 의존성을
가정하지 않고도 확인할 수 있도록 Python 표준 라이브러리를 사용했습니다.
차트는 Jupyter에서 inline 표시하고 SVG 문자열을 반환합니다. 파일/아티팩트 등록은
후속 Executor 연동에서 추가해야 합니다. 실제 업무 분석 함수의 대체물이 아닙니다.

## 셀·파라미터 계약

LLM은 `skill_id`, `skill_version`, `tool_id`, `tool_version`을 별도로 반환합니다.
ID에 `@0.1.0`을 붙이거나 존재하지 않는 조합이면 승인 화면까지 진행하지 않습니다.

모델과 공개 계획 모두 `parameters` 객체를 사용합니다.
JSON을 다시 문자열로 감싸지 않습니다. 추가 인자, NaN/Infinity, 범위 오류를
차단합니다. 문자열을 코드로 해석하지 않고 Python 리터럴로 렌더링합니다.
식별자·인자·Python 구조 검증에 실패하면 내부 피드백으로 1회만 수정 요청합니다.
다시 실패하면 승인 카드를 만들지 않습니다. 코드 실행 재시도가 아닙니다.

`input_step`은 이전 셀 번호(1부터)이며 그 결과를 함수의 첫 번째 인자로 전달합니다.
카탈로그 함수는 `data`, 직접 작성한 함수는 `df` 등 자신이 정의한 이름을 사용합니다.
`input_parameter`에는 AST에서 확인한 실제 인자 이름을 저장하며 원문을 바꾸지 않습니다.
앞으로의 셀/자기 자신을 참조할 수 없고, 카탈로그 분석 함수는 records 결과만
받을 수 있습니다. 현재는 셀당 이전 결과 하나를 입력받습니다. 여러 데이터셋을
조인하는 도메인 함수가 추가되면 이름별 다중 참조 계약을 확장해야 합니다.

```python
def inspect_data(data):
    # 실제 코드에는 해당 함수 전체 원문이 들어갑니다.
    ...


step_2 = inspect_data(data=step_1)
```

셀당 함수 정의 하나와 호출 하나이며, 필요한 import는 함수 안에 둡니다.
직접 코드는 `function_lines` 배열로 받고 개행으로 조립하여 원문으로 저장합니다.
들여쓰기는 각 줄의 선행 공백으로 보존합니다. AST/compile 검사는 구문과 호출 구조만
검증합니다. **보안 샌드박스, 실행 성공, 수학적 정확성, 설치 의존성을 보증하지 않습니다.**

## 직접 코드 작성 경로

코드 작업은 `generated_code` 전용 Agent를 사용합니다. 분석 작업도 사용자가
내부 함수를 쓰지 말라고 수정하면 초안 Agent가 구현 방식을 변경합니다.
실제 코드 생성 Agent의 시스템 프롬프트에는 카탈로그를 제공하지 않고,
이전 Agent의 Skill/Tool 카드나 함수 원문을 메시지로 전달하지 않습니다.
사용자 자신이 메시지에 쓴 함수명까지 삭제하는 것은 아닙니다.
직접 코드 조립 경로는 카탈로그 로더를 호출하지 않습니다.

## 추적·저장·공개 경계

- `management.execution_plans`: `(run_id, plan_version)`별 불변 스냅샷.
  `plan_id`, 공개 계획, 실제 함수 원문/셀 코드, Skill 문서 원문, 각 SHA-256,
  created/updated at/by를 저장합니다. 수정은 덮어쓰기가 아닌 새 버전입니다.
- 공개 계획: 단계별 내용·선택/작성 이유·예상 산출물, Skill/Tool ID·버전,
  파라미터, 입력 셀 번호, 위험 경고, `plan_sha256`를 포함합니다.
- `plan_sha256`: 자체 hash 필드를 제외한 공개 계획과 전체 셀 묶음을 canonical
  JSON으로 직렬화한 SHA-256입니다. 배포 후 함수가 바뀌어도 원문을 확인할 수 있습니다.
- `run_interrupts`: 승인 대상 계획/hash와 사용자의 수정/승인/거절 응답을 보관합니다.
- 공식 LangGraph 체크포인트: 계획 준비가 완료된 상태와 진행 위치를 복원합니다.

계획 스냅샷, 화면 메시지, 승인 카드와 이벤트를 한 관리 DB 트랜잭션에서 저장합니다.
체크포인트 저장 뒤 관리 DB 반영이 실패하면 저장된 원문을 다시 반영하고 LLM을
재호출하지 않습니다. 같은 버전에 다른 내용이 들어오면 덮어쓰지 않고 실패합니다.
체크포인트 자체가 커밋되기 전 장애는 해당 노드/모델 호출을 다시 수행할 수 있습니다.

원문은 일반 Run API·SSE·승인 카드에 노출하지 않습니다. 별도 코드 조회 API도
이번에 추가하지 않았습니다. 나중에 실행 ID 기반 노트북 조회와 연결할 예정입니다.
최종 사용자 리포트는 아직 생성하지 않습니다.

## 위험 검토·성능·배포

`GenerationRiskReview`가 코드 생성 전에 별도 LLM으로 요청 위험을 평가합니다.
경고는 계획 검토 화면에 표시하며 현재는 생성 차단 정책이 아닙니다. 실행은
애초에 연결하지 않았습니다. 실제 실행 전 코드 위험 검토는 Executor 연결 단계에
별도로 구현해야 합니다. 위험 LLM도 잘못 판단할 수 있습니다.

질문은 분류+답변 2회, 작업 최초 준비는 분류+초안+위험 검토+셀 계획 4회,
수정은 초안+위험 검토+셀 계획 3회입니다. 승인/거절 자체는 모델을 호출하지 않습니다.
검증 오류를 수정하는 경우 위험 검토+셀 계획 호출이 최대 한 번 추가됩니다.
스냅샷에는 생성 시도 수와 검증 피드백도 남깁니다.
`code_plan_max_tokens`(기본 8192)는 셀 원문을 포함한 응답 한도이며 일반 답변의
`model_max_tokens`와 분리했습니다. LLM/DB IO는 async이고, 파일 로딩과 셀 구문
검사는 이벤트 루프 밖에서 처리합니다. 승인 대기는 worker/DB 연결을 점유하지 않습니다.

배포 전 `python -m agent_service.migrate`로 `management_0005`까지 적용합니다.
새 코드는 v1/v2의 진행 중인 Run도 처리합니다. 구버전으로 돌아가려면 신규 접수를
중단하고 v3 활성 Run을 먼저 완료/취소해야 합니다. 구버전 worker와 v3 접수를
동시에 운영하는 롤링 배포는 지원하지 않습니다. 데이터 삭제/자동 다운그레이드는 없습니다.

## 검증과 다음 작업

`tests/test_catalog.py`는 고정된 저장소 예제 코드만 실행합니다. 실제 모델이 만든
자유 코드는 Agent/test 프로세스에서 실행하지 않습니다. Docker 테스트는 설치된
패키지와 실제 PostgreSQL을 사용합니다. UI 검증은 실제 개발 모델을 사용하지만
승인 뒤에도 Executor를 호출하지 않습니다.

2026-09-14 검증: Docker Python 3.11 + 격리 PostgreSQL에서 120개 테스트 통과.
`qwen38-27b-nvfp4` 실제 모델로 카탈로그 4개 함수 계획, 승인 대기 중 API 재시작,
카드 복원, 직접 코드 계획으로 수정, 거절, 승인 시 실행 미연결 안내를 확인했습니다.
실제 모델 응답을 Executor/Jupyter에서 실행한 검증은 아직 아닙니다.

다음 순서는 워크플로우 추천·선택(single/multi 결정), Executor 제출 및 이벤트
재개, 실행 전 위험 검토, 성공 시 노트북/리포트 연동입니다. 프로젝트 공유 메모리,
파일/이미지 업로드, 배치도 별도 후속 범위입니다.
