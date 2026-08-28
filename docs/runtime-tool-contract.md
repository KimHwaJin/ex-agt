# Runtime Tool Contract

상태: `IMPLEMENTED_V1_BASELINE`

## 1. 배경

데이터 분석 Tool 구현은 BFF/Agent 서비스 저장소에 있지만 실제 코드는 Executor가 연결한
Jupyter Runtime에서 실행된다. 현재 Jupyter 환경은 Agent 서비스의 Python package를 import할
수 없다.

초기 구현은 Tool source를 각 Jupyter cell에 포함하는 source-injection 방식을 사용한다.

## 2. Tool 선택과 실행 코드 생성을 분리한다

LLM의 책임:

- 사용자 요청에 맞는 Skill과 Tool 선택
- Tool input parameter 생성
- Step 목적과 예상 결과 설명

Tool 함수 정의 source는 개발자가 미리 작성하고 테스트한 고정 코드다. 자유 코드 요청에서만
LLM이 함수 정의 전체를 생성한다.

결정론적 Tool compiler의 책임:

- Tool registry에서 승인된 정확한 Tool version/source 조회
- input parameter schema 검증
- 고정된 함수 정의 source와 호출문을 하나의 cell source로 조립
- source SHA-256과 Tool lineage 생성
- Executor PATH Step payload 생성(INLINE은 Agent 경계에서 금지)

LLM은 등록된 Tool의 함수 구현을 매 실행마다 다시 작성하지 않는다.

## 3. Cell 규칙

Tool 기반 Step 한 개는 Jupyter cell 한 개이며 다음을 포함한다.

1. 필요한 import
2. 하나의 최상위 Tool 함수 정의
3. 검증된 인자로 해당 함수를 한 번 호출
4. 표준 결과 변수 할당

예시:

```python
import json
from pathlib import Path


def fetch_dataset(query: str, output_path: str, seed: int = 42) -> dict:
    # Registry에 저장된 고정된 함수 구현
    ...


step_0_result = fetch_dataset(
    query="SELECT ...",
    output_path="workspace/sample.csv",
    seed=42,
)
print(json.dumps(step_0_result, ensure_ascii=False, default=str))
```

Tool 함수는 closure, Agent process global, 상대 import에 의존하지 않는 self-contained source여야
한다. 외부 dependency는 선택한 Jupyter runtime profile에 설치된 package만 사용한다.

자유 코드 Step은 LLM이 함수 정의와 호출을 함께 생성하지만, 같은 한 함수/한 호출 규칙과
위험 판정/정적 검증을 적용한다.

## 4. Tool Registry 초안

```text
runtime_tools/
└── data_access/
    ├── SKILL.md
    └── tools/
        └── fetch_dataset/
            ├── manifest.yaml
            ├── function.py
            └── tests/
                └── test_fetch_dataset.py
```

`function.py`는 함수 원문의 canonical source다. 실행 시 `inspect.getsource()`로 임의 추출하지
않고 파일 원문을 읽어 compiler에 전달한다.

Manifest 필드 초안:

```yaml
name: fetch_dataset
version: 0.1.0
skill: data-access
description: Fetch a dataset for analysis from a supplied query.
creation_rationale: Provide the standard governed path for acquiring analysis data.
function_name: fetch_dataset
input_schema: {}
output_schema: {}
runtime_profiles: [basic]
dependencies: []
risk_categories: [data-access, filesystem-write]
source_sha256: ...
owner: analytics-platform
change_log: []
```

계획 및 실행 기록에 다음 provenance를 남긴다.

- Skill name/version
- Tool name/version
- Tool definition creation rationale, owner and version change record
- Tool source SHA-256
- 검증된 input parameters
- compiler version
- 최종 cell source SHA-256
- Executor execution/operation/step ID

## 5. 사용자 승인 표시

사용자 숙련도를 구분하지 않고 동일한 계획 표현을 사용한다. 함수 source code는 표시하지
않는다.

각 Step에 표시할 정보:

- 수행 목적과 설명
- 선택된 Skill
- 선택된 Tool
- 입력 데이터/주요 parameter 요약
- 예상 결과 또는 생성 Artifact
- 예상 시간/timeout을 알 수 있을 때 그 값
- 위험 판정과 경고

자유 코드 Step은 Skill/Tool 대신 `CUSTOM_CODE`로 표시하고 작업 설명, 입력, 예상 결과와 위험
판정을 보여준다.

승인은 표시된 plan version과 함께 Tool version/source hash, 검증된 parameters를 묶는다.
승인 뒤 Tool source/version 또는 중요한 parameter가 바뀌면 기존 승인을 재사용하지 않는다.

BFF 사용자/감사 API는 compiled Python source를 직접 제공하지 않는다. 실제 실행 코드는
추적된 Executor `execution_id`를 사용해 Jupyter notebook 조회/다운로드 API에서 확인한다.

Parameter는 승인 화면에 표시한다. 긴 query, 대용량 collection 또는 민감할 수 있는 값은
전체 원문 대신 안전한 요약과 checksum을 표시하고, 원문은 권한이 있는 상세조회 계약에서만
제공한다.

## 6. 향후 import 방식으로 전환

장기적으로 Tool 수가 늘고 공통 helper가 많아지면 versioned runtime package를 Jupyter image에
설치하는 방식이 유지보수에 유리하다. 초기 compiler에 다음 rendering mode를 둘 수 있다.

```text
INLINE_DEFINITION  # 초기 구현
RUNTIME_IMPORT     # 향후 Jupyter image에 package 설치
```

Planner와 승인 계약은 두 mode에서 동일하게 Skill/Tool/parameter를 사용하므로 실행 renderer만
교체할 수 있다.

## 7. Fake 데이터 Tool

초기 `fetch_dataset`은 실제 데이터 레이크를 호출하지 않는다.

- 외부에서 작성된 query 문자열을 입력받는다.
- 고정 seed로 재현 가능한 샘플 분석 데이터를 생성한다.
- CSV 또는 Parquet 파일을 Jupyter workspace에 저장한다.
- 파일 경로, format, row/column 수, schema, seed를 dict로 반환한다.
- query 원문 또는 checksum을 lineage에 남긴다.
