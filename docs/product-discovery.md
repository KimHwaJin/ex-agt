# Agent Product Discovery

## 현재 합의된 목표

다수 사용자가 사용하는 데이터 분석 및 코드 실행 Agent를 만든다. 주요 사용자는 데이터
분석 경험이 적지만 분석을 수행하고 싶은 사용자와, 분석 경험은 있지만 반복 작업을
Agent로 줄이고 싶은 사용자다.

지원할 사용자 의도:

1. 데이터 분석 관련 질의응답
2. 일반 질의응답
3. 데이터 분석 실행 요청
4. 자유 코드 실행 요청

단순 질의응답은 실행 계획과 Executor 없이 답할 수 있다. 데이터 분석/코드 실행 요청은
계획 생성, HITL 승인, Executor 실행, 결과 종합, 리포트 생성 단계를 거친다.

## 현재 권장 프레임워크 경계

- LangChain: `create_agent`, 모델, structured output, planning middleware
- LangGraph: 의도 분류, 계획, HITL, SINGLE/MULTI loop, recovery, report 상태 머신
- Executor: 승인된 Python Step의 비동기 실행과 실행 증거 보존

제품 상태 전이는 명시적인 LangGraph가 소유한다. V1 planning leaf는 LangChain
`create_agent()`와 custom middleware로 구현하며 Executor lifecycle을 제어하지 않는다. Deep
Agents는 V1 dependency에서 제외하고 대규모 file context/subagent가 실제로 필요할 때 재평가한다.

## 초안 워크플로

```text
receive_input
  -> classify_intent
      -> answer_general -------------------------------> END
      -> answer_data_question -------------------------> END
      -> clarify_execution_request (필요할 때)
          -> choose_execution_mode/runtime_profile
          -> load_relevant_skills (분석 요청만)
          -> build_structured_plan
          -> validate_plan
          -> human_review (approve / revise / reject)
              -> reject_response ----------------------> END
              -> revise_plan -> build/validate -> human_review
              -> submit_approved_plan
                  -> SINGLE wait terminal -> reconcile_result
                  -> MULTI wait operation boundary
                       -> interpret_step_result
                       -> decide next step or finalize
                       -> append operation / finalize
                  -> build_final_report ----------------> END
```

## 계획 데이터 초안

각 계획은 최소 다음을 가져야 한다.

- 목적과 사용자 의도
- execution mode: SINGLE 또는 MULTI
- runtime profile
- 예상 입력 데이터와 접근 방법
- 순서가 있는 Step 목록
- Step별 설명, 생성 코드, timeout
- Tool 기반 Step이면 skill/tool/parameter lineage
- 자유 코드 Step이면 그 사실을 나타내는 provenance
- 예상 산출물과 검증 기준
- 위험, 비용 또는 사용자가 알아야 할 가정

## 최종 리포트 초안

- 요청과 분석 목적
- 승인된 최초 계획
- 실제 수행한 Operation/Step 및 변경 이력
- 사용한 Skill/Tool과 자유 코드 구분
- 데이터와 방법론
- 핵심 결과와 근거
- 오류, 누락, 제한 및 결과 completeness
- 생성 Artifact/Notebook/결과 참조
- 다음 추천 작업

## 아직 확정되지 않은 핵심 결정

### 제품/API 경계

- Agent 서비스가 제공할 외부 transport와 인증 주체
- user/project/session/task ID의 발급 및 소유 시스템
- 한 conversation과 한 Task/Execution의 관계

### 모델과 데이터 접근

- 사용할 LLM provider/model과 모델 라우팅 정책
- 사용자 데이터의 위치, 업로드/선택 방식, 권한 검증 주체
- 허용 데이터 크기, PII/민감정보 정책, 보존 기간

### Skill과 Tool

- 최초 제공 분석 도메인과 Tool 목록
- Skill이 설명만 제공하는지, 호출 가능한 Python 함수 구현도 포함하는지
- Tool 함수 코드가 Jupyter에 정의/실행되는 정확한 cell 형태
- Tool 버전, 의존성, 재현성 및 lineage 규칙
- 사용자가 수정 시 “Skill/Tool을 사용하지 말라”는 의도를 표현하는 API/UI 계약

### 실행 정책

- SINGLE/MULTI의 사용자 선택 또는 자동 선택 규칙
- 기본 Runtime profile과 package availability
- 실행할 수 있는 자유 코드의 보안 제한
- 네트워크, filesystem, credential, 외부 side effect 정책
- timeout, 비용, 동시 실행, 취소, 재실행 정책

### HITL

- 승인 화면에서 코드 전문까지 보여줄지
- 수정 요청이 자연어 feedback인지 structured edit인지
- 승인된 계획 이후 코드가 달라질 수 있는 허용 범위
- MULTI의 후속 Step마다 추가 승인이 필요한지

### 결과와 메모리

- 결과 리포트의 저장 위치와 포맷
- 사용자별 장기 기억 범위와 삭제/수정 정책
- 대화 checkpoint와 업무 기록의 보존 기간
- 결과 manifest를 LLM context로 가져오는 크기/MIME 정책

### 운영

- 개발/운영 배포 형태
- 이벤트 consumer의 scale-out 및 동일 Execution 직렬화 방식
- 관측성, 감사로그, 모델/실행 비용 추적

## 인터뷰 상태

현재 상태: `V1_IMPLEMENTATION_STARTED`

## 인터뷰 1차 답변

확정된 내용:

- BFF 서비스가 Agent application을 직접 포함한다. BFF와 Agent는 별도 서비스가 아니다.
- `user_id`는 Frontend 요청에서 전달된다.
- `project_id`, `session_id`, `task_id`는 BFF API가 발급한다.
- 분석 및 코드 실행은 최대 5일까지 지속될 수 있다.
- 최초 데이터는 데이터 레이크에서 Jupyter workspace로 다운로드한다.
- 데이터 다운로드 자체도 수시간에서 수일 걸릴 수 있다.
- 데이터 다운로드 함수와 분석 함수는 Agent Skill/Tool로 제공한다.
- 실제 함수 catalog는 아직 전달되지 않았으므로 예제 함수로 골격을 개발한다.
- 올해에는 강제 sandbox를 구현하지 않는다.
- 코드 생성 전과 실행 전 LLM 위험 판정을 수행하고 사용자에게 위험을 알린다.
- 실행계획 승인 커밋부터 성공 리포트 또는 실패/취소 사용자 메시지 저장 완료까지 해당 Session을
  잠그고 새로운 채팅 요청을 받지 않는다. 상태 조회, SSE reconnect, 결과 조회, 취소 요청은
  계속 허용한다. 사용자-facing terminal 상태 저장 후 잠금을 해제한다.
- MULTI 후속 Step은 최초 승인된 전략 범위에서 자동 실행하고, 중대한 계획 변경·새 데이터
  접근·위험도 상승·새 외부 부수효과가 있을 때만 재승인한다.
- 위험도 `LOW`는 일반 진행, `MEDIUM`은 승인 화면 경고, `HIGH`는 추가 확인,
  `CRITICAL`은 실행 차단으로 처리한다.
- 데이터 레이크 query는 외부 작업자가 작성해 데이터 조회 함수에 전달한다. 초기 Fake
  adapter는 실제 조회 대신 결정론적인 샘플 분석 데이터를 workspace에 생성한다.
- 자유 코드 실행의 SINGLE/MULTI mode는 Agent가 추론하지 않고 사용자가 선택한다. 데이터 분석
  실행 mode는 승격 Workflow 선택 시 SINGLE, 미선택/후보 없음 시 MULTI로 결정한다.
- 사용자 숙련도는 구분하지 않는다. 승인 화면에는 code source 대신 Step 설명과 선택된
  Skill/Tool을 표시한다.
- Tool 기반 Jupyter cell은 Agent 저장소의 고정된 함수 정의 source와 검증된 호출문을 함께
  포함한다. LLM은 Tool 구현을 매번 다시 작성하지 않는다.
- 자유 코드 요청에서만 LLM이 함수 정의 전체를 생성한다.
- 사용자 mode 선택에 대해 휴리스틱 추천/경고를 하지 않는다. 선택된 mode와 요청 의미가
  양립 불가능할 때만 clarification으로 모순을 해소한다.
- 승인 화면에는 Tool parameter를 표시한다. 긴 query는 요약과 checksum으로 표시한다.
- 실행계획은 LangGraph checkpoint에만 두지 않고 BFF PostgreSQL의 append-only Plan,
  PlanRevision, PlanStep으로 저장한다.
- 모든 Step은 Tool 선택 이유 또는 Custom Code 생성 이유, 입력 근거, 예상 결과와 검증 기준을
  구조화해 저장한다. 모델의 비공개 사고과정 원문은 저장하지 않는다.
- Agent PlanStep과 Executor execution/operation/step/result/artifact ID를 BFF mapping으로
  연결해 최종 리포트까지 추적한다.
- LangGraph production checkpointer는 PostgreSQL을 사용하고
  `thread_id=task_id`로 task 실행 상태를 격리한다.
  BFF Message/Event 테이블은 Frontend 복원의 원본이고, checkpoint는 Agent workflow 재개의
  원본이다.
- 일반 대화에는 개별 `message_id`를 사용하고 하나의 실행 요청부터 최종 리포트까지 같은
  `task_id`를 유지한다.
- 모델은 LangChain `init_chat_model`로 생성하고 provider/model/API 설정은 외부 환경변수로
  주입한다.
- Agent와 Executor의 명령 통신은 Executor REST API를 사용한다. Executor lifecycle 알림은
  Executor의 Redis Stream event를 Agent 전용 consumer group으로 구독한다. MCP는 초기 통합
  경로로 사용하지 않는다.
- 개발 단계부터 실제 PostgreSQL과 Redis를 사용한다. 단위 테스트용 in-memory fake는 허용하되,
  PostgreSQL checkpoint/도메인 저장소/pgvector와 Redis consumer의 통합 테스트를 별도로 둔다.
- 로컬 환경은 Executor Compose의 PostgreSQL/Redis를 공유한다. Executor와 Agent는 같은
  PostgreSQL server를 쓰되 서로 다른 database/credential을 사용하고 테이블을 공유하지 않는다.
  Executor Compose는 `pgvector/pgvector:pg17`과 fresh volume용 `CREATE EXTENSION vector`로 이미
  갱신되었다. Agent bootstrap은 별도 `agent` database/role과 해당 DB의 vector extension을 만든다.
- Executor `executor.events`를 Agent 전용 consumer group으로 읽되 `executor.work`는 소비하지
  않는다. Agent 내부 command/wake-up stream은 별도 이름을 사용한다.
- 초기 예제 Tool은 `fetch_dataset`, `inspect_dataset`, `profile_missing_values`,
  `summarize_numeric_columns`, `group_aggregate`, `plot_distribution`으로 한다. 각 Tool은 고정된
  source, manifest, 입력/출력 schema, version/hash와 테스트를 가진다.
- 모델 원본 prompt/response와 비공개 사고과정은 저장하지 않는다. model/prompt template
  version, 입력 참조, structured output, trace ID, token/latency metadata를 저장한다.
- 사용자는 PlanRevision 이력과 revision 간 diff/변경 이유를 조회할 수 있다.
- 실제 실행 코드는 BFF에서 제공하지 않고, BFF가 추적한 `execution_id`로 Executor의 Jupyter
  notebook 조회/다운로드 API를 사용한다. PlanStep에는 execution/operation/executor step ID
  mapping을 저장한다.
- 최종 리포트는 성공한 Task에만 생성한다. 실패는 실패 내용/원인 메시지, 취소는 Executor
  취소 완료 확인 후 취소 메시지만 저장한다.
- SINGLE 실패 시 Agent가 코드를 자동 수정하거나 다시 실행하지 않는다. Executor 통신 오류만
  동일 멱등성 키를 사용하는 제한된 backoff retry를 적용한다.
- MULTI 오류 보정은 승인 전략 범위에서 최대 3회 자동 수행하며, 최종 실패 시 리포트 없이
  실패 내용과 원인을 사용자에게 알린다.
- 사용자는 자연어 query만 전달하며 LLM structured output이 일반 Q&A, 데이터 분석 Q&A,
  데이터 분석 실행, 자유 코드 실행으로 분류한다. 의미 분류에 rule/keyword routing을 사용하지
  않는다.
- 자유 코드 실행 intent인데 mode가 없으면 HITL로 사용자의 SINGLE/MULTI 선택을 받는다.
- 성공 리포트는 prompt만으로 형식을 관리하지 않는다. Pydantic structured output,
  authoritative Plan/Executor assembler, 결정론적 Markdown renderer를 함께 사용한다.
- 생성한 Markdown은 Executor REPORT Artifact API로 materialize하고 notebook에 Markdown cell로
  append한다. 같은 내용을 BFF가 사용자 화면에 표시한다.
- 데이터 분석 실행 요청은 동적 계획 생성 전에 사용자 승격 Workflow를 PostgreSQL/pgvector로
  검색하고 최상위 eligible Workflow를 제안한다.
- Workflow는 성공한 Tool 기반 Plan을 사용자가 명시적으로 승격한 불변 version이다. 초기에는
  CUSTOM_CODE가 포함된 Plan을 승격하지 않는다.
- 사용자가 제안 Workflow를 선택하면 고정 계획을 SINGLE로 실행하고, 선택하지 않거나 적합한
  Workflow가 없으면 Skill/Tool 기반 동적 MULTI 계획으로 진행한다.
- Workflow 검색은 초기 `SERVICE` visibility와 Skill/Tool/runtime compatibility를 vector
  similarity 전에 filter하며 검색 결과만으로 자동 실행하지 않는다.
- 초기 Workflow 공개 범위는 인증된 서비스 전체 사용자다. 모델에는 향후 USER/PROJECT/ROLE
  ACL을 추가할 수 있는 access policy 필드를 둔다.
- 서비스 전체 공개 Workflow에는 원본 query, 실제 데이터 값과 사용자/프로젝트 식별자를
  포함하지 않고 parameter template만 저장한다.
- Workflow의 모든 Step은 Skill과 Tool의 name/version/hash, 선택 이유와 parameter template를
  함께 추적한다.
- 사용자가 binding parameter와 전체 Workflow Step을 확인하고 선택하는 행위가 계획 승인이다.
  선택 transaction에서 Session을 잠그고 SINGLE 실행으로 진행한다.
- 자유 코드 실행은 Workflow 검색 없이 사용자가 SINGLE/MULTI를 직접 선택한다.
- 성공 Task의 Workflow 승격 시 Agent가 이름/설명/요청 예시/태그 초안을 만들고 사용자가
  확인·수정 후 승격한다.
- 초기에는 모든 인증 사용자가 승격할 수 있지만 모든 승격 요청은 versioned
  `PromotionPolicy`를 통과한다. 향후 역할/조직/프로젝트 자격을 추가할 수 있다.
- Workflow 검색은 상위 3개를 기본 표시하고 cursor로 추가 후보를 조회한다.
- 초기 구현 범위에 Agent core뿐 아니라 실제 PostgreSQL/pgvector migration, Redis worker/event
  subscriber, Executor REST adapter와 최소 FastAPI/SSE endpoint를 포함한다.
- 운영 배치는 같은 Kubernetes Pod 안에서 같은 image를 사용하는 `api`와 `worker` 두 container로
  분리한다. API는 bounded foreground graph 구간을, worker는 승인 후 실행과 event resume을 맡는다.
- 초기 Jupyter runtime profile은 `basic`이다. 기본 profile에 pandas, NumPy, PyArrow,
  Matplotlib, Plotly, Polars 등 예제 분석 Tool에 필요한 package가 포함돼 있다. 이후 특정 Skill이
  필요할 때만 `ml` profile을 요구한다.
- Agent database와 전용 role은 같은 PostgreSQL server에 별도로 bootstrap한다. `agent`
  database에도 `CREATE EXTENSION vector`를 적용한다.
- 초기 FastAPI 인증 구현은 교체 가능한 `IdentityProvider` port를 사용한다. 개발환경에서는
  전달된 `user_id`를 신뢰하고, 운영에서는 upstream이 검증한 `UserContext` adapter로 교체한다.
- 하나의 Task SSE가 answer delta와 계획/승인/실행/terminal/report event를 전달한다. Token
  delta는 transient이고 완성된 Assistant message와 주요 상태 event만 영속화한다.
- 상세 LangGraph 구조는 `docs/langgraph-design.md`의 사용자 승인을 받은 뒤 구현한다.
- V1 planner는 Deep Agents가 아니라 LangChain `create_agent()`를 사용한다. Skill 후보 검색,
  Skill/Tool manifest context 주입, 허용 범위, model audit와 structured PlanDraft 검증은 custom
  middleware가 담당한다.
- Jupyter에서 실행될 분석 Tool은 LangChain callable Tool로 planner에게 제공하지 않는다.
  Planner는 middleware가 주입한 versioned manifest를 보고 Tool 이름/parameter만 PlanDraft에서
  선택하며, 실제 source binding/compile/실행은 application service가 수행한다.

현재 확정:

- BFF API process는 질의응답·분류·계획·HITL까지의 bounded graph 구간을 직접 호출할 수 있다.
- 승인 이후에는 durable command/outbox를 저장하고 Browser에 `202`를 반환한다. 같은 BFF
  코드베이스의 Agent Workflow Worker가 PostgreSQL checkpoint에서 LangGraph를 실행/재개한다.
- Redis Stream은 BFF 내부 API/Worker wake-up과 Executor event 구독에 사용한다.
- 장기 작업 진행은 durable event history + SSE + snapshot/polling fallback으로 Frontend에
  전달한다.
- LLM risk review는 advisory이며 실제 sandbox 또는 권한 경계가 아니다.
- 위험 판정은 middleware에만 숨기지 않고 명시적 LangGraph state/node로 남긴다.

아래 조건이 충족되면 `READY_FOR_IMPLEMENTATION`으로 변경한다.

- 첫 번째 릴리스 범위와 비범위를 명시했다.
- 외부 API, identity, storage 소유권을 정했다.
- Skill/Tool 및 계획 schema를 합의했다.
- HITL revise/approve/reject 의미를 합의했다.
- SINGLE/MULTI와 오류·취소·재시도 정책을 합의했다.
- sandbox, 데이터 권한, 보안 경계를 합의했다.
- checkpoint/store/event subscriber의 영속성 경계를 합의했다.
- acceptance scenario와 테스트 전략을 합의했다.
