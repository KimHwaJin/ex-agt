# Performance Backlog

이 문서는 이번 성능 개선 범위에서 의도적으로 구현하지 않은 항목을 추적한다.
두 항목 모두 실제 운영 데이터와 모델이 준비된 뒤 benchmark를 먼저 수행한다.

## P1. pgvector ANN index와 조회 projection 최적화

상태: `NOT_IMPLEMENTED`

현재 Workflow 검색은 정확한 cosine distance 정렬을 사용한다. 실제 embedding
모델과 운영 규모가 확정되기 전에는 HNSW/IVFFlat index를 추가하지 않는다.

구현 전 확인 항목:

- 실제 embedding 모델, 차원, 거리 함수 확정
- Workflow version 수와 검색 QPS를 반영한 baseline 측정
- 활성 version만 대상으로 하는 partial HNSW index 검토
- 검색 단계에서는 plan 전체 JSON 대신 식별자와 score만 먼저 조회하는
  projection 검토
- `ef_search`, recall, p95 latency, index build/쓰기 비용 비교
- embedding 모델 변경 시 재색인과 index 교체 절차 정의

완료 조건은 brute-force 대비 recall 허용 기준을 만족하면서 p95가 유의미하게
개선되는 것이다.

## P1. Workflow risk 결과 사전 계산

상태: `NOT_IMPLEMENTED`

현재 승격 Workflow를 사용할 때도 risk prerequisite를 실행 시점에 평가한다.
Workflow 승격/새 version 생성 시 정적 risk 결과를 계산해 저장하는 최적화는
승격 API와 권한 정책이 구현된 뒤 진행한다.

구현 전 확인 항목:

- Workflow version에 risk level, 근거, policy version, 평가 시각 저장
- Skill/Tool registry hash와 risk policy version을 cache invalidation key로 사용
- 사용자 입력과 parameter처럼 실행 시점에만 알 수 있는 위험은 별도 동적 평가
- 승격 자격과 재검토 권한 정책 정의
- 기존 Workflow version backfill과 policy 변경 시 재평가 job 설계

사전 계산은 실행 전 코드 생성·실행 guardrail을 대체하지 않으며, 변경되지 않은
Workflow 구조에 대한 반복 LLM 호출만 줄이는 용도다.
