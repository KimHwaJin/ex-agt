# Workflow Operations API

Workflow 운영 API는 BFF가 전달한 `X-User-ID`와 Workflow owner를 비교한다.
현재 V1은 owner만 조회·변경할 수 있으며 application policy port를 교체해 향후
관리자·조직 권한을 추가할 수 있다. 공개 Workflow 실행 후보 검색은 이 운영 권한과
별개다.

## 조회 API

- `GET /api/v1/workflows/{workflow_id}`: 상태, 최신 version과 활성 version
- `GET /api/v1/workflows/{workflow_id}/versions`: immutable version 목록
- `GET /api/v1/workflows/{workflow_id}/versions/{version_id}`: 공개 Plan,
  입력 계약, Skill/Tool, 선택 이유와 source Task/Plan/Execution lineage
- `GET /api/v1/workflows/{workflow_id}/lifecycle-actions`: 요청자, 작업, 사유,
  idempotency key, 요청 hash, 적용 정책과 당시 결과 snapshot

목록 API의 `limit`은 1~100이다. 응답의 `next_cursor`를 다음 요청의 `cursor`로
그대로 전달하며, cursor 내부 형식에 의존하면 안 된다. version 목록은 version
번호, 감사 이력은 생성시각과 action UUID를 사용하는 keyset pagination이다.

## 상태 변경 API

- `POST /api/v1/workflows/{workflow_id}/versions`
- `POST /api/v1/workflows/{workflow_id}/versions/{version_id}/reviews`
- `POST /api/v1/workflows/{workflow_id}/versions/{version_id}/activate`
- `POST /api/v1/workflows/{workflow_id}/status`

새 version은 `PENDING_REVIEW`와 비활성 상태로 생성된다. `APPROVE`는 기존 활성
version을 내리고 대상 version만 활성화한다. `REJECTED` version은 활성화할 수
없다. 모든 상태 변경은 idempotency key와 요청 hash를 비교하며 같은 key에 다른
payload를 사용하면 `409`를 반환한다.
