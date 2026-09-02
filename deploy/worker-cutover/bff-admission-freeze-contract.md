# BFF START admission freeze contract

Worker 전환 중에는 BFF가 새 Task의 `START`만 차단해야 한다. 단순 운영자 확인이나
Agent Pod의 환경 변수는 BFF가 실제로 요청을 거절한다는 증거가 아니다. BFF는 모든
replica가 공유하는 durable gate를 사용하고 아래 읽기 전용 증거 API를 제공한다.

## 쓰기 경계

배포 파이프라인은 고유한 `freeze_id`를 만들고 인증된 BFF 운영 API로 freeze를
요청한다. 같은 `freeze_id`의 재요청은 멱등이어야 한다. BFF는 Task ID 채번 및
Task 생성과 같은 admission 경계에서 gate를 확인해야 한다. 프로세스 메모리 flag나
서로 다른 replica에 비동기 전파되는 cache만으로 구현하면 안 된다.

freeze가 적용되면 다음 동작을 보장한다.

- 새 Task `START`는 영속 Task나 Agent input receipt를 만들기 전에 거절한다.
- 기존 Task의 승인, 취소, 상태 조회와 이벤트 전달은 계속 허용한다.
- freeze 응답은 모든 BFF replica가 해당 revision을 적용한 뒤에만 성공한다.
- unfreeze는 동일한 `freeze_id`를 요구하며 다른 배포가 건 freeze를 해제하지 않는다.
- freeze/unfreeze 요청자, 사유, 시각, revision, 결과를 감사 로그에 남긴다.

새 `START`의 HTTP 상태는 BFF 정책에 맞게 `423` 또는 `503`을 사용할 수 있지만,
client가 재시도 가능한 전환 차단임을 식별할 안정적인 error code와
`Retry-After`를 제공해야 한다.

## 읽기 전용 증거 API

권장 경로는 `GET /internal/operations/admission-freeze`다. 실제 경로는 바꿀 수
있지만 응답 계약은 다음과 같다.

```json
{
  "schema_version": 1,
  "state": "FROZEN",
  "scope": "NEW_TASK_START",
  "freeze_id": "release-2026-09-02-001",
  "revision": "bff-gate-revision-184",
  "frozen_at": "2026-09-02T03:10:00Z",
  "expires_at": "2026-09-02T05:10:00Z"
}
```

필수 필드는 `schema_version`, `state`, `scope`, `freeze_id`, `revision`,
`frozen_at`이다. `expires_at`은 선택 사항이지만 제공한 경우 preflight 시각보다
미래여야 한다. 모든 시각은 timezone이 있는 RFC 3339 형식이어야 한다.

`ex-agent-cutover-check`는 다음을 검증한다.

- 서비스 간 bearer token으로 API 호출 성공
- schema version 1, `FROZEN`, `NEW_TASK_START`
- 응답 `freeze_id`와 현재 배포의 `CUTOVER_FREEZE_ID` 일치
- 만료되지 않은 receipt
- 안정 구간의 두 조회에서 revision을 포함한 전체 증거가 동일

token은 명령행 인자로 받지 않는다. 기본 환경 변수는
`BFF_CUTOVER_BEARER_TOKEN`이며 Kubernetes Secret으로 주입한다. URL에는 token,
query string, user info를 넣지 않는다. 서비스 메시나 mTLS를 추가해도 응답 계약과
freeze ID 상관관계는 유지한다.

## 해제 조건

통합 Worker와 API가 ready인 것만으로 freeze를 해제하지 않는다. smoke Task 성공,
Session lock 0, 미완료 Worker command 0, 미전송 outbox 0, Redis consumer group의
pending/lag 0을 확인한 뒤 같은 `freeze_id`로 해제한다. 해제 후 새 Task 한 건을
추가 확인하고 배포 증거에 기록한다.
