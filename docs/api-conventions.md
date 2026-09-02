# API Conventions

## Resource audit fields

단건 resource와 목록의 각 item은 다음 필드를 항상 제공한다.

- `created_at`: timezone이 포함된 생성 시각
- `updated_at`: timezone이 포함된 마지막 변경 시각
- `created_by`: 생성 actor
- `updated_by`: 마지막 변경 actor

사용자 작업은 BFF가 서명한 `X-User-ID`, Agent 내부 상태 전이는 `AGENT`, Executor
event에 의한 상태 전이는 `EXECUTOR`, 기존 lineage로 actor를 복원할 수 없는
데이터는 `SYSTEM`을 사용한다. 운영 서명 계약은
[BFF 요청 서명](bff-request-signing.md)을 따른다. Task 생성·resume·cancel 접수
응답도 대상 Task의 현재 audit snapshot을 함께 반환한다.

## Cursor pagination

여러 item을 반환하는 일반 조회는 다음 envelope를 사용한다.

```json
{
  "items": [],
  "next_cursor": null,
  "has_more": false
}
```

`next_cursor`는 서버가 만든 불투명 문자열이다. `has_more=true`이면 같은 filter와
limit에 cursor만 전달해 다음 page를 조회한다. offset과 전체 count는 기본 응답에
포함하지 않는다. 현재 Workflow version과 lifecycle action 목록이 이 계약을
사용한다.

Task event는 장기 연결 SSE이므로 page envelope 대신 `Last-Event-ID`를 cursor로
사용한다. PostgreSQL event ID가 재연결과 누락 복구의 기준이다.

## OpenAPI

공개 endpoint는 명시적이고 고유한 camelCase `operationId`를 사용한다. 공통 오류는
`ErrorResponse`의 `detail`에 표현하며 BFF client가 상태 코드와 operation ID를
안정적인 계약으로 사용할 수 있도록 semantic OpenAPI 회귀 테스트로 보호한다.
