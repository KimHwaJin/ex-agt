# Legacy Worker → integrated Worker cutover

이 전환은 일반 rolling update가 아니다. 구·신 Worker를 같은 Redis consumer
group에 동시에 넣지 않는다.

## 왜 혼합 배포가 금지되는가

| 계약 | 레거시 Worker | 통합 Worker |
|---|---|---|
| command envelope | command_id, task_id | schema_version, namespace, command_id, generation |
| checkpoint thread_id | task_id | session_id |
| 전달 저장소 | agent_workflow_commands, agent_stream_inbox | ew_inbox, ew_commands, ew_outbox |
| Executor event 처리 | 업무 binding 조회 후 직접 graph command 생성 | 먼저 inbox ACK 후 binding 기반 command routing |

같은 group에서는 이벤트 한 건을 Pod 하나만 받는다. 통합 Worker가 레거시 실행
이벤트를 받으면 레거시 graph가 재개되지 않고, 레거시 Worker가 새 command를 받으면
envelope를 해석하지 못한다. 이것은 중복 처리보다 더 위험한 유실/정체 조건이다.

## 전환 순서

1. BFF에서 **새 Task START만** 차단한다. 기존 Task의 승인·취소·조회는 drain을
   위해 유지한다.
2. 레거시 API/Worker를 유지한 채 모든 Task를 terminal 상태로 만든다. 장기 실행은
   완료 또는 명시적 취소까지 기다린다.
3. 아래 preflight를 실행한다. `--admissions-frozen`은 도구가 BFF를 변경하는 옵션이
   아니라 운영자가 이미 차단했음을 명시하는 assertion이다.

   ```bash
   ex-agent-cutover-check --admissions-frozen --stable-seconds 10
   ```

4. 결과가 `ready: true`이면 레거시 Worker를 0개로 scale하고 같은 검사를 다시
   실행한다. 두 번째 검사까지 DB backlog, Session lock, Redis PEL/lag가 0이어야
   한다.
5. `ex-agent-migrate` Job으로 Agent, `ew_*`, LangGraph checkpoint migration을
   완료한다.
6. 통합 Worker를 먼저 배포하고 `/health/ready`를 확인한다. 이어서 통합 API를
   배포한다. 두 Deployment 모두 같은 Worker namespace, Stream/group 이름,
   PostgreSQL checkpoint DB를 사용해야 한다.
7. smoke Task가 완료되고 Session lock, Worker command/outbox, 두 group의
   pending/lag가 다시 0으로 수렴한 뒤 BFF START 차단을 해제한다.

## 판정 범위

preflight는 읽기만 수행하며 다음을 두 번 동일하게 관찰해야 성공한다.

- 비종료 `agent_tasks` 0
- 미완료 `agent_workflow_commands` 0
- 미발행 `agent_task_events` 0
- 잠긴 `agent_session_locks` 0
- command와 Executor event consumer group의 pending/lag 0
- 두 consumer group의 `last-delivered-id`가 안정 구간 동안 불변

이 검사는 BFF 차단 여부를 원격 검증할 수 없다. 배포 파이프라인의 승인 단계와
BFF 지표를 별도로 증거로 남긴다. 실패 시 출력 JSON의 `blockers`를 해소하기 전에는
새 Worker를 기동하지 않는다.
