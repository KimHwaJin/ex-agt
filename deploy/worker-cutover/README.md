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

1. 배포별 고유 `freeze_id`를 만들고 BFF에서 **새 Task START만** 차단한다.
   기존 Task의 승인·취소·조회는 drain을 위해 유지한다. BFF 구현 계약은
   [bff-admission-freeze-contract.md](bff-admission-freeze-contract.md)를 따른다.
2. 레거시 API/Worker를 유지한 채 모든 Task를 terminal 상태로 만든다. 장기 실행은
   완료 또는 명시적 취소까지 기다린다.
3. BFF 증거 URL, 동일한 freeze ID와 service token을 주입해 preflight를 실행한다.
   token은 명령행이나 로그에 넣지 않는다.

   ```bash
   export BFF_CUTOVER_BEARER_TOKEN='<secret injection>'
   ex-agent-cutover-check \
     --admission-evidence-url \
     http://bff-api/internal/operations/admission-freeze \
     --expected-freeze-id release-2026-09-02-001 \
     --stable-seconds 10
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
   pending/lag가 다시 0으로 수렴한 뒤 같은 freeze ID로 BFF START 차단을
   해제한다.

## 판정 범위

preflight는 읽기만 수행하며 다음을 두 번 동일하게 관찰해야 성공한다.

- BFF의 인증된 freeze receipt와 현재 배포 freeze ID 일치
- 안정 구간 중 BFF freeze revision 불변
- 비종료 `agent_tasks` 0
- 미완료 `agent_workflow_commands` 0
- 미발행 `agent_task_events` 0
- 잠긴 `agent_session_locks` 0
- command와 Executor event consumer group의 pending/lag 0
- 두 consumer group의 `last-delivered-id`가 안정 구간 동안 불변

실패 시 출력 JSON의 `blockers`를 해소하기 전에는 새 Worker를 기동하지 않는다.
격리된 로컬 리허설에서만
`--unsafe-accept-operator-freeze-assertion`을 사용할 수 있다. 운영 Job과 배포
파이프라인에는 이 옵션을 넣지 않는다.

## 단계별 롤백 경계

현재 단계를 배포 기록에 append-only로 남긴다. 판단은 다음 명령으로도 확인할 수
있다.

```bash
ex-agent-cutover-rollback --phase TARGET_STARTED
```

| 마지막 완료 단계 | 허용 동작 |
|---|---|
| `ADMISSION_OPEN` | 전환 전이므로 기존 운영 유지 |
| `FREEZE_VERIFIED`, `LEGACY_DRAINED` | 레거시 유지 후 freeze 해제 가능 |
| `LEGACY_STOPPED` | 고정된 레거시 이미지를 다시 기동한 뒤 검증 가능 |
| `MIGRATION_APPLIED` | 레거시 schema compatibility 검증 전 재기동 금지 |
| `TARGET_STARTED` 이후 | 레거시 재기동 금지, 호환 target으로 전진 복구 |

`TARGET_STARTED`가 point of no return이다. 통합 Worker는 기동 직후 이벤트를
소비하거나 session 기반 checkpoint를 쓸 수 있으므로 실제 smoke 성공 여부와
무관하다. 이 경계를 지난 후 레거시를 같은 group에 다시 넣으면 유실이나 잘못된
graph 재개가 발생할 수 있다.

## 배포 증거

각 단계에서 다음 값을 하나의 배포 기록에 남긴다.

- freeze ID, BFF revision, freeze/unfreeze 감사 event ID
- 레거시와 target image digest, Git commit
- 두 preflight Job 이름과 전체 JSON 결과
- 레거시 API/Worker Pod UID와 종료 확인 시각
- migration Job 이름과 적용 revision
- target API/Worker Pod UID, readiness 확인 시각
- smoke Task ID, Execution ID와 최종 drain 지표

레거시 scale 0 전과 후의 preflight는 서로 다른 Job으로 실행한다. 예시 manifest는
`generateName`을 사용하므로 결과를 덮어쓰지 않는다.
