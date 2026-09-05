# Agent / Worker Redis 버전 호환 운영 안내

## 선택할 설정

| 설정 | Redis 서버 | 미처리 메시지 회수 | 안전한 Stream 정리 |
| --- | --- | --- | --- |
| `compat` (기본) | 6.0.8 및 상위 버전 | Lua + XPENDING + XCLAIM | Lua + XRANGE + XDEL |
| `native` | 6.2 이상 | XAUTOCLAIM | XTRIM MINID |

명령 지원 하한과 검증 범위는 다르다. 실제 테스트 대상은 Redis 6.0.8 및
7.4이며, 그 외 버전은 배포 전 아래 테스트를 실행한다. Executor 서비스의
Redis 호환성은 별도 담당 범위이고 이 변경에서는 Executor를 수정하지 않는다.

우리 서비스에서는 `.env` / 컨테이너 환경변수에 다음 하나를 지정한다.

```dotenv
AGENT_REDIS_STREAM_MODE=compat
```

`agent.runtime.config.build_worker_settings()`가 위 값을 공통 Worker 설정으로
전달한다. 우리 API/Worker에서는 `EW_` 값을 중복 설정하지 않는다.
독립적으로 `worker.Settings`를 사용하는 개발자는 `EW_REDIS_STREAM_MODE`를
사용한다. 수동 컨슈머 생성은 `RedisStreamConsumerConfig(redis_stream_mode=...)`,
수동 정리는 `SafeStreamTrimmer(redis_stream_mode=...)`로 지정한다.

지원하지 않는 모드 문자열은 설정 검증에서 거절한다. Redis 6.0.8에서
`native`를 선택하면 미지원 명령 오류로 컨슈머가 실패한다. 오류를 숨기고
계속 Ready인 채 재시도하지 않으므로 설정을 고친 후 다시 시작해야 한다.
일시적인 연결 장애는 기존 backoff 재시도를 유지하고 소비기 건강 상태를 내린다.

Python 클라이언트는 `redis>=5.3,<6`, lock은 5.3.1이다. Redis 6.0 지원을
위한 선택이며 서버를 7.4로 올려도 클라이언트를 다시 바꿀 필요는 없다.

## 적용 범위와 유지되는 계약

- 정식 `src/worker` 및 과거 호환 경로 `ex_agent.transport.consumer`에 적용한다.
- Inbox/Outbox, 중복 처리 방어, SessionGuard, heartbeat, 재시도/DLQ,
  Agent의 LangGraph 실행 계약과 이벤트 스키마는 바꾸지 않는다.
- 회수는 최대 500개씩 검사하며 새 메시지도 페이지 사이에 읽는다.
  아주 큰 Pending 목록 때문에 신규 작업이 계속 뒤로 밀리지 않게 한다.
- 삭제된 본문에 대한 Pending 항목만 정리한다. 살아 있는 본문을 임의 ACK하지 않는다.
- DLQ 다음 페이지 조회는 exclusive cursor 대신 정확한 ID 후속값을 사용한다.
  Stream ID는 uint64 두 부분을 유지하며 부동소수점으로 변환하지 않는다.
- Redis 6.0에는 lag 지표가 없으므로 `-1`로 표시하고 `has_unread`를 별도 제공한다.
  Worker 지표는 `lag_known`도 제공한다. 미확인 lag를 0으로 해석하지 않는다.
  전환 검사는 unread 존재 여부까지 확인하며 확인 불가능하면 전환을 막는다.
- 기동 시 idle consumer 자동 삭제는 두 모드 모두 끈다. 구버전 idle은
  생존 여부가 아니므로 다른 정상 replica의 consumer를 삭제할 수 있기 때문이다.
  consumer 메타데이터 정리는 별도 운영 작업이며 native 전환으로 켜지지 않는다.
- Lua 실행 권한 및 스크립트 내부 명령 권한이 필요하다. EVAL만 허용하면
  충분하지 않다. 기존 잠금/발행/DLQ 스크립트도 계속 사용한다.
  Lua는 명령 간 끼어들기를 막지만 런타임 오류 시 이미 한 쓰기를 롤백하지 않는다.
  전달 보장은 at-least-once이며 핸들러의 멱등성을 계속 유지해야 한다.

## Redis 업그레이드 후 기존 경로로 전환

1. 먼저 새 Redis에서 `compat` 그대로 검증한다. 서버 업그레이드와 알고리즘
   변경을 동시에 할 필요가 없다. DB/Stream/Consumer Group/namespace는 유지한다.
2. 새 서버 버전 및 ACL을 검증한 후 `AGENT_REDIS_STREAM_MODE=native`로 변경한다.
3. 같은 설정으로 API와 Worker 컨테이너를 재시작한다. Kubernetes는 ConfigMap
   `deploy/k8s/configmap.yaml.example`의 값을 바꾼 후 rollout한다.
   환경변수는 실행 중인 프로세스에 자동 반영되지 않는다.
4. readiness, Pending 회수, Inbox/Outbox 전달, 이벤트 재개와 DLQ를 확인한다.

Compose에서 설정 변경 후 반영하는 예:

```bash
# .env를 AGENT_REDIS_STREAM_MODE=native로 수정한 뒤
docker compose up -d --build --force-recreate api worker
```

전환 중 두 모드가 같은 group을 사용하는 것은 가능하지만, 운영에서는 기존
신규 접수 중지/배포 drain 절차를 따른다. 실행 중인 핸들러가 있다면 graceful
shutdown을 기다린다. 강제 종료 시에도 Pending 및 durable state로 복구하지만
이미 수행한 외부 부수효과까지 exactly-once가 되는 것은 아니다.

## 다시 호환 경로로 복귀

`.env` 또는 ConfigMap을 `AGENT_REDIS_STREAM_MODE=compat`로 되돌리고 같은
API/Worker 재시작 절차를 실행한다. 코드 revert, DB migration 되돌리기,
consumer group 삭제/재생성, Pending 초기화는 필요하지 않다.

이것은 **클라이언트 알고리즘 복귀**다. Redis 서버 바이너리/RDB/AOF를
다운그레이드하는 절차가 아니며 서버 자체 롤백은 별도 백업·복원 계획이 필요하다.

## Stream 정리의 차이

두 모드 모두 retention, 최소 보관 개수, 가장 느린 group의 읽기 경계,
가장 오래된 Pending 경계를 원자적으로 재검증한다.

`compat`는 한 번에 최대 500개만 삭제한다. 안전한 대상이 더 많으면 다음
정리 작업으로 이어서 실행한다. API에서 같은 멱등키를 재사용하면 기존 작업
결과가 반환되므로 **다음 정리 작업은 새 요청/멱등키**로 제출한다.
CLI도 필요하면 반복 실행한다. 반환된 삭제 건수는 그 실행에서 실제 삭제한 수다.
`native`는 기존 XTRIM MINID 경로다. XDEL은 논리적 삭제이며 일부 항목만
삭제된 내부 노드의 메모리가 즉시 회수되는 것은 아니므로 메모리 감소 효과나
대량 정리 소요시간이 XTRIM과 같다고 가정하지 않는다.

```bash
ex-agent-stream-maintenance --stream agent.commands \
  --redis-stream-mode compat plan
# 위 결과 확인 후에만 실제 정리
ex-agent-stream-maintenance --stream agent.commands \
  --redis-stream-mode compat trim --yes
```

기존 `STREAM_CLAIM_BATCH_SIZE` 범위도 1~500으로 제한했다. 기본값 10은 동일하다.
기존에 500을 넘겨 지정한 환경만 값을 낮춰야 한다.

## 격리된 재현 테스트

저장소 루트에서 실행한다. 별도 tmpfs Redis/PostgreSQL을 사용하며 실행 중인
Executor 인프라 및 사용자 데이터는 접근하지 않는다. 테스트 이미지는
`uv sync --frozen --no-editable`로 설치된 패키지를 사용한다.

```bash
TEST_REDIS_IMAGE=redis:6.0.8 docker compose \
  -f deploy/worker/compose.test.yaml run --build --rm test \
  python -m pytest -q -p no:cacheprovider -m 'not llm and not executor'
docker compose -f deploy/worker/compose.test.yaml down

TEST_REDIS_IMAGE=redis:7.4-alpine docker compose \
  -f deploy/worker/compose.test.yaml run --build --rm test \
  python -m pytest -q -p no:cacheprovider -m 'not llm and not executor'

# 위에서 만든 같은 상위 버전 서버로 native 기본 설정도 검증한다.
TEST_REDIS_IMAGE=redis:7.4-alpine docker compose \
  -f deploy/worker/compose.test.yaml run --rm \
  -e AGENT_REDIS_STREAM_MODE=native -e EW_REDIS_STREAM_MODE=native test \
  python -m pytest -q -p no:cacheprovider -m 'not llm and not executor'
docker compose -f deploy/worker/compose.test.yaml down
```

회수 경쟁, heartbeat, 없는 본문, 신규 메시지 공정성, ACL 실패, 연결 복구,
6.0 native 오류, 상위 버전에서 compat → native → compat Pending 인계,
정리 경계/분할 정리, DLQ 페이지, lag 없는 전환 검사를 포함한다.
외부 LLM과 실제 Executor/Jupyter 전체 경로는 이 테스트와 별개다.

2026-09-05 검증 결과 (`redis-py 5.3.1`, Python 3.12, non-editable 설치):

| 서버 | 기본 설정 | 결과 |
| --- | --- | --- |
| Redis 6.0.8 | compat | 570 passed, 3 skipped, 28 deselected |
| Redis 7.4.10 | compat | 569 passed, 4 skipped, 28 deselected |
| Redis 7.4.10 | native | 569 passed, 4 skipped, 28 deselected |

3개 skip은 live API 비활성화이고 상위 서버의 추가 1개 skip은 Redis 6.0의
native 미지원 오류 전용 검사다. 28개는 외부 LLM/Executor 테스트 제외분이다.
각 실행에는 명시적 모드별 검사도 포함된다. 상위 버전 native 실행은 Agent와
공통 Worker 환경변수를 모두 native로 지정해 durable pipeline 회귀도 확인했다.
ruff check/format 및 ty도 테스트 이미지에서 통과했다. 독립 전달용 프로젝트는
동명의 agent/worker 패키지가 있으므로 루트 ty 검사에 섞지 않고 해당 디렉터리에서
별도 ty 검사를 수행한다.

명령 지원 근거: [XAUTOCLAIM](https://redis.io/docs/latest/commands/xautoclaim/),
[XTRIM](https://redis.io/docs/latest/commands/xtrim/),
[XINFO GROUPS](https://redis.io/docs/latest/commands/xinfo-groups/),
[XINFO CONSUMERS](https://redis.io/docs/latest/commands/xinfo-consumers/),
[redis-py 5.3.1](https://github.com/redis/redis-py/tree/v5.3.1#supported-redis-versions).
