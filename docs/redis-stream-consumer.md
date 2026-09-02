# Reusable Redis Stream Consumer

`ex_agent.transport.consumer`는 Agent 도메인, LangGraph, PostgreSQL에 의존하지
않는 Redis Stream 소비 런타임이다. 별도 Agent 구현에서는 이 모듈만 패키지로
가져가거나 파일 단위로 복사하고, 메시지별 Handler를 구현하면 된다.

## 보장하는 동작

- consumer group 생성과 무한 `XREADGROUP` loop
- configurable bounded concurrency와 slot별 Handler instance
- 처리 중 분산 락과 Stream PEL lease의 주기적 갱신
- handler의 durable side effect뿐 아니라 ACK/DLQ 확정까지 lease 유지
- `XAUTOCLAIM` cursor를 끝까지 전진시키는 stale message 복구
- 성공 후에만 ACK하는 at-least-once delivery
- 재시도 가능한 실패는 PEL에 유지
- handler가 실제로 요청한 retry 횟수를 Redis에 기록하고 설정된 상한을 넘으면
  poison message를 DLQ로 이동
- 영구적인 envelope 오류는 DLQ 기록과 원본 ACK를 하나의 Redis transaction으로
  수행
- pending이 없고 충분히 오래 idle한 consumer metadata 정리
- transport 오류에 대한 bounded exponential backoff
- metrics/log adapter를 위한 observer contract

Business side effect는 중복 실행될 수 있으므로 Handler는 idempotent해야 한다.
이 프로젝트의 command/event Handler는 PostgreSQL idempotency와 event sequence
검증을 함께 사용한다.

retry counter는 Stream PEL의 delivery count와 별도로 관리한다. PEL delivery
count에는 같은 business lock을 기다린 claim도 포함될 수 있기 때문이다. 따라서
`RETRY` 반환이나 retry 가능한 handler 예외만 counter를 증가시킨다. counter key는
stream/group/message ID의 SHA-256 식별자를 사용하고 TTL이 있어 비정상적인 Stream
삭제 후에도 영구히 남지 않는다. 성공적으로 재처리하거나 DLQ로 이동할 때는
counter를 제거한다.

분산 락을 사용하는 메시지는 lock 갱신과 PEL `XCLAIM`을 하나의
non-transaction pipeline으로 전송한다. 두 lease의 성공 여부는 각각 검사하되
heartbeat당 Redis network round-trip은 한 번만 사용한다. idle consumer metadata
여러 건의 삭제도 같은 방식으로 일괄 전송한다.

## 최소 사용 예시

```python
from redis.asyncio import Redis

from ex_agent.transport import (
    AckDecision,
    HandlerResult,
    RedisStreamConsumer,
    RedisStreamConsumerConfig,
    StreamMessage,
)


class JobHandler:
    def lock_key(self, message: StreamMessage) -> str | None:
        return f"job-lock:{message.fields['job_id']}"

    async def handle(self, message: StreamMessage) -> HandlerResult:
        await run_idempotent_job(message.fields)
        return HandlerResult(AckDecision.ACK)


consumer = RedisStreamConsumer(
    Redis.from_url(redis_url, decode_responses=True),
    RedisStreamConsumerConfig(
        stream="jobs",
        group="analytics-agent-v1",
        consumer_prefix="pod-a-job",
        concurrency=4,
        dead_letter_stream="jobs.dlq",
    ),
    lambda slot_index: JobHandler(),
)
await consumer.run()
```

서비스 종료 시에는 `run()` task를 무조건 취소하는 대신 먼저 drain을 요청한다.

```python
import asyncio

run_task = asyncio.create_task(consumer.run())

# SIGTERM 등 종료 신호를 받은 뒤
await consumer.shutdown(grace_period_seconds=30)
await run_task
```

`request_stop()`은 새 메시지 처리를 중단하고 현재 handler가 끝나기를 기다린다.
`shutdown()`은 같은 요청을 보낸 뒤 grace period 안에 끝나지 않은 slot task를
취소한다. 취소된 메시지는 ACK하지 않고 PEL에 남으며, 보유한 분산 락은
token 검증 후 해제되어 다른 replica가 claim idle 이후 복구할 수 있다. Redis
connection의 소유권은 호출자에게 있으므로 consumer 종료가 connection을 닫지는
않는다. stop 요청은 되돌리지 않는 one-shot lifecycle이다. 같은 설정으로 다시
실행해야 하면 새 consumer 인스턴스를 만든다.

Compose 통합 테스트는 처리 중인 첫 runtime을 grace timeout으로 종료한 뒤 원본
message가 PEL에 남고 lock이 해제되는지 확인한다. 이어 두 번째 runtime이 같은
group에서 `XAUTOCLAIM`으로 message를 가져와 `reclaimed=True`로 한 번 처리하고
ACK하여 pending 0건으로 수렴하는지 검증한다.

현재 contract의 message ID와 fields는 문자열이므로 Redis client는
`decode_responses=True`로 생성한다.

Handler 반환값의 의미는 다음과 같다.

- `ACK`: side effect와 durable state 저장이 끝났으며 원본을 ACK한다.
- `RETRY`: ACK하지 않고 PEL에 남겨 claim idle 이후 다시 처리한다.
- retry 횟수가 `max_retry_attempts`에 도달하면 원래 `RETRY` 사유와 시도 횟수를
  기록하고 `DEAD_LETTER`로 확정한다.
- `PermanentMessageError`가 아닌 handler 예외는 retry 가능한 실패로 취급한다.
- `DEAD_LETTER`: DLQ에 사유와 원본 fields를 저장한 뒤 원본을 ACK한다.
- `PermanentMessageError`: parse/validation 불가능한 envelope에 사용하며
  `DEAD_LETTER`와 동일하게 처리한다.

## Consumer group 규칙

같은 group은 동일한 메시지를 경쟁 소비한다. 따라서 같은 business side effect를
수행하는 replica만 group을 공유해야 한다. 별도 Agent가 같은 Stream 이벤트를
독립적으로 모두 받아야 한다면 반드시 별도 group 이름을 사용한다.

`consumer_prefix`는 배포 instance와 역할을 식별할 수 있어야 한다. 예를 들어
`{pod_name}-command`를 사용하면 실제 consumer 이름은
`{pod_name}-command-0` 형태가 된다. concurrency 축소나 rolling restart로 남은
metadata는 pending 0건과 idle 기준을 모두 만족할 때만 삭제한다.

## DLQ 계약

DLQ entry는 다음 필드를 가진다.

- `schema_version`
- `failure_id`: source stream/group/message ID 기반 안정 식별자
- `dead_lettered_at`
- `source_stream`
- `source_group`
- `source_message_id`
- `consumer`
- `error_type`
- `reason`
- `retry_attempts`: 영구 오류는 `0`, retry 소진은 실제 retry 횟수
- `reclaimed`
- `fields`: 원본 field map의 JSON 문자열

구형 entry에는 `schema_version`, `failure_id` 등 일부 필드가 없을 수 있다. DLQ
reader는 기존 필수 필드와 `fields`가 유효하면 이를 version `0`으로 읽는다.

## DLQ 운영 CLI

`ex-agent-dlq`는 DLQ를 오래된 순서로 조회하고 replay 또는 discard한다.

```bash
ex-agent-dlq --stream agent.commands.dlq list --limit 50

ex-agent-dlq --stream agent.commands.dlq replay 1730000000000-0 \
  --actor operator@example.com \
  --reason "dependency recovered" \
  --yes

ex-agent-dlq --stream executor.events.agent-dlq discard 1730000000001-0 \
  --actor operator@example.com \
  --reason "invalid legacy envelope" \
  --yes
```

replay는 원본 `fields`를 source stream에 새 ID로 발행한다. source 발행, DLQ
entry 삭제, `<dlq-stream>.audit` 기록과 action marker 저장은 하나의 Lua
transaction이다. marker 때문에 응답 유실 후 같은 DLQ entry ID를 다시 replay해도
source에는 한 번만 발행된다. 원본 Stream ID는 재사용하지 않으며 message 안의
business idempotency key는 그대로 유지한다.

discard도 audit 기록, DLQ 삭제와 marker 저장을 원자적으로 수행한다. 두 명령은
`actor`, `reason`, `--yes`가 필수다. marker 기본 TTL은 90일이며
`DLQ_ACTION_MARKER_TTL_SECONDS` 또는 CLI option으로 바꿀 수 있다. batch 명령은
순차 fail-fast지만 완료된 entry는 멱등 marker가 남으므로 같은 전체 명령을
안전하게 다시 실행할 수 있다.

현재 배포는 standalone Redis를 전제로 한다. Redis Cluster로 전환할 때는 source,
DLQ, audit와 marker key가 같은 hash slot을 사용하도록 stream 이름에 동일한 hash
tag를 적용해야 한다.

## 현재 Worker 연결

`WorkflowWorker`의 command와 Executor event loop는 모두 이 런타임을 사용한다.
Agent 고유 로직은 `_CommandHandler`와 `_ExecutorEventHandler`에만 남아 있다.
Executor event가 실행 binding보다 먼저 도착하면 ACK하지 않고 PEL에 유지하며,
stale reclaim 시 Executor history와 함께 복구한다.

설정값:

- `STREAM_CLAIM_BATCH_SIZE`
- `COMMAND_MAX_RETRY_ATTEMPTS`
- `EXECUTOR_EVENT_MAX_RETRY_ATTEMPTS`
- `STREAM_RETRY_STATE_TTL_SECONDS`
- `DLQ_ACTION_MARKER_TTL_SECONDS`
- `CONSUMER_GC_IDLE_MILLISECONDS`
- `AGENT_COMMAND_DEAD_LETTER_STREAM`
- `EXECUTOR_EVENT_DEAD_LETTER_STREAM`
- 기존 Stream/group, claim idle, lock TTL/renewal, concurrency 설정

Stream retention은 producer 및 모든 consumer group의 보존 요구사항을 함께
결정해야 하므로 소비 경로에서 임의로 `MAXLEN`을 적용하지 않는다. 대신
`SafeStreamTrimmer`와 운영 CLI가 별도의 maintenance 경계에서 보존 정책을
적용한다.

## Safe trim 운영

`ex-agent-stream-maintenance`는 다음 네 경계 중 가장 오래된 Stream ID보다
이전인 entry만 삭제한다.

- `STREAM_RETENTION_SECONDS`로 계산한 복구 보존기간
- 모든 consumer group의 `last-delivered-id`
- 모든 consumer group PEL의 가장 오래된 pending ID
- `STREAM_MINIMUM_RETAINED_ENTRIES`로 보장하는 최근 tail

즉 느린 group, 아직 ACK되지 않은 message와 설정된 복구기간 안의 entry는
삭제되지 않는다. `plan`은 변경 없는 운영 점검이며, `trim`은 `--yes`를
요구한다.

```bash
ex-agent-stream-maintenance \
  --stream agent.commands \
  --stream executor.events \
  plan

ex-agent-stream-maintenance \
  --stream agent.commands \
  --stream executor.events \
  trim --yes
```

`plan`과 `trim` 사이의 상태는 달라질 수 있다. `trim`은 plan 결과를 입력으로
사용하지 않고 Lua script 안에서 group, PEL, tail 경계를 다시 계산한 뒤 같은
원자적 연산에서 exact `XTRIM MINID`를 실행한다. 따라서 계산과 삭제 사이에 새
group이 생성되는 race가 없다. 반환되는 `trim_before_id` 자체와 그 이후 ID는
보존되며 그보다 오래된 entry만 삭제된다.

보존기간 계산은 producer가 기본 `XADD *`처럼 Unix millisecond 기반 Stream ID를
사용한다는 전제다. 임의의 과거/미래 ID를 사용하는 Stream에는 이 age 정책을
적용하면 안 된다. `0-0`에서 시작한 신규 group이나 방치된 group은 의도적으로
trim을 차단한다. group 제거는 pending 0건, 해당 소비자의 폐기 여부와 replay
요건을 별도로 확인한 뒤 수행해야 한다.

현재 구현은 standalone Redis를 대상으로 검증했다. 한 번의 trim script는 한
Stream key만 사용하므로 Redis Cluster에서도 key 간 transaction은 필요 없지만,
운영 전 대상 Redis의 Lua/XINFO/XPENDING 정책과 Redis 7.4 호환성을 확인한다.

서비스 운영에서는 CLI 대신
[Stream maintenance API](stream-maintenance-api.md)를 사용할 수 있다. API는
등록된 Stream과 보존정책 하한만 허용하고 실제 trim은 background Worker가 수행한다.
