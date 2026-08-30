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
- 영구적인 envelope 오류는 DLQ 기록과 원본 ACK를 하나의 Redis transaction으로
  수행
- pending이 없고 충분히 오래 idle한 consumer metadata 정리
- transport 오류에 대한 bounded exponential backoff
- metrics/log adapter를 위한 observer contract

Business side effect는 중복 실행될 수 있으므로 Handler는 idempotent해야 한다.
이 프로젝트의 command/event Handler는 PostgreSQL idempotency와 event sequence
검증을 함께 사용한다.

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

현재 contract의 message ID와 fields는 문자열이므로 Redis client는
`decode_responses=True`로 생성한다.

Handler 반환값의 의미는 다음과 같다.

- `ACK`: side effect와 durable state 저장이 끝났으며 원본을 ACK한다.
- `RETRY`: ACK하지 않고 PEL에 남겨 claim idle 이후 다시 처리한다.
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

- `source_stream`
- `source_group`
- `source_message_id`
- `reason`
- `fields`: 원본 field map의 JSON 문자열

재처리 도구는 `fields`를 decode하여 원본 Stream에 새 entry로 발행해야 한다.
원본 ID를 재사용하지 않으므로 business idempotency key를 그대로 유지해야 한다.

## 현재 Worker 연결

`WorkflowWorker`의 command와 Executor event loop는 모두 이 런타임을 사용한다.
Agent 고유 로직은 `_CommandHandler`와 `_ExecutorEventHandler`에만 남아 있다.
Executor event가 실행 binding보다 먼저 도착하면 ACK하지 않고 PEL에 유지하며,
stale reclaim 시 Executor history와 함께 복구한다.

설정값:

- `STREAM_CLAIM_BATCH_SIZE`
- `CONSUMER_GC_IDLE_MILLISECONDS`
- `AGENT_COMMAND_DEAD_LETTER_STREAM`
- `EXECUTOR_EVENT_DEAD_LETTER_STREAM`
- 기존 Stream/group, claim idle, lock TTL/renewal, concurrency 설정

Stream retention은 producer 및 모든 consumer group의 보존 요구사항을 함께
결정해야 하므로 이 소비기 모듈이 임의로 `MAXLEN`을 적용하지 않는다.
