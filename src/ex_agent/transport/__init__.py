"""HTTP and Redis transport adapters."""

from ex_agent.transport.consumer import (
    AckDecision,
    ConsumerObserver,
    HandlerResult,
    NullConsumerObserver,
    PermanentMessageError,
    RedisStreamConsumer,
    RedisStreamConsumerConfig,
    StreamLeaseLostError,
    StreamMessage,
    StreamMessageHandler,
)
from ex_agent.transport.dlq import (
    DeadLetterAction,
    DeadLetterActionResult,
    DeadLetterEntry,
    DeadLetterFormatError,
    DeadLetterManager,
    DeadLetterPage,
)
from ex_agent.transport.stream_maintenance import (
    ConsumerGroupBoundary,
    SafeStreamTrimmer,
    StreamTrimPlan,
    StreamTrimResult,
)

__all__ = [
    "AckDecision",
    "ConsumerGroupBoundary",
    "ConsumerObserver",
    "DeadLetterAction",
    "DeadLetterActionResult",
    "DeadLetterEntry",
    "DeadLetterFormatError",
    "DeadLetterManager",
    "DeadLetterPage",
    "HandlerResult",
    "NullConsumerObserver",
    "PermanentMessageError",
    "RedisStreamConsumer",
    "RedisStreamConsumerConfig",
    "SafeStreamTrimmer",
    "StreamLeaseLostError",
    "StreamMessage",
    "StreamMessageHandler",
    "StreamTrimPlan",
    "StreamTrimResult",
]
