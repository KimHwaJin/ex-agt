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

__all__ = [
    "AckDecision",
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
    "StreamLeaseLostError",
    "StreamMessage",
    "StreamMessageHandler",
]
