from agent.effects.store import EffectStore
from agent.failure.executor import FailureExecutor
from agent.failure.service import FailureService
from agent.failure.store import FailureStore


def cleanup(h, **kwargs):
    sender = h.service.execution.sender
    return FailureService(
        h.graph,
        h.guard,
        FailureStore(h.sessions),
        h.store,
        FailureExecutor(EffectStore(h.sessions), sender, sender.executor),
        retry_seconds=0.01,
        **kwargs,
    )
