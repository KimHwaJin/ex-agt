"""Translate service settings without relying on duplicate environment keys."""

from ex_agent.config import Settings as AgentSettings
from worker.config import Settings as WorkerSettings


def build_worker_settings(settings: AgentSettings) -> WorkerSettings:
    """Build the reusable Worker configuration from the Agent contract."""

    ingress = settings.worker_executor_event_concurrency
    dispatch = settings.worker_command_concurrency
    result = WorkerSettings(
        database_url=settings.agent_checkpoint_database_url,
        redis_url=settings.agent_redis_url,
        namespace=settings.executor_worker_namespace,
        executor_base_url=settings.executor_base_url,
        executor_event_stream=settings.executor_event_stream,
        command_stream_name=settings.agent_command_stream,
        event_group_name=settings.executor_event_consumer_group,
        command_group_name=settings.agent_command_consumer_group,
        concurrency=max(ingress, dispatch),
        ingress_concurrency=ingress,
        dispatch_concurrency=dispatch,
        pool_size=max(
            2,
            settings.checkpoint_pool_max_size,
            ingress,
            dispatch,
        ),
        batch_size=settings.stream_claim_batch_size,
        poll_seconds=settings.outbox_poll_milliseconds / 1000,
        idle_poll_seconds=settings.outbox_idle_max_milliseconds / 1000,
        claim_idle_milliseconds=min(
            settings.command_claim_idle_milliseconds,
            settings.executor_event_claim_idle_milliseconds,
        ),
        lease_ttl_seconds=settings.task_lock_ttl_seconds,
        lease_renew_seconds=settings.task_lock_renew_interval_seconds,
        publish_lease_seconds=settings.outbox_claim_timeout_seconds,
        max_handler_attempts=settings.executor_event_max_retry_attempts,
        shutdown_seconds=settings.worker_shutdown_grace_seconds,
        request_timeout_seconds=settings.executor_request_timeout_seconds,
        health_port=(
            settings.worker_metrics_port
            if settings.worker_metrics_enabled
            else 0
        ),
    )
    if settings.worker_instance_id:
        return result.model_copy(
            update={"instance_id": settings.worker_instance_id}
        )
    return result
