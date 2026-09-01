# 운영 Agent Runtime

`agent.runtime`은 API+Agent와 Background Worker가 동일한 업무 그래프와 복구
정책을 조립하기 위한 공통 경계다. LangGraph `thread_id`는 항상 `session_id`이며,
API와 Worker는 같은 Agent DB, checkpoint DB, Redis guard namespace를 사용한다.

`open_agent_runtime()`은 그래프, API 접수 서비스, Executor 이벤트 handler,
RequestRecovery와 FailureRecovery를 만든다. 테이블이나 checkpoint schema를 만들지
않으며 배포 migration Job이 먼저 완료되지 않으면 소비 시작 전에 실패한다.

Worker 호스트는 `AgentRuntime.run_worker(worker)`를 실행한다. API 호스트는 같은
factory로 만든 뒤 `recovery_lifespan()`을 API lifespan에 넣는다. 종료 시 두 루프를
grace 안에 join한 뒤 factory context를 닫는다. 복구 루프가 예기치 않게 종료되면
같은 supervision 단위의 Worker도 종료된다.

```python
async with open_agent_runtime(settings, bridge, saver) as runtime:
    async with recovery_lifespan(
        runtime.lifecycle,
        shutdown_timeout_seconds=settings.worker_shutdown_grace_seconds,
    ):
        yield runtime.admission
```

앱 시작에서 `alembic upgrade`나 `checkpointer.setup()`을 호출하면 안 된다. API가
사용자 인증·Task 소유권을 검증한 `ApiRequest`만 `runtime.admission`에 전달해야
하며 raw graph State나 Executor 이벤트를 공개 HTTP 입력으로 받지 않는다.
