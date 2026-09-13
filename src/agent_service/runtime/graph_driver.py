"""Durable conversation runner, independent of HTTP connection lifetime."""

import asyncio
import logging
from contextlib import aclosing, asynccontextmanager
from time import monotonic
from uuid import UUID, uuid4

from langchain_core.messages import AIMessageChunk, HumanMessage
from psycopg import OperationalError
from psycopg_pool import PoolTimeout

from agent_service.application.outputs import OutputWriter
from agent_service.domain.runs import TERMINAL
from agent_service.graphs.assistant.builder import GRAPH_VERSION, build_graph
from agent_service.graphs.assistant.nodes import answer_id, public_text

logger = logging.getLogger("agent_service.graph")


class Superseded(Exception):
    """This attempt may no longer mutate the run."""


class CancelRequested(Exception):
    """Cancel only after the graph task and checkpoint writes have stopped."""


class GraphDriver:
    def __init__(self, service, checkpoints, agent):
        self.service = service
        self.settings = service.settings
        self.checkpoints = checkpoints
        self.agent = agent

    @asynccontextmanager
    async def owned(self, run_id, owner, attempt):
        async with self.service.repository() as repo:
            user = await repo.user(owner)
            run = await repo.run(owner, run_id, lock=True)
            if run["checkpoint"].get("attempt_id") != attempt:
                raise Superseded
            if run["status"] == "cancelling":
                raise CancelRequested
            if run["status"] in TERMINAL:
                raise Superseded
            if user["status"] != "active":
                raise PermissionError("User disabled")
            yield OutputWriter(repo, run)

    async def finish(self, run_id, owner, attempt, status, error=None):
        async with self.service.repository() as repo:
            run = await repo.run(owner, run_id, lock=True)
            if (
                run["checkpoint"].get("attempt_id") != attempt
                or run["status"] in TERMINAL
            ):
                return
            if run["status"] == "cancelling":
                status, error = "cancelled", None
            await OutputWriter(repo, run).terminate(status, error)

    async def run_one(self, run_id, owner):
        async with self.service.repository() as repo:
            target = await repo.run(owner, run_id)
        # One bounded pool slot per active graph. No long SQL transaction.
        async with self.checkpoints.session(target["session_id"]) as saver:
            if saver is None:
                return
            attempt = str(uuid4())
            async with self.service.repository() as repo:
                run = await repo.run(owner, run_id, lock=True)
                if run["backend"] != "langgraph" or run["status"] in TERMINAL:
                    return
                state = run["checkpoint"]
                state["attempt_id"] = attempt
                state["attempts"] = state.get("attempts", 0) + 1
                await repo.checkpoint(
                    run, state, self.settings.worker_reschedule_seconds
                )
                if run["status"] == "cancelling":
                    await OutputWriter(repo, run).terminate("cancelled")
                    return
                if state["attempts"] > self.settings.recovery_max_attempts:
                    await OutputWriter(repo, run).terminate(
                        "failed",
                        {
                            "code": "RECOVERY_EXHAUSTED",
                            "message": "재시작 복구 횟수를 초과했습니다.",
                        },
                    )
                    return
                await repo.set_status(run, "running")
            logger.info(
                "graph_run_started run_id=%s attempt=%s",
                run_id,
                state["attempts"],
            )
            try:
                if state.get("graph_version") != GRAPH_VERSION:
                    raise ValueError("Unsupported graph version")
                async with asyncio.timeout(self.settings.run_timeout_seconds):
                    await self.watch(run, attempt, saver)
            except (OperationalError, PoolTimeout) as error:
                # The next worker re-reads the durable checkpoint. It does
                # not assume that an ambiguous write failed to commit.
                logger.warning(
                    "graph_retry run_id=%s type=%s",
                    run_id,
                    type(error).__name__,
                )
            except Superseded:
                return
            except CancelRequested:
                await self.finish(run_id, owner, attempt, "cancelled")
            except Exception as error:
                logger.warning(
                    "graph_failed run_id=%s type=%s",
                    run_id,
                    type(error).__name__,
                )
                await self.finish(
                    run_id,
                    owner,
                    attempt,
                    "failed",
                    {
                        "code": "GRAPH_TIMEOUT"
                        if isinstance(error, TimeoutError)
                        else "GRAPH_EXECUTION_FAILED",
                        "message": (
                            "대화 처리 제한 시간을 초과했습니다."
                            if isinstance(error, TimeoutError)
                            else "대화 처리에 실패했습니다. 모델 연결/응답과 "
                            "그래프 설정을 확인해 주세요."
                        ),
                    },
                )

    async def watch(self, run, attempt, saver):
        task = asyncio.create_task(self.execute(run, attempt, saver))
        renewed = monotonic()
        try:
            while True:
                done, _ = await asyncio.wait(
                    {task}, timeout=self.settings.worker_poll_seconds
                )
                if done:
                    return await task
                async with self.owned(
                    run["run_id"], run["owner_user_uuid"], attempt
                ) as output:
                    delay = self.settings.worker_reschedule_seconds
                    if monotonic() - renewed >= delay / 3:
                        # Scheduling hint only; session advisory lock is the
                        # execution fence. Other workers can scan past us.
                        await output.repo.connection.execute(
                            "UPDATE management.runs SET due_at = "
                            "clock_timestamp() + %s * interval '1 second' "
                            "WHERE run_id = %s",
                            (delay, run["run_id"]),
                        )
                        renewed = monotonic()
        finally:
            if not task.done():
                task.cancel()
            await asyncio.gather(task, return_exceptions=True)

    async def execute(self, run, attempt, saver):
        run_id, owner = run["run_id"], run["owner_user_uuid"]
        graph = build_graph(
            self.agent, saver, self.settings.context_message_limit
        )
        config = {
            "configurable": {"thread_id": str(run["session_id"])},
            "recursion_limit": 20,
        }
        snapshot = await graph.aget_state(config)
        values = snapshot.values
        same_run = values.get("run_id") == str(run_id)
        completed = same_run and values.get("completed_run_id") == str(run_id)
        message_id = UUID(answer_id(str(run_id)))
        async with self.owned(run_id, owner, attempt) as output:
            await output.start(message_id, [])
            if not completed:
                await output.replace(message_id, "")
        if not completed:
            if (
                run["checkpoint"].get("model_name") != self.settings.model_name
                or run["checkpoint"].get("model_provider")
                != self.settings.model_provider
            ):
                raise ValueError("Run model configuration changed")
            graph_input = (
                None
                if same_run
                else {
                    "run_id": str(run_id),
                    "answer": "",
                    "completed_run_id": None,
                    "messages": [
                        HumanMessage(
                            id=f"{run_id}:user",
                            content="\n\n".join(
                                b["text"] for b in run["input"]["content"]
                            ),
                        )
                    ],
                }
            )
            pending, size, sequence = [], 0, 0
            flushed = monotonic()
            total = 0
            stream = graph.astream(
                graph_input,
                config,
                stream_mode="messages",
                subgraphs=True,
                durability="sync",
            )
            async with aclosing(stream):
                async for _namespace, (message, metadata) in stream:
                    if (
                        not isinstance(message, AIMessageChunk)
                        or metadata.get("lc_agent_name")
                        != "conversation_assistant"
                    ):
                        continue
                    text = public_text(message)
                    pending.append(text)
                    size += len(text)
                    total += len(text)
                    if total > self.settings.output_max_chars:
                        raise ValueError("Output size limit exceeded")
                    if size and (
                        size >= self.settings.output_flush_chars
                        or monotonic() - flushed
                        >= self.settings.output_flush_seconds
                    ):
                        async with self.owned(run_id, owner, attempt) as out:
                            await out.append(
                                message_id,
                                "".join(pending),
                                f"{attempt}:chunk:{sequence}",
                            )
                        sequence += 1
                        pending, size, flushed = [], 0, monotonic()
            snapshot = await graph.aget_state(config)
            values = snapshot.values
        if values.get("completed_run_id") != str(run_id) or snapshot.next:
            raise RuntimeError("Graph did not finish this run")
        answer = values["answer"]
        if len(answer) > self.settings.output_max_chars:
            raise ValueError("Output size limit exceeded")
        async with self.owned(run_id, owner, attempt) as output:
            # Reconcile from durable state, not a potentially partial stream.
            await output.replace(message_id, answer, complete=True)
            await output.terminate("completed")
        logger.info("graph_run_completed run_id=%s", run_id)

    async def serve(self):
        jobs = {}
        logger.info(
            "graph_worker_started concurrency=%s",
            self.settings.worker_concurrency,
        )
        try:
            while True:
                for run_id, task in list(jobs.items()):
                    if task.done():
                        error = task.exception()
                        if error:
                            logger.warning(
                                "graph_job_error run_id=%s type=%s",
                                run_id,
                                type(error).__name__,
                            )
                        del jobs[run_id]
                capacity = self.settings.worker_concurrency - len(jobs)
                if capacity:
                    try:
                        async with self.service.repository() as repo:
                            candidates = await repo.all(
                                "SELECT run_id, owner_user_uuid FROM "
                                "management.runs WHERE backend = 'langgraph' "
                                "AND status IN ('queued','running', "
                                "'cancelling') AND due_at <= now() "
                                "ORDER BY due_at LIMIT %s",
                                (capacity + self.settings.worker_concurrency,),
                            )
                        for job in candidates:
                            run_id = job["run_id"]
                            if run_id not in jobs and capacity:
                                jobs[run_id] = asyncio.create_task(
                                    self.run_one(
                                        run_id, job["owner_user_uuid"]
                                    )
                                )
                                capacity -= 1
                    except Exception as error:
                        logger.warning(
                            "graph_poll_failed type=%s", type(error).__name__
                        )
                await asyncio.sleep(self.settings.worker_poll_seconds)
        finally:
            for task in jobs.values():
                task.cancel()
            await asyncio.gather(*jobs.values(), return_exceptions=True)
            logger.info("graph_worker_stopped")
