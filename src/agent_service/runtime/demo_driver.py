"""Explicit simulator. Never calls an LLM, executes code or writes a report."""

import asyncio
import logging
from uuid import UUID, uuid4

from psycopg.types.json import Jsonb
from pydantic import TypeAdapter

from agent_service.application.outputs import OutputWriter
from agent_service.application.runs import RunService
from agent_service.domain.runs import TERMINAL, PlanResponse
from agent_service.infrastructure.database.runs import RunRepository

logger = logging.getLogger("agent_service.demo")
SCHEMA = TypeAdapter(PlanResponse).json_schema()


class DemoDriver:
    def __init__(self, service: RunService):
        self.service = service

    async def tick(self, run_id: UUID, owner: UUID) -> bool:
        """One atomic simulated step; no long-running I/O under DB locks."""
        async with self.service.repository() as repo:
            # Skip concurrent workers without changing the API's lock order.
            claim = await repo.one(
                "SELECT pg_try_advisory_xact_lock(hashtextextended(%s, 0)) "
                "AS acquired",
                (str(run_id),),
            )
            if not claim or not claim["acquired"]:
                return False
            user = await repo.user(owner)
            run = await repo.run(owner, run_id, lock=True)
            if run["backend"] != "demo" or run["status"] in TERMINAL:
                return False
            if run["status"] == "awaiting_input":
                return False
            due = await repo.one(
                "SELECT due_at <= clock_timestamp() AS ready "
                "FROM management.runs WHERE run_id = %s",
                (run_id,),
            )
            if not due or not due["ready"]:
                return False
            output = OutputWriter(repo, run)
            if run["status"] == "cancelling":
                bindings = await repo.executions(run_id)
                # Fail closed: this simulator cannot confirm real cancellation.
                if any(not binding["simulated"] for binding in bindings):
                    logger.error(
                        "demo_refused_real_cancellation run_id=%s", run_id
                    )
                    return False
                await repo.connection.execute(
                    "UPDATE management.run_executions "
                    "SET status = 'cancelled', "
                    "updated_at = clock_timestamp(), updated_by = %s "
                    "WHERE run_id = %s "
                    "AND status NOT IN ('completed', 'failed')",
                    (owner, run_id),
                )
                await output.terminate("cancelled")
            elif user["status"] != "active":
                await output.terminate(
                    "failed",
                    {
                        "code": "USER_DISABLED",
                        "message": "비활성 사용자입니다.",
                    },
                )
            else:
                await self.advance(repo, run, output)
            return True

    async def advance(
        self, repo: RunRepository, run: dict, output: OutputWriter
    ) -> None:
        cp = run["checkpoint"]
        phase = cp["phase"]
        if phase == "start":
            await repo.set_status(run, "running")
            cp["message_id"] = str(uuid4())
            await output.start(UUID(cp["message_id"]), [])
            cp["phase"] = "answer_first"
        elif phase == "answer_first":
            await output.append(
                UUID(cp["message_id"]),
                "[DEMO] API 흐름 검증용 응답입니다. ",
                "demo:first",
            )
            cp["phase"] = "answer_last"
        elif phase == "answer_last":
            if cp["scenario"] == "failure":
                await output.terminate(
                    "failed",
                    {
                        "code": "DEMO_FAILURE",
                        "message": "의도한 테스트 실패입니다.",
                    },
                )
                return
            await output.append(
                UUID(cp["message_id"]),
                "LLM·실제 코드 실행·분석 리포트는 연결되지 않았습니다.",
                "demo:last",
            )
            await output.finish(UUID(cp["message_id"]))
            if cp["scenario"] == "reply":
                await output.terminate("completed")
                return
            await self.review(repo, run)
            return
        elif phase == "review_response":
            response = cp["response"]
            if response["decision"] == "reject":
                await output.terminate("rejected")
                return
            if response["decision"] == "modify":
                cp["revision"] += 1
                await self.review(repo, run)
                return
            # Durable intent and lock BEFORE the simulated submission.
            await repo.lock_session(run)
            cp["binding_id"] = str(uuid4())
            await repo.connection.execute(
                """
                INSERT INTO management.run_executions
                    (binding_id, run_id, idempotency_key, status, simulated,
                     created_by, updated_by)
                VALUES (%s, %s, %s, 'submitting', true, %s, %s)
                """,
                (
                    cp["binding_id"],
                    run["run_id"],
                    f"demo:{run['run_id']}",
                    run["owner_user_uuid"],
                    run["owner_user_uuid"],
                ),
            )
            await repo.set_status(run, "running")
            cp["phase"] = "submitted"
        elif phase == "submitted":
            # This UUID is explicitly marked simulated, not an Executor ID.
            cp["execution_id"] = str(uuid4())
            await repo.connection.execute(
                "UPDATE management.run_executions SET execution_id = %s, "
                "status = 'running', updated_at = clock_timestamp(), "
                "updated_by = %s WHERE binding_id = %s AND simulated",
                (cp["execution_id"], run["owner_user_uuid"], cp["binding_id"]),
            )
            await repo.set_status(run, "waiting_execution")
            await repo.emit(
                run,
                "run.progress",
                {
                    "stage": "execution",
                    "simulated": True,
                    "message": "[DEMO] 외부 실행 대기 상태를 모사합니다.",
                },
            )
            cp["phase"] = "execution_finished"
        elif phase == "execution_finished":
            await repo.connection.execute(
                "UPDATE management.run_executions SET status = 'completed', "
                "updated_at = clock_timestamp(), updated_by = %s "
                "WHERE binding_id = %s AND simulated",
                (run["owner_user_uuid"], cp["binding_id"]),
            )
            await repo.set_status(run, "running")
            await repo.emit(
                run,
                "run.progress",
                {
                    "stage": "report",
                    "simulated": True,
                    "message": (
                        "[DEMO] 결과 정리 모사입니다. 실제 리포트는 없습니다."
                    ),
                },
            )
            cp["phase"] = "report"
        elif phase == "report":
            message_id = uuid4()
            await output.start(message_id, [])
            await output.append(
                message_id,
                "[DEMO] 승인 → 실행 대기 → 결과 정리 흐름을 검증했습니다. "
                "실제 데이터·노트북·분석 리포트는 생성하지 않았습니다.",
                "demo:report",
            )
            await output.finish(message_id)
            await output.terminate("completed")
            return
        else:
            await output.terminate(
                "failed",
                {
                    "code": "INVALID_DEMO_CHECKPOINT",
                    "message": "테스트 실행 단계가 유효하지 않습니다.",
                },
            )
            return
        await repo.checkpoint(run, cp, self.service.settings.demo_step_seconds)

    async def review(self, repo: RunRepository, run: dict) -> None:
        cp = run["checkpoint"]
        payload = {
            "simulated": True,
            "title": "[DEMO] 실행계획 승인 흐름 테스트",
            "plan_id": str(uuid4()),
            "plan_version": cp["revision"],
            "steps": ["실행 대기 모사", "결과 정리 모사"],
            "instruction": cp.get("response", {}).get("instruction"),
            "response_schema": SCHEMA,
        }
        row = await repo.one(
            """
            INSERT INTO management.run_interrupts
                (interrupt_id, run_id, response_type, schema_version,
                 payload, created_by, updated_by)
            VALUES (%s, %s, 'plan_review', 1, %s, %s, %s) RETURNING *
            """,
            (
                uuid4(),
                run["run_id"],
                Jsonb(payload),
                run["owner_user_uuid"],
                run["owner_user_uuid"],
            ),
        )
        assert row is not None
        cp["phase"] = "waiting_review"
        await repo.checkpoint(run, cp, 0)
        await repo.set_status(run, "awaiting_input")
        await repo.emit(
            run,
            "run.interrupted",
            {
                "interrupt_id": str(row["interrupt_id"]),
                "response_type": "plan_review",
                "schema_version": 1,
                "payload": payload,
            },
        )

    async def serve(self) -> None:
        logger.info("demo_worker_started backend=demo")
        try:
            while True:
                try:
                    async with self.service.repository() as repo:
                        jobs = await repo.all(
                            "SELECT run_id, owner_user_uuid "
                            "FROM management.runs WHERE backend = 'demo' "
                            "AND status IN ('queued', 'running', "
                            "'waiting_execution', 'cancelling') "
                            "AND due_at <= clock_timestamp() "
                            "ORDER BY due_at LIMIT 20",
                            (),
                        )
                    for job in jobs:
                        await self.tick(job["run_id"], job["owner_user_uuid"])
                except Exception as error:
                    # Do not expose submitted messages or DB credentials.
                    logger.warning(
                        "demo_tick_failed type=%s", type(error).__name__
                    )
                await asyncio.sleep(self.service.settings.demo_step_seconds)
        finally:
            logger.info("demo_worker_stopped")
