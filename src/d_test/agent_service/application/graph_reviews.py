"""Idempotent projection of a durable graph interrupt into the public API."""

from uuid import uuid5

from psycopg.types.json import Jsonb


async def publish_review(output, interruption):
    repo, run = output.repo, output.run
    public_id = uuid5(run["run_id"], f"graph-review:{interruption.id}")
    payload = interruption.value
    row = await repo.one(
        "INSERT INTO management.run_interrupts "
        "(interrupt_id, run_id, graph_interrupt_id, response_type, "
        "schema_version, payload, created_by, updated_by) "
        "VALUES (%s, %s, %s, 'plan_review', 1, %s, %s, %s) "
        "ON CONFLICT (interrupt_id) DO NOTHING RETURNING interrupt_id",
        (
            public_id,
            run["run_id"],
            interruption.id,
            Jsonb(payload),
            run["owner_user_uuid"],
            run["owner_user_uuid"],
        ),
    )
    if row is None:
        existing = await repo.one(
            "SELECT payload, status FROM management.run_interrupts "
            "WHERE interrupt_id = %s AND run_id = %s",
            (public_id, run["run_id"]),
        )
        if (
            not existing
            or existing["payload"] != payload
            or existing["status"] != "pending"
        ):
            raise ValueError("Graph interrupt projection mismatch")
    await repo.set_status(run, "awaiting_input")
    await repo.emit(
        run,
        "run.interrupted",
        {
            "interrupt_id": str(public_id),
            "response_type": "plan_review",
            "schema_version": 1,
            "payload": payload,
        },
        f"graph-review:{interruption.id}",
    )
