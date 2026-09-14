"""Persist plans atomically with approval projection, not while calling LLM."""

import json

from psycopg.types.json import Jsonb

from agent_service.catalog.registry import digest


async def save_plan_snapshot(output, bundle, public_plan):
    if not bundle or bundle["plan"] != public_plan:
        raise ValueError("Plan checkpoint does not match approval payload")
    plan = bundle["plan"]
    canonical = {
        **bundle,
        "plan": {
            key: value for key, value in plan.items() if key != "plan_sha256"
        },
    }
    fingerprint = digest(
        json.dumps(
            canonical,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )
    )
    if plan["plan_sha256"] != fingerprint:
        raise ValueError("Plan snapshot fingerprint mismatch")
    repo, run = output.repo, output.run
    row = await repo.one(
        "INSERT INTO management.execution_plans "
        "(run_id, plan_version, plan_id, plan_sha256, snapshot, "
        "created_by, updated_by) VALUES (%s,%s,%s,%s,%s,%s,%s) "
        "ON CONFLICT (run_id, plan_version) DO NOTHING RETURNING plan_id",
        (
            run["run_id"],
            plan["plan_version"],
            plan["plan_id"],
            fingerprint,
            Jsonb(bundle),
            run["owner_user_uuid"],
            run["owner_user_uuid"],
        ),
    )
    if row is None:
        existing = await repo.one(
            "SELECT snapshot FROM management.execution_plans "
            "WHERE run_id = %s AND plan_version = %s",
            (run["run_id"], plan["plan_version"]),
        )
        if not existing or existing["snapshot"] != bundle:
            raise ValueError("A plan version cannot be overwritten")
