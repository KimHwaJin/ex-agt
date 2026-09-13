"""Management use cases with durable idempotency and ownership checks."""

import hashlib
import json
from uuid import UUID

from agent_service.application.cursors import CursorCodec
from agent_service.domain.management import (
    DomainError,
    Home,
    Page,
    Project,
    Session,
    User,
)
from agent_service.infrastructure.database.management import Repository, Store


def fingerprint(payload: dict) -> str:
    serialized = json.dumps(
        payload, sort_keys=True, separators=(",", ":"), default=str
    )
    return hashlib.sha256(serialized.encode()).hexdigest()


def active_user(row: dict) -> User:
    user = User.model_validate(row)
    if user.status != "active":
        raise DomainError("USER_DISABLED", "비활성화된 사용자입니다.", 403)
    return user


class ManagementService:
    def __init__(self, store: Store, cursors: CursorCodec):
        self.store = store
        self.cursors = cursors

    async def me(self, user_id: str) -> Home:
        async with self.store.transaction() as repository:
            user = active_user(await repository.provision_user(user_id))
            project = await repository.default_project(user.user_uuid)
            return Home(
                user=user, default_project=Project.model_validate(project)
            )

    async def get_me(self, user_uuid: UUID) -> Home:
        async with self.store.transaction() as repository:
            user = await self.checked(repository, user_uuid)
            project = await repository.get_default_project(user_uuid)
            return Home(
                user=user, default_project=Project.model_validate(project)
            )

    async def user(self, user_uuid: UUID) -> User:
        async with self.store.transaction() as repository:
            return active_user(await repository.user(user_uuid))

    async def checked(self, repository: Repository, owner: UUID) -> User:
        return active_user(await repository.user(owner))

    async def create_project(
        self, owner: UUID, key: str, name: str, description: str | None
    ) -> Project:
        async with self.store.transaction() as repository:
            await self.checked(repository, owner)
            digest = fingerprint({"name": name, "description": description})
            prior = await repository.reserve(
                owner, "projects.create", key, digest
            )
            if prior is not None:
                # Do not resurrect a resource after deletion on replay.
                await repository.project(owner, UUID(prior["project_id"]))
                return Project.model_validate(prior)
            item_id = await repository.insert_project(owner, name, description)
            result = Project.model_validate(
                await repository.project(owner, item_id)
            )
            await repository.finish(
                owner, "projects.create", key, result.model_dump(mode="json")
            )
            return result

    async def projects(
        self, owner: UUID, limit: int, cursor: str | None
    ) -> Page[Project]:
        scope = f"projects:{owner}"
        boundary = self.cursors.decode(scope, cursor)
        async with self.store.transaction() as repository:
            await self.checked(repository, owner)
            rows = await repository.projects(owner, limit + 1, boundary)
        items = [Project.model_validate(row) for row in rows[:limit]]
        more = len(rows) > limit
        next_cursor = None
        if more:
            last = items[-1]
            next_cursor = self.cursors.encode(
                scope, last.created_at, last.project_id
            )
        return Page(items=items, next_cursor=next_cursor, has_more=more)

    async def project(self, owner: UUID, project_id: UUID) -> Project:
        async with self.store.transaction() as repository:
            await self.checked(repository, owner)
            return Project.model_validate(
                await repository.project(owner, project_id)
            )

    async def update_project(
        self, owner: UUID, project_id: UUID, version: int, changes: dict
    ) -> Project:
        async with self.store.transaction() as repository:
            await self.checked(repository, owner)
            current = await repository.project(owner, project_id, lock=True)
            await repository.update_project(
                owner,
                project_id,
                version,
                changes.get("name", current["name"]),
                changes.get("description", current["description"]),
            )
            return Project.model_validate(
                await repository.project(owner, project_id)
            )

    async def delete_project(self, owner: UUID, project_id: UUID) -> None:
        async with self.store.transaction() as repository:
            await self.checked(repository, owner)
            current = await repository.project(
                owner, project_id, deleted=True, lock=True
            )
            if current["is_default"]:
                raise DomainError(
                    "DEFAULT_PROJECT_DELETE_FORBIDDEN",
                    "기본 프로젝트는 삭제할 수 없습니다.",
                )
            if current["deleted_at"] is None:
                await repository.delete_project(owner, project_id)

    async def create_session(
        self, owner: UUID, key: str, project_id: UUID, title: str
    ) -> Session:
        async with self.store.transaction() as repository:
            await self.checked(repository, owner)
            digest = fingerprint({"project_id": project_id, "title": title})
            prior = await repository.reserve(
                owner, "sessions.create", key, digest
            )
            if prior is not None:
                await repository.session(owner, UUID(prior["session_id"]))
                return Session.model_validate(prior)
            await repository.project(owner, project_id)
            item_id = await repository.insert_session(owner, project_id, title)
            result = Session.model_validate(
                await repository.session(owner, item_id)
            )
            await repository.finish(
                owner, "sessions.create", key, result.model_dump(mode="json")
            )
            return result

    async def sessions(
        self, owner: UUID, project_id: UUID, limit: int, cursor: str | None
    ) -> Page[Session]:
        scope = f"sessions:{owner}:{project_id}"
        boundary = self.cursors.decode(scope, cursor)
        async with self.store.transaction() as repository:
            await self.checked(repository, owner)
            rows = await repository.sessions(
                owner, project_id, limit + 1, boundary
            )
        items = [Session.model_validate(row) for row in rows[:limit]]
        more = len(rows) > limit
        next_cursor = None
        if more:
            last = items[-1]
            next_cursor = self.cursors.encode(
                scope, last.created_at, last.session_id
            )
        return Page(items=items, next_cursor=next_cursor, has_more=more)

    async def session(self, owner: UUID, session_id: UUID) -> Session:
        async with self.store.transaction() as repository:
            await self.checked(repository, owner)
            return Session.model_validate(
                await repository.session(owner, session_id)
            )

    async def update_session(
        self, owner: UUID, session_id: UUID, version: int, title: str
    ) -> Session:
        async with self.store.transaction() as repository:
            await self.checked(repository, owner)
            await repository.session(owner, session_id, lock=True)
            await repository.update_session(owner, session_id, version, title)
            return Session.model_validate(
                await repository.session(owner, session_id)
            )

    async def delete_session(self, owner: UUID, session_id: UUID) -> None:
        async with self.store.transaction() as repository:
            await self.checked(repository, owner)
            current = await repository.session(
                owner, session_id, deleted=True, lock=True
            )
            if current["deleted_at"] is None:
                await repository.delete_session(owner, session_id)
