"""Central router registry used by standalone and Gaia applications."""

from fastapi import APIRouter

from . import messages, models, projects, runs, sessions, users


def get_management_routers() -> list[APIRouter]:
    """Append these routers to the host get_routers() exactly once."""
    return [
        users.router,
        projects.router,
        sessions.router,
        models.router,
        runs.router,
        messages.router,
    ]
