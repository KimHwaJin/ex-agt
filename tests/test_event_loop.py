"""Portable Windows-branch checks, not a native Windows integration test."""

import asyncio
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from fastapi import FastAPI

from d_test.agent_service import migrate, worker_main
from d_test.agent_service.bootstrap import event_loop
from d_test.api_service import server


@pytest.fixture
def windows_runner(monkeypatch):
    platform = SimpleNamespace(platform="win32")
    for module in (event_loop, server, worker_main):
        monkeypatch.setattr(module, "sys", platform)


def test_windows_runner_selects_and_closes_loop(windows_runner):
    async def current_loop():
        return asyncio.get_running_loop()

    loop = event_loop.run_async(current_loop())
    assert isinstance(loop, asyncio.SelectorEventLoop)
    assert loop.is_closed()


def test_windows_runner_cleans_up_on_failure(windows_runner):
    loops = []

    async def fail():
        loops.append(asyncio.get_running_loop())
        raise ValueError("test failure")

    with pytest.raises(ValueError, match="test failure"):
        event_loop.run_async(fail())
    assert loops[0].is_closed()


def test_linux_runner_preserves_asyncio_run(monkeypatch):
    monkeypatch.setattr(event_loop, "sys", SimpleNamespace(platform="linux"))
    run = Mock(wraps=asyncio.run)
    monkeypatch.setattr(asyncio, "run", run)

    async def value():
        return 42

    assert event_loop.run_async(value()) == 42
    run.assert_called_once()


def test_windows_server_lifespan_uses_selector(windows_runner, monkeypatch):
    calls = []

    @asynccontextmanager
    async def lifespan(app):
        assert isinstance(
            asyncio.get_running_loop(), asyncio.SelectorEventLoop
        )
        calls.append("start")
        try:
            yield
        finally:
            calls.append("stop")

    app = FastAPI(lifespan=lifespan)

    class Server:
        def __init__(self, config):
            assert config.app is app
            assert config.port == 5000
            assert config.log_config is None
            assert config.proxy_headers is False

        async def serve(self):
            async with app.router.lifespan_context(app):
                calls.append("serve")

    monkeypatch.setattr(server.uvicorn, "Server", Server)
    server.run_server(app, port=5000, log_config=None, proxy_headers=False)
    assert calls == ["start", "serve", "stop"]


@pytest.mark.parametrize("options", [{"reload": True}, {"workers": 2}])
def test_windows_server_rejects_subprocess_modes(windows_runner, options):
    with pytest.raises(ValueError, match="one worker without reload"):
        server.run_server(FastAPI(), **options)


def test_linux_server_keeps_existing_uvicorn_path(monkeypatch):
    monkeypatch.setattr(server, "sys", SimpleNamespace(platform="linux"))
    run = Mock()
    monkeypatch.setattr(server.uvicorn, "run", run)
    app = FastAPI()
    server.run_server(app, port=8020, proxy_headers=False)
    run.assert_called_once_with(app, port=8020, proxy_headers=False)


def test_migrate_checkpoint_runner(windows_runner, monkeypatch):
    calls = []

    def upgrade(config, revision):
        assert revision == "head"
        calls.append("alembic")

    async def setup(settings):
        assert isinstance(
            asyncio.get_running_loop(), asyncio.SelectorEventLoop
        )
        calls.append("checkpoints")

    monkeypatch.setattr(migrate.command, "upgrade", upgrade)
    monkeypatch.setattr(migrate, "setup_checkpoints", setup)
    migrate.main()
    assert calls == ["alembic", "checkpoints"]


def test_windows_worker_skips_unavailable_signals(
    windows_runner, monkeypatch, settings
):
    calls = []

    async def start():
        calls.append("start")

    async def serve():
        calls.append("serve")
        raise asyncio.CancelledError

    async def close():
        calls.append("close")

    runtime = SimpleNamespace(
        start=start, close=close, driver=SimpleNamespace(serve=serve)
    )
    monkeypatch.setattr(
        worker_main.ManagementRuntime, "build", lambda _: runtime
    )
    monkeypatch.setattr(worker_main, "initialize_logging", lambda _: None)

    def unsupported(*args):
        raise AssertionError("Windows cannot use add_signal_handler")

    monkeypatch.setattr(
        asyncio.SelectorEventLoop, "add_signal_handler", unsupported
    )
    settings = settings.model_copy(update={"agent_backend": "demo"})
    event_loop.run_async(worker_main.main(settings))
    assert calls == ["start", "serve", "close"]
