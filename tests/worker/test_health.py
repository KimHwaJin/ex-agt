import asyncio

import httpx
import pytest


@pytest.mark.postgres
@pytest.mark.redis
async def test_health_readiness_and_metrics(worker):
    server = await asyncio.start_server(worker._health, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    run = None
    try:
        async with httpx.AsyncClient(
            base_url=f"http://127.0.0.1:{port}"
        ) as http:
            assert (await http.get("/health/live")).status_code == 200
            assert (await http.get("/health/ready")).status_code == 503
            run = asyncio.create_task(worker.run())
            for _ in range(100):
                if (await http.get("/health/ready")).status_code == 200:
                    break
                await asyncio.sleep(0.02)
            assert (await http.get("/health/ready")).status_code == 200
            assert "ew_operations" in (await http.get("/metrics")).text
            worker.request_stop()
            assert (await http.get("/health/ready")).status_code == 503
    finally:
        worker.request_stop()
        if run is not None:
            await run
        server.close()
        await server.wait_closed()
