import asyncio

import pytest

from aiormq.tools import LazyCoroutine


@pytest.mark.asyncio
class TestLazyCoroutine:
    async def test_coro(self, event_loop):
        async def foo():
            await asyncio.sleep(0, loop=event_loop)
            return 42

        bar = LazyCoroutine(foo)

        assert await bar == 42

    async def test_future(self, event_loop):
        def foo():
            f = event_loop.create_future()
            event_loop.call_soon(f.set_result, 42)
            return f

        bar = LazyCoroutine(foo)

        assert await bar == 42

    async def test_task(self, event_loop):
        def foo():
            async def inner():
                await asyncio.sleep(0, loop=event_loop)
                return 42

            return event_loop.create_task(inner())

        bar = LazyCoroutine(foo)

        assert await bar == 42
