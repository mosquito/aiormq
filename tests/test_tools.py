import asyncio

import pytest

from aiormq.tools import LazyCoroutine


@pytest.mark.asyncio
async def test_lazy_coroutine_coro(event_loop):
    async def foo():
        await asyncio.sleep(0, loop=event_loop)
        return 42

    bar = LazyCoroutine(foo)

    assert await bar == 42


@pytest.mark.asyncio
async def test_lazy_coroutine_future(event_loop):
    def foo():
        f = event_loop.create_future()
        event_loop.call_soon(f.set_result, 42)
        return f

    bar = LazyCoroutine(foo)

    assert await bar == 42


@pytest.mark.asyncio
async def test_lazy_coroutine_task(event_loop):
    def foo():
        async def inner():
            await asyncio.sleep(0, loop=event_loop)
            return 42

        return event_loop.create_task(inner())

    bar = LazyCoroutine(foo)

    assert await bar == 42
