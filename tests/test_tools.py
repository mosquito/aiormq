import asyncio

import pytest

from aiormq.tools import LazyCoroutine, awaitable


class TestLazyCoroutine:
    async def test_coro(self, loop):
        async def foo():
            await asyncio.sleep(0)
            return 42

        bar = LazyCoroutine(foo)

        assert await bar == 42

    async def test_future(self, loop):
        def foo():
            f = loop.create_future()
            loop.call_soon(f.set_result, 42)
            return f

        bar = LazyCoroutine(foo)

        assert await bar == 42

    async def test_task(self, loop):
        def foo():
            async def inner():
                await asyncio.sleep(0)
                return 42

            return loop.create_task(inner())

        bar = LazyCoroutine(foo)

        assert await bar == 42


def simple_func():
    return 1


def await_result_func():
    return asyncio.sleep(0)


async def await_func():
    await asyncio.sleep(0)
    return 2


def return_future():
    loop = asyncio.get_event_loop()
    f = loop.create_future()
    loop.call_soon(f.set_result, 5)
    return f


async def await_future():
    return (await return_future()) + 1


def return_coroutine():
    return await_future()


AWAITABLE_FUNCS = [
    (simple_func, 1),
    (await_result_func, None),
    (await_func, 2),
    (return_future, 5),
    (await_future, 6),
    (return_coroutine, 6),
]


@pytest.mark.parametrize("func,result", AWAITABLE_FUNCS)
async def test_awaitable(func, result, loop):
    assert await awaitable(func)() == result
