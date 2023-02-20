import asyncio

import pytest

from aiormq.tools import Countdown


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


async def test_countdown(event_loop):
    countdown = Countdown(timeout=0.1)
    await countdown(asyncio.sleep(0))

    # waiting for the countdown exceeded
    await asyncio.sleep(0.2)

    task = asyncio.create_task(asyncio.sleep(0))

    with pytest.raises(asyncio.TimeoutError):
        await countdown(task)

    assert task.cancelled()
