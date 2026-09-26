import asyncio

import pytest

from aiormq.tools import Countdown, awaitable


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
async def test_awaitable(func, result):
    assert await awaitable(func)() == result


async def test_countdown():
    countdown = Countdown(timeout=0.1)
    await countdown(asyncio.sleep(0))

    # waiting for the countdown exceeded
    await asyncio.sleep(0.2)

    task = asyncio.create_task(asyncio.sleep(0))

    with pytest.raises(asyncio.TimeoutError):
        await countdown(task)

    assert task.cancelled()


async def test_countdown_context_releases_lock_after_timeout_inside():
    countdown = Countdown(timeout=0.05)
    lock = asyncio.Lock()

    with pytest.raises(asyncio.TimeoutError):
        async with countdown.enter_context(lock):
            assert lock.locked()
            # The deadline expires here, like a slow write_queue.put()
            await countdown(asyncio.sleep(1))

    assert not lock.locked()


async def test_countdown_context_releases_lock_after_deadline():
    countdown = Countdown(timeout=0.05)
    lock = asyncio.Lock()

    async with countdown.enter_context(lock):
        assert lock.locked()
        # The body completes, but the deadline is already in the past
        await asyncio.sleep(0.1)

    assert not lock.locked()


async def test_countdown_context_does_not_enter_after_deadline():
    countdown = Countdown(timeout=0.05)
    lock = asyncio.Lock()

    await asyncio.sleep(0.1)

    with pytest.raises(asyncio.TimeoutError):
        async with countdown.enter_context(lock):
            pytest.fail("The context body must not run")

    assert not lock.locked()
