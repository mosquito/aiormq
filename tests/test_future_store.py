import asyncio

import pytest

from aiormq.base import FutureStore


@pytest.mark.asyncio
async def test_simple(event_loop):
    root_store = FutureStore(loop=event_loop)
    future1 = root_store.create_future()

    child_store = root_store.get_child()
    future2 = child_store.create_future()

    assert root_store.futures
    assert child_store.futures

    await root_store.reject_all(RuntimeError)
    await asyncio.sleep(0.1, loop=event_loop)

    assert isinstance(future1.exception(), RuntimeError)
    assert isinstance(future2.exception(), RuntimeError)
    assert not root_store.futures
    assert not child_store.futures

    async def result():
        await asyncio.sleep(0.1, loop=event_loop)
        return 'result'

    assert await child_store.create_task(result()) == 'result'

    async def coro(store):
        await asyncio.sleep(0.1, loop=event_loop)
        await store.reject_all(RuntimeError)

    task1 = child_store.create_task(coro(child_store))
    assert root_store.futures
    assert child_store.futures

    with pytest.raises(RuntimeError):
        await task1

    await asyncio.sleep(0.1, loop=event_loop)

    assert not root_store.futures
    assert not child_store.futures

    child = child_store.get_child().get_child().get_child()
    task = child.create_task(coro(child))

    assert root_store.futures
    assert child_store.futures
    assert child.futures

    with pytest.raises(RuntimeError):
        await task

    await asyncio.sleep(0.1, loop=event_loop)

    assert not root_store.futures
    assert not child_store.futures
    assert not child.futures
