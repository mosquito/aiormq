import asyncio

import pytest

from aiormq.base import FutureStore


@pytest.fixture
def root_store(event_loop):
    store = FutureStore(loop=event_loop)
    try:
        yield store
    finally:
        event_loop.run_until_complete(
            store.reject_all(Exception("Cancelling"))
        )


@pytest.fixture
def child_store(event_loop, root_store):
    store = root_store.get_child()
    try:
        yield store
    finally:
        event_loop.run_until_complete(
            store.reject_all(Exception("Cancelling"))
        )


@pytest.mark.asyncio
async def test_reject_all(event_loop, root_store: FutureStore,
                          child_store: FutureStore):

    future1 = root_store.create_future()
    future2 = child_store.create_future()

    assert root_store.futures
    assert child_store.futures

    await root_store.reject_all(RuntimeError)
    await asyncio.sleep(0.1, loop=event_loop)

    assert isinstance(future1.exception(), RuntimeError)
    assert isinstance(future2.exception(), RuntimeError)
    assert not root_store.futures
    assert not child_store.futures


@pytest.mark.asyncio
async def test_result(event_loop, root_store: FutureStore,
                      child_store: FutureStore):

    async def result():
        await asyncio.sleep(0.1, loop=event_loop)
        return 'result'

    assert await child_store.create_task(result()) == 'result'


@pytest.mark.asyncio
async def test_siblings(event_loop, root_store: FutureStore,
                        child_store: FutureStore):

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
