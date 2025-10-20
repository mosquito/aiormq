import asyncio

import pytest

from aiormq.abc import TaskWrapper
from aiormq.base import FutureStore


@pytest.fixture
def root_store(event_loop):
    store = FutureStore(loop=event_loop)
    try:
        yield store
    finally:
        event_loop.run_until_complete(
            store.reject_all(Exception("Cancelling")),
        )


@pytest.fixture
def child_store(event_loop, root_store):
    store = root_store.get_child()
    try:
        yield store
    finally:
        event_loop.run_until_complete(
            store.reject_all(Exception("Cancelling")),
        )


async def test_reject_all(root_store: FutureStore, child_store: FutureStore):

    future1 = root_store.create_future()
    future2 = child_store.create_future()

    assert root_store.futures
    assert child_store.futures

    await root_store.reject_all(RuntimeError)
    await asyncio.sleep(0.1)

    assert isinstance(future1.exception(), RuntimeError)
    assert isinstance(future2.exception(), RuntimeError)
    assert not root_store.futures
    assert not child_store.futures


async def test_result(root_store: FutureStore, child_store: FutureStore):
    async def result():
        await asyncio.sleep(0.1)
        return "result"

    assert await child_store.create_task(result()) == "result"


async def test_siblings(root_store: FutureStore, child_store: FutureStore):
    async def coro(store):
        await asyncio.sleep(0.1)
        await store.reject_all(RuntimeError)

    task1 = child_store.create_task(coro(child_store))
    assert root_store.futures
    assert child_store.futures

    with pytest.raises(RuntimeError):
        await task1

    await asyncio.sleep(0.1)

    assert not root_store.futures
    assert not child_store.futures

    child = child_store.get_child().get_child().get_child()
    task = child.create_task(coro(child))

    assert root_store.futures
    assert child_store.futures
    assert child.futures

    with pytest.raises(RuntimeError):
        await task

    await asyncio.sleep(0.1)

    assert not root_store.futures
    assert not child_store.futures
    assert not child.futures


async def test_task_wrapper(event_loop):
    future = event_loop.create_future()
    wrapped = TaskWrapper(future)

    wrapped.throw(RuntimeError())

    with pytest.raises(asyncio.CancelledError):
        await future

    with pytest.raises(RuntimeError):
        await wrapped
