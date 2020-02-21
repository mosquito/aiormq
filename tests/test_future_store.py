import asyncio

import pytest

from aiormq.base import FutureStore, TaskPriority


@pytest.fixture
def root_store(event_loop):
    store = FutureStore(loop=event_loop)
    try:
        yield store
    finally:
        event_loop.run_until_complete(
            store.reject(Exception("Cancelling"), TaskPriority.HIGH)
        )


@pytest.fixture
def child_store(event_loop, root_store):
    store = root_store.get_child()
    try:
        yield store
    finally:
        event_loop.run_until_complete(
            store.reject(Exception("Cancelling"), TaskPriority.HIGH)
        )


@pytest.mark.asyncio
async def test_reject(
    event_loop, root_store: FutureStore, child_store: FutureStore
):

    future1 = root_store.create_future(TaskPriority.LOW)
    future2 = child_store.create_future(TaskPriority.HIGH)

    assert root_store.future_sets[TaskPriority.LOW]
    assert child_store.future_sets[TaskPriority.HIGH]

    await root_store.reject(RuntimeError, TaskPriority.HIGH)
    await asyncio.sleep(0.1)

    assert isinstance(future1.exception(), RuntimeError)
    assert isinstance(future2.exception(), RuntimeError)
    assert not root_store.future_sets[TaskPriority.LOW]
    assert not child_store.future_sets[TaskPriority.HIGH]


@pytest.mark.asyncio
async def test_result(
    event_loop, root_store: FutureStore, child_store: FutureStore
):
    async def result():
        await asyncio.sleep(0.1)
        return "result"

    assert await child_store.create_task(result()) == "result"


@pytest.mark.asyncio
async def test_siblings(
    event_loop, root_store: FutureStore, child_store: FutureStore
):
    async def coro(store):
        await asyncio.sleep(0.1)
        await store.reject(RuntimeError, TaskPriority.LOW)

    task1 = child_store.create_task(coro(child_store), TaskPriority.LOW)
    assert root_store.future_sets[TaskPriority.LOW]
    assert child_store.future_sets[TaskPriority.LOW]

    with pytest.raises(RuntimeError):
        await task1

    await asyncio.sleep(0.1)

    assert not root_store.future_sets[TaskPriority.LOW]
    assert not child_store.future_sets[TaskPriority.LOW]

    child = child_store.get_child().get_child().get_child()
    task = child.create_task(coro(child))

    assert root_store.future_sets[TaskPriority.LOW]
    assert child_store.future_sets[TaskPriority.LOW]
    assert child.future_sets[TaskPriority.LOW]

    with pytest.raises(RuntimeError):
        await task

    await asyncio.sleep(0.1)

    assert not root_store.future_sets[TaskPriority.LOW]
    assert not child_store.future_sets[TaskPriority.LOW]
    assert not child.future_sets[TaskPriority.LOW]
