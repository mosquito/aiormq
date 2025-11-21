import asyncio

import pytest

from aiormq.abc import TaskWrapper


async def _busy_work():
    # simple coroutine that waits long enough to be cancelled by tests
    await asyncio.sleep(1)


async def test_throw_with_instance(event_loop):
    task = event_loop.create_task(_busy_work())
    wrapped = TaskWrapper(task)

    wrapped.throw(RuntimeError("boom"))

    # original task should be cancelled and raise CancelledError when awaited
    with pytest.raises(asyncio.CancelledError) as excinfo:
        await task
    print("\ntest_throw_with_instance 1", repr(excinfo))
    assert "boom" in str(excinfo.value)

    # wrapper should re-raise provided exception
    with pytest.raises(RuntimeError) as excinfo:
        await wrapped
    print("\ntest_throw_with_instance 2", repr(excinfo))
    assert "boom" in str(excinfo.value)


async def test_throw_with_type(event_loop):
    task = event_loop.create_task(_busy_work())
    wrapped = TaskWrapper(task)

    # pass exception class instead of instance
    wrapped.throw(RuntimeError)

    with pytest.raises(asyncio.CancelledError) as excinfo:
        await task
    print("\ntest_throw_with_type", repr(excinfo))
    


async def test_throw_with_cancellederror(event_loop):
    task = event_loop.create_task(_busy_work())
    wrapped = TaskWrapper(task)

    # cancel with CancelledError instance
    wrapped.throw(asyncio.CancelledError())

    # original task raises CancelledError
    with pytest.raises(asyncio.CancelledError) as excinfo:
        await task
    print("\ntest_throw_with_cancellederror 1", repr(excinfo))

    # wrapper should raise CancelledError as well
    with pytest.raises(asyncio.CancelledError) as excinfo:
        await wrapped

    print("\ntest_throw_with_cancellederror 2", repr(excinfo))

    
