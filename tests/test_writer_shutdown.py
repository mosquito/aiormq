import asyncio

import pytest


@pytest.mark.parametrize("concurrent_close", [False, True])
async def test_close_waits_for_writer_cleanup(
    amqp_connection, monkeypatch, concurrent_close,
):
    connection = amqp_connection
    cleanup_started = asyncio.Event()
    release_cleanup = asyncio.Event()
    reader_handled = asyncio.Event()
    cleanup_finished = False
    cleanup_cancelled = False
    close_writer = connection._Connection__close_writer
    reader_done = connection._on_reader_done

    async def delayed_close(writer):
        nonlocal cleanup_finished, cleanup_cancelled
        cleanup_started.set()
        try:
            await release_cleanup.wait()
            await close_writer(writer)
            cleanup_finished = True
        except asyncio.CancelledError:
            cleanup_cancelled = True
            raise

    def on_reader_done(task):
        reader_done(task)
        # Run after the reader callback's close_writer_task has started.
        connection.loop.call_soon(reader_handled.set)

    # connect() already registered the original bound callback.
    connection._reader_task.remove_done_callback(reader_done)
    connection._reader_task.add_done_callback(on_reader_done)
    monkeypatch.setattr(connection, "_Connection__close_writer", delayed_close)
    closers = [asyncio.create_task(connection.close())]
    try:
        await asyncio.wait_for(cleanup_started.wait(), 5)
        await asyncio.wait_for(reader_handled.wait(), 5)
        if concurrent_close:
            second_started = asyncio.Event()
            on_close = connection._on_close

            async def second_on_close(exc):
                connection.loop.call_soon(second_started.set)
                await on_close(exc)

            monkeypatch.setattr(connection, "_on_close", second_on_close)
            closers.append(asyncio.create_task(connection.close()))
            await asyncio.wait_for(second_started.wait(), 5)

        assert not cleanup_cancelled
        assert not cleanup_finished
        assert all(not closer.done() for closer in closers)
    finally:
        release_cleanup.set()
        await asyncio.wait_for(asyncio.gather(*closers), 5)

    assert cleanup_finished
    assert not cleanup_cancelled
    assert connection._writer_task.done()
