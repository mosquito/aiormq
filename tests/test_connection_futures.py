import asyncio
import gc
import weakref

import pytest

from aiormq.exceptions import ConnectionClosed


async def test_connection_close_during_publish_drain(
    amqp_connection, monkeypatch, event_loop,
):
    channel = await amqp_connection.channel()
    entered = asyncio.Event()
    contexts = []
    previous = event_loop.get_exception_handler()
    event_loop.set_exception_handler(
        lambda loop, context: contexts.append(context),
    )

    class Queue:
        async def put(self, frame):
            # Hold the frame before the writer drains it. Connection shutdown
            # rejects both futures while publish is only awaiting the drain.
            entered.set()

    try:
        with monkeypatch.context() as patch:
            patch.setattr(channel, "write_queue", Queue())
            task = asyncio.create_task(channel.basic_publish(b"test"))
            await asyncio.wait_for(entered.wait(), 2)
            confirmation = weakref.ref(channel.confirmations[1])
            error = ConnectionClosed(320, "CONNECTION_FORCED")
            await amqp_connection.close(error)
            with pytest.raises(ConnectionClosed):
                await task
            # Closing futures retain the shared exception. Drop its traceback
            # so the publish frame does not keep the confirmation alive.
            error.__traceback__ = None
            del task, error
        channel.confirmations.clear()
        await asyncio.sleep(0)
        gc.collect()
        assert not contexts
        assert confirmation() is None
    finally:
        event_loop.set_exception_handler(previous)
