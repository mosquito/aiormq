import asyncio

import pytest


@pytest.mark.parametrize("method", ["basic_ack", "basic_nack", "basic_reject"])
@pytest.mark.parametrize("failure", ["cancel", "error"])
@pytest.mark.parametrize("wait", [False, True])
async def test_disposition_enqueue_failure(
    amqp_connection, monkeypatch, method, failure, wait,
):
    channel = await amqp_connection.channel()
    entered = asyncio.Event()
    frames = []

    class Queue:
        async def put(self, frame):
            frames.append(frame)
            entered.set()
            if failure == "error":
                raise RuntimeError("enqueue failed")
            await asyncio.Event().wait()

    with monkeypatch.context() as patch:
        patch.setattr(channel, "write_queue", Queue())
        task = asyncio.create_task(getattr(channel, method)(1, wait=wait))
        await asyncio.wait_for(entered.wait(), 2)
        if failure == "cancel":
            task.cancel()
        error = asyncio.CancelledError if failure == "cancel" else RuntimeError
        with pytest.raises(error):
            await task

    drain = frames[0].drain_future
    if wait:
        assert drain is not None and drain.cancelled()
        await asyncio.sleep(0)
        assert drain not in channel._Base__future_store.futures
        assert drain not in amqp_connection._Base__future_store.futures
    else:
        assert drain is None
    await channel.close()


@pytest.mark.parametrize("method", ["basic_ack", "basic_nack", "basic_reject"])
async def test_disposition_without_wait(amqp_connection, monkeypatch, method):
    channel = await amqp_connection.channel()
    frames = []

    class Queue:
        async def put(self, frame):
            frames.append(frame)

    with monkeypatch.context() as patch:
        patch.setattr(channel, "write_queue", Queue())
        await asyncio.wait_for(getattr(channel, method)(1, wait=False), 2)
    assert frames[0].drain_future is None
    await channel.close()
