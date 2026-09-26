import asyncio
import uuid

import aiomisc
import pytest

from aiormq.exceptions import ChannelAccessRefused


@pytest.mark.parametrize("separate_publisher", [False, True])
@pytest.mark.parametrize("wait", [False, True])
@pytest.mark.parametrize("timeout", [None, 2])
@aiomisc.timeout(15)
async def test_consumer_can_catch_publish_error(
    amqp_connection, separate_publisher, wait, timeout,
):
    setup = await amqp_connection.channel()
    queue = await setup.queue_declare(exclusive=True)
    exchange = "internal-" + uuid.uuid4().hex
    await setup.exchange_declare(exchange, internal=True)
    consumer = await amqp_connection.channel()
    publisher = (
        await amqp_connection.channel() if separate_publisher else consumer
    )
    result = asyncio.get_running_loop().create_future()

    async def callback(message):
        try:
            await publisher.basic_publish(
                b"response", exchange=exchange, wait=wait, timeout=timeout,
            )
        except Exception as exc:
            # This is the user's exception handler, inside the consumer
            # task, rather than an observer awaiting a TaskWrapper.
            if separate_publisher:
                await consumer.basic_ack(message.delivery_tag)
            result.set_result((exc, asyncio.current_task().cancelling()))
        except BaseException as exc:
            result.set_result((exc, asyncio.current_task().cancelling()))
        else:
            result.set_result((None, asyncio.current_task().cancelling()))

    try:
        await consumer.basic_consume(queue.queue, callback)
        await setup.basic_publish(b"request", routing_key=queue.queue)
        exc, cancelling = await result
        assert isinstance(exc, ChannelAccessRefused)
        assert cancelling == 0
        if not separate_publisher:
            assert consumer.is_closed
            # The failed callback must not silently acknowledge the input.
            message = await setup.basic_get(queue.queue, no_ack=True)
            assert message.body == b"request"
            assert message.delivery.redelivered
        else:
            assert not consumer.is_closed
    finally:
        await setup.exchange_delete(exchange)


@pytest.mark.parametrize("close_connection", [False, True])
@aiomisc.timeout(15)
async def test_publish_shutdown_remains_cancelled(
    amqp_connection, monkeypatch, close_connection,
):
    channel = await amqp_connection.channel()
    entered = asyncio.Event()
    result = asyncio.get_running_loop().create_future()

    queue = await channel.queue_declare(exclusive=True)

    def delay_confirm(delivery_tag, frame):
        # Let the writer finish and release the publish lock, then keep
        # the callback waiting for confirmation when shutdown starts.
        entered.set()

    async def callback():
        try:
            await channel.basic_publish(b"sent", routing_key=queue.queue)
        except BaseException as exc:
            result.set_result(exc)

    with monkeypatch.context() as patch:
        patch.setattr(channel, "_confirm_delivery", delay_confirm)
        channel.create_task(callback())
        await entered.wait()

    if close_connection:
        await amqp_connection.close()
    else:
        await channel.close()
    assert isinstance(await result, asyncio.CancelledError)


@pytest.mark.parametrize("external", ["before", "after", "only"])
@aiomisc.timeout(15)
async def test_publish_preserves_external_cancellation(
    amqp_connection, monkeypatch, external,
):
    channel = await amqp_connection.channel()
    entered = asyncio.Event()
    result = asyncio.get_running_loop().create_future()
    reason = ChannelAccessRefused("broker refusal")

    class BlockingQueue:
        async def put(self, item):
            entered.set()
            await asyncio.Event().wait()

    async def callback():
        try:
            await channel.basic_publish(b"not sent")
        except BaseException as exc:
            result.set_result(exc)

    with monkeypatch.context() as patch:
        patch.setattr(channel, "write_queue", BlockingQueue())
        # Keep this task outside FutureStore to control the two competing
        # cancellation requests without also closing the real transport.
        task = asyncio.create_task(callback())
        await entered.wait()
        if external != "only":
            channel._close_exception = reason
            channel.closing.set_exception(reason)
        if external == "before":
            task.cancel("user cancellation")
            task.cancel(reason)
        elif external == "after":
            task.cancel(reason)
            task.cancel("user cancellation")
        else:
            # An exception passed by the user is not a channel close.
            task.cancel(reason)
        await task
    assert isinstance(await result, asyncio.CancelledError)
