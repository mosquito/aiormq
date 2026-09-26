import asyncio

import aiomisc
import pytest

from aiormq.exceptions import ChannelInvalidStateError


@pytest.mark.parametrize("method", ["basic_ack", "basic_nack", "basic_reject"])
@pytest.mark.parametrize("wait", [False, True])
@pytest.mark.parametrize("cause", ["task-cancel", "channel-close"])
@aiomisc.timeout(20)
async def test_ack_from_cancelled_consumer(
    proxy_connection, method, wait, cause,
):
    connection = proxy_connection
    setup = await connection.channel()
    consumer = await connection.channel()
    queue = await setup.queue_declare(exclusive=True)
    entered = asyncio.get_running_loop().create_future()
    result = asyncio.get_running_loop().create_future()

    async def callback(message):
        entered.set_result(asyncio.current_task())
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            try:
                await getattr(message.channel, method)(
                    message.delivery_tag, wait=wait,
                )
            except BaseException as exc:
                result.set_result(exc)
            else:
                result.set_result(None)

    tag = await consumer.basic_consume(queue.queue, callback)
    await setup.basic_publish(b"pending", routing_key=queue.queue)
    callback_task = await entered
    await consumer.basic_cancel(tag.consumer_tag)
    if cause == "channel-close":
        await consumer.close()
    else:
        callback_task.cancel()
    error = await result
    # An ack after Channel.CloseOk must not make RabbitMQ close the connection.
    await setup.basic_qos(prefetch_count=1)
    assert not connection.is_closed
    if cause == "channel-close":
        assert isinstance(error, ChannelInvalidStateError)
    else:
        assert error is None
        await consumer.basic_qos(prefetch_count=1)
    message = await setup.basic_get(queue.queue, no_ack=True)
    if cause == "task-cancel" and method == "basic_ack":
        assert message.delivery_tag is None
    else:
        assert message.body == b"pending"
        assert message.delivery.redelivered
