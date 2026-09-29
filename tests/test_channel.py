import asyncio
import uuid
from os import urandom

import aiomisc
import pytest
from aiomisc_pytest import TCPProxy

import aiormq
from aiormq.abc import DeliveredMessage


async def test_simple(amqp_channel: aiormq.Channel):
    await amqp_channel.basic_qos(prefetch_count=1)
    assert amqp_channel.number

    queue = asyncio.Queue()

    deaclare_ok = await amqp_channel.queue_declare(auto_delete=True)
    consume_ok = await amqp_channel.basic_consume(deaclare_ok.queue, queue.put)
    await amqp_channel.basic_publish(
        b"foo",
        routing_key=deaclare_ok.queue,
        properties=aiormq.spec.Basic.Properties(message_id="123"),
    )

    message: DeliveredMessage = await queue.get()
    assert message.body == b"foo"

    cancel_ok = await amqp_channel.basic_cancel(consume_ok.consumer_tag)
    assert cancel_ok.consumer_tag == consume_ok.consumer_tag
    assert cancel_ok.consumer_tag not in amqp_channel.consumers
    await amqp_channel.queue_delete(deaclare_ok.queue)

    deaclare_ok = await amqp_channel.queue_declare(auto_delete=True)
    await amqp_channel.basic_publish(b"foo bar", routing_key=deaclare_ok.queue)

    message = await amqp_channel.basic_get(deaclare_ok.queue, no_ack=True)
    assert message.body == b"foo bar"


async def test_simple_nowait(amqp_channel: aiormq.Channel):
    if amqp_channel.publisher_confirms:
        pytest.skip("nowait makes no sense with publisher_confirms")
    await amqp_channel.basic_qos(prefetch_count=1)
    assert amqp_channel.number

    queue = asyncio.Queue()

    deaclare_ok = await amqp_channel.queue_declare(auto_delete=True)
    consume_ok = await amqp_channel.basic_consume(deaclare_ok.queue, queue.put)
    amqp_channel.basic_publish_nowait(
        b"foo",
        routing_key=deaclare_ok.queue,
        properties=aiormq.spec.Basic.Properties(message_id="123"),
    )

    message: DeliveredMessage = await queue.get()
    assert message.body == b"foo"

    cancel_ok = await amqp_channel.basic_cancel(consume_ok.consumer_tag)
    assert cancel_ok.consumer_tag == consume_ok.consumer_tag
    assert cancel_ok.consumer_tag not in amqp_channel.consumers
    await amqp_channel.queue_delete(deaclare_ok.queue)

    deaclare_ok = await amqp_channel.queue_declare(auto_delete=True)
    amqp_channel.basic_publish_nowait(b"foo bar", routing_key=deaclare_ok.queue)

    message = await amqp_channel.basic_get(deaclare_ok.queue, no_ack=True)
    assert message.body == b"foo bar"


async def test_blank_body(amqp_channel: aiormq.Channel):
    await amqp_channel.basic_qos(prefetch_count=1)
    assert amqp_channel.number

    queue = asyncio.Queue()

    deaclare_ok = await amqp_channel.queue_declare(auto_delete=True)
    consume_ok = await amqp_channel.basic_consume(deaclare_ok.queue, queue.put)
    await amqp_channel.basic_publish(
        b"",
        routing_key=deaclare_ok.queue,
        properties=aiormq.spec.Basic.Properties(message_id="123"),
    )

    message: DeliveredMessage = await queue.get()
    assert message.body == b""

    cancel_ok = await amqp_channel.basic_cancel(consume_ok.consumer_tag)
    assert cancel_ok.consumer_tag == consume_ok.consumer_tag
    assert cancel_ok.consumer_tag not in amqp_channel.consumers
    await amqp_channel.queue_delete(deaclare_ok.queue)

    deaclare_ok = await amqp_channel.queue_declare(auto_delete=True)
    await amqp_channel.basic_publish(b"foo bar", routing_key=deaclare_ok.queue)

    message = await amqp_channel.basic_get(deaclare_ok.queue, no_ack=True)
    assert message.body == b"foo bar"


async def test_bad_consumer(amqp_channel: aiormq.Channel, event_loop):
    channel: aiormq.Channel = amqp_channel
    await channel.basic_qos(prefetch_count=1)

    declare_ok = await channel.queue_declare()

    future = event_loop.create_future()

    await channel.basic_publish(b"urgent", routing_key=declare_ok.queue)

    consumer_tag = event_loop.create_future()

    async def bad_consumer(message):
        await channel.basic_cancel(await consumer_tag)
        future.set_result(message)
        raise Exception

    consume_ok = await channel.basic_consume(
        declare_ok.queue, bad_consumer, no_ack=False,
    )

    consumer_tag.set_result(consume_ok.consumer_tag)

    message = await future
    await channel.basic_reject(message.delivery.delivery_tag, requeue=True)
    assert message.body == b"urgent"

    future = event_loop.create_future()

    await channel.basic_consume(
        declare_ok.queue, future.set_result, no_ack=True,
    )

    message = await future

    assert message.body == b"urgent"
    assert message.delivery_tag is not None
    assert message.exchange is not None
    assert message.redelivered


async def test_ack_nack_reject(amqp_channel: aiormq.Channel):
    channel: aiormq.Channel = amqp_channel
    await channel.basic_qos(prefetch_count=1)

    declare_ok = await channel.queue_declare(auto_delete=True)
    queue = asyncio.Queue()

    await channel.basic_consume(declare_ok.queue, queue.put, no_ack=False)

    await channel.basic_publish(b"rejected", routing_key=declare_ok.queue)
    message: DeliveredMessage = await queue.get()
    assert message.body == b"rejected"
    assert message.delivery_tag is not None
    assert message.exchange is not None
    assert not message.redelivered
    await channel.basic_reject(message.delivery.delivery_tag, requeue=False)

    await channel.basic_publish(b"nacked", routing_key=declare_ok.queue)
    message = await queue.get()
    assert message.body == b"nacked"
    await channel.basic_nack(message.delivery.delivery_tag, requeue=False)

    await channel.basic_publish(b"acked", routing_key=declare_ok.queue)
    message = await queue.get()
    assert message.body == b"acked"
    await channel.basic_ack(message.delivery.delivery_tag)


async def test_confirm_multiple(amqp_channel: aiormq.Channel):
    """
    RabbitMQ has been observed to send confirmations in a strange pattern
    when publishing simultaneously where only some messages are delivered
    to a queue. It sends acks like this 1 2 4 5(multiple, confirming also 3).
    This test is probably inconsequential without publisher_confirms
    This is a regression for https://github.com/mosquito/aiormq/issues/10
    """
    channel: aiormq.Channel = amqp_channel
    exchange = uuid.uuid4().hex
    await channel.exchange_declare(exchange, exchange_type="topic")
    try:
        declare_ok = await channel.queue_declare(exclusive=True)
        await channel.queue_bind(
            declare_ok.queue, exchange, routing_key="test.5",
        )

        for i in range(10):
            messages = [
                asyncio.ensure_future(
                    channel.basic_publish(
                        b"test", exchange=exchange,
                        routing_key="test.{}".format(i),
                    ),
                )
                for i in range(10)
            ]
            _, pending = await asyncio.wait(messages, timeout=0.2)
            assert not pending, "not all publishes were completed (confirmed)"
            await asyncio.sleep(0.05)
    finally:
        await channel.exchange_delete(exchange)


async def test_exclusive_queue_locked(amqp_connection):
    channel0 = await amqp_connection.channel()
    channel1 = await amqp_connection.channel()

    qname = str(uuid.uuid4())

    await channel0.queue_declare(qname, exclusive=True)

    try:
        await channel0.basic_consume(qname, print, exclusive=True)

        with pytest.raises(aiormq.exceptions.ChannelLockedResource):
            await channel1.queue_declare(qname)
            await channel1.basic_consume(qname, print, exclusive=True)
    finally:
        await channel0.queue_delete(qname)


async def test_remove_writer_when_closed(amqp_channel: aiormq.Channel):
    with pytest.raises(aiormq.exceptions.ChannelClosed):
        await amqp_channel.queue_declare(
            "amq.forbidden_queue_name", auto_delete=True,
        )

    with pytest.raises(aiormq.exceptions.ChannelInvalidStateError):
        await amqp_channel.queue_delete("amq.forbidden_queue_name")


async def test_proxy_connection(proxy_connection, proxy: TCPProxy):
    channel: aiormq.Channel = await proxy_connection.channel()
    await channel.queue_declare(auto_delete=True)


async def test_declare_queue_timeout(proxy_connection, proxy: TCPProxy):
    for _ in range(3):
        channel: aiormq.Channel = await proxy_connection.channel()

        qname = str(uuid.uuid4())

        with proxy.slowdown(read_delay=5, write_delay=0):
            with pytest.raises(asyncio.TimeoutError):
                await channel.queue_declare(
                    qname, auto_delete=True, timeout=0.5,
                )


async def test_big_message(amqp_channel: aiormq.Channel):
    size = 20 * 1024 * 1024
    message = urandom(size)
    await amqp_channel.basic_publish(message)


async def test_routing_key_too_large(amqp_channel: aiormq.Channel):
    routing_key = "x" * 256

    with pytest.raises(ValueError):
        await amqp_channel.basic_publish(b"foo bar", routing_key=routing_key)

    exchange = uuid.uuid4().hex
    await amqp_channel.exchange_declare(exchange, exchange_type="topic")

    with pytest.raises(ValueError):
        await amqp_channel.exchange_bind(exchange, exchange, routing_key)

    with pytest.raises(ValueError):
        await amqp_channel.exchange_unbind(exchange, exchange, routing_key)

    queue = await amqp_channel.queue_declare(exclusive=True)

    with pytest.raises(ValueError):
        await amqp_channel.queue_bind(queue.queue, exchange, routing_key)

    with pytest.raises(ValueError):
        await amqp_channel.queue_unbind(queue.queue, exchange, routing_key)

    await amqp_channel.exchange_delete(exchange)


@aiomisc.timeout(20)
async def test_channel_close_after_rpc_timeout(proxy_connection, proxy):
    # Regression test for issue #83. A timed out RPC call must not block
    # channel.close() and the connection must stay usable.
    channel = await proxy_connection.channel()

    with proxy.slowdown(1, 1):
        with pytest.raises(asyncio.TimeoutError):
            await channel.queue_declare(auto_delete=True, timeout=0.1)

    await asyncio.wait_for(channel.close(), timeout=5)

    assert not proxy_connection.is_closed
    channel = await proxy_connection.channel()
    await channel.queue_declare(auto_delete=True)
    await channel.close()


@aiomisc.timeout(20)
async def test_channel_close_while_rpc_pending(proxy_connection, proxy):
    # close() waits for the channel lock while an RPC call is pending. When
    # that call is cancelled the channel is closed once; a second
    # Channel.Close on the closed channel would be a protocol error.
    channel = await proxy_connection.channel()

    with proxy.slowdown(0.5, 0.5):
        declare = asyncio.ensure_future(
            channel.queue_declare(auto_delete=True),
        )
        await asyncio.sleep(0.1)
        close = asyncio.ensure_future(channel.close())
        await asyncio.sleep(0.1)
        declare.cancel()
        with pytest.raises(asyncio.CancelledError):
            await declare
        await asyncio.wait_for(close, timeout=10)

    # Give the broker time to react to the frames it received.
    await asyncio.sleep(0.5)
    assert not proxy_connection.is_closed
    channel = await proxy_connection.channel()
    await channel.queue_declare(auto_delete=True)
    await channel.close()


@aiomisc.timeout(20)
async def test_rpc_cancelled_during_put(amqp_connection: aiormq.Connection):
    # A cancel while the frame waits for the write queue must still close
    # the open channel on the broker with a Channel.Close handshake.
    channel = await amqp_connection.channel()

    class BlockingQueue:
        def __init__(self, queue: asyncio.Queue):
            self.queue = queue
            self.block = True

        async def put(self, item):
            if self.block:
                self.block = False
                await asyncio.Event().wait()    # blocks until cancelled
            await self.queue.put(item)

        def __getattr__(self, name):
            return getattr(self.queue, name)

    channel.write_queue = BlockingQueue(channel.write_queue)    # type: ignore

    declare = asyncio.ensure_future(channel.queue_declare(auto_delete=True))
    await asyncio.sleep(0.1)
    declare.cancel()
    with pytest.raises(asyncio.CancelledError):
        await declare

    assert channel.number in amqp_connection.channels

    # The broker confirms the close and the number becomes free.
    while channel.number in amqp_connection.channels:
        await asyncio.sleep(0.05)

    other = await amqp_connection.channel(channel_number=channel.number)
    await other.queue_declare(auto_delete=True)
    await other.close()


@aiomisc.timeout(20)
async def test_consumer_cancel_callbacks(amqp_channel: aiormq.Channel):
    # The broker cancels the consumer when its queue is deleted. Every
    # callback in on_consumer_cancel_callbacks gets the Basic.Cancel frame.
    declare_ok = await amqp_channel.queue_declare(auto_delete=True)
    consume_ok = await amqp_channel.basic_consume(
        declare_ok.queue, lambda message: None,
    )

    async_calls: asyncio.Queue = asyncio.Queue()
    sync_calls: list = []

    async def on_cancel_async(frame: aiormq.spec.Basic.Cancel) -> None:
        await async_calls.put(frame)

    def on_cancel_sync(frame: aiormq.spec.Basic.Cancel) -> None:
        sync_calls.append(frame)

    def on_cancel_broken(frame: aiormq.spec.Basic.Cancel) -> None:
        raise RuntimeError("callback failure")

    amqp_channel.on_consumer_cancel_callbacks.update(
        {on_cancel_async, on_cancel_sync, on_cancel_broken},
    )

    await amqp_channel.queue_delete(declare_ok.queue)

    frame = await asyncio.wait_for(async_calls.get(), timeout=5)
    assert isinstance(frame, aiormq.spec.Basic.Cancel)
    assert frame.consumer_tag == consume_ok.consumer_tag

    while not sync_calls:
        await asyncio.sleep(0.01)
    assert sync_calls[0].consumer_tag == consume_ok.consumer_tag

    assert consume_ok.consumer_tag not in amqp_channel.consumers

    # The failing callback did not break the channel.
    assert not amqp_channel.is_closed
    await amqp_channel.queue_declare(auto_delete=True)


@aiomisc.timeout(20)
async def test_client_cancel_does_not_call_callbacks(
    amqp_channel: aiormq.Channel,
):
    # Basic.CancelOk answers a client basic_cancel() call. It is not a
    # broker-initiated cancel.
    declare_ok = await amqp_channel.queue_declare(auto_delete=True)
    consume_ok = await amqp_channel.basic_consume(
        declare_ok.queue, lambda message: None,
    )

    calls: list = []
    amqp_channel.on_consumer_cancel_callbacks.add(calls.append)

    await amqp_channel.basic_cancel(consume_ok.consumer_tag)
    await asyncio.sleep(0.1)

    assert calls == []
    assert consume_ok.consumer_tag not in amqp_channel.consumers
