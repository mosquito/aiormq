import asyncio
import uuid
from os import urandom

import aiomisc
import pamqp.frame
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


def written_frames(write_queue: asyncio.Queue) -> list:
    """Return the frames which wait in a channel write queue."""
    frames = []
    while not write_queue.empty():
        payload = write_queue.get_nowait().payload
        while payload:
            consumed, _, frame = pamqp.frame.unmarshal(payload)
            payload = payload[consumed:]
            frames.append(frame)
    return frames


@pytest.mark.parametrize("capability", [True, False])
@aiomisc.timeout(20)
async def test_broker_cancel_answer_needs_capability(
    amqp_channel: aiormq.Channel, capability: bool,
):
    # A broker which advertises accept_consumer_cancel_ok holds the consumer
    # tag until the client answers Basic.Cancel with Basic.CancelOk. Brokers
    # without the capability close the channel on that answer.
    declare_ok = await amqp_channel.queue_declare(auto_delete=True)
    consume_ok = await amqp_channel.basic_consume(
        declare_ok.queue, lambda message: None,
    )

    # The answer goes to a private queue, so it never reaches the broker
    # and the test result does not depend on the broker version.
    write_queue: asyncio.Queue = asyncio.Queue()
    pending_before_callback: list = []
    done = asyncio.Event()

    def on_cancel(frame: aiormq.spec.Basic.Cancel) -> None:
        pending_before_callback.append(write_queue.qsize())
        done.set()

    amqp_channel.on_consumer_cancel_callbacks.add(on_cancel)
    capabilities = amqp_channel.connection.server_capabilities
    original = capabilities.get("accept_consumer_cancel_ok")
    capabilities["accept_consumer_cancel_ok"] = capability
    original_write_queue = amqp_channel.write_queue
    amqp_channel.write_queue = write_queue

    try:
        # The broker always sets the nowait flag in its own Basic.Cancel.
        await amqp_channel.frames.put(
            (1, aiormq.spec.Basic.Cancel(
                consumer_tag=consume_ok.consumer_tag, nowait=True,
            )),
        )
        await asyncio.wait_for(done.wait(), timeout=5)
    finally:
        amqp_channel.write_queue = original_write_queue
        if original is None:
            capabilities.pop("accept_consumer_cancel_ok", None)
        else:
            capabilities["accept_consumer_cancel_ok"] = original

    frames = written_frames(write_queue)

    if capability:
        assert len(frames) == 1
        assert isinstance(frames[0], aiormq.spec.Basic.CancelOk)
        assert frames[0].consumer_tag == consume_ok.consumer_tag
        # The answer is queued before the callbacks run, therefore a
        # callback which consumes again with the same tag reaches the
        # broker later than the answer.
        assert pending_before_callback == [1]
    else:
        assert frames == []
        assert pending_before_callback == [0]

    assert consume_ok.consumer_tag not in amqp_channel.consumers
    assert not amqp_channel.is_closed


@aiomisc.timeout(20)
async def test_broker_cancel_unknown_tag_is_not_answered(
    amqp_channel: aiormq.Channel,
):
    # An unknown consumer tag gets no answer. The broker holds no such
    # consumer, and brokers before 4.3.7 would close the channel.
    done = asyncio.Event()
    amqp_channel.on_consumer_cancel_callbacks.add(lambda frame: done.set())

    write_queue: asyncio.Queue = asyncio.Queue()
    capabilities = amqp_channel.connection.server_capabilities
    original = capabilities.get("accept_consumer_cancel_ok")
    capabilities["accept_consumer_cancel_ok"] = True
    original_write_queue = amqp_channel.write_queue
    amqp_channel.write_queue = write_queue

    try:
        await amqp_channel.frames.put(
            (1, aiormq.spec.Basic.Cancel(
                consumer_tag="ctag-unknown", nowait=True,
            )),
        )
        await asyncio.wait_for(done.wait(), timeout=5)
    finally:
        amqp_channel.write_queue = original_write_queue
        if original is None:
            capabilities.pop("accept_consumer_cancel_ok", None)
        else:
            capabilities["accept_consumer_cancel_ok"] = original

    assert written_frames(write_queue) == []
    assert not amqp_channel.is_closed


@aiomisc.timeout(60)
async def test_consumer_timeout_allows_the_same_consumer_tag(
    amqp_direct_url,
):
    # RabbitMQ 4.3 and later cancel a timed out consumer instead of closing
    # the channel, and hold the consumer tag until the client answers. A
    # client which consumes again with the same tag before the answer gets
    # "NOT_ALLOWED - attempt to reuse consumer tag" and loses the whole
    # connection.
    async with aiormq.Connection(amqp_direct_url) as connection:
        if not connection.accept_consumer_cancel_ok:
            pytest.skip("Broker without accept_consumer_cancel_ok")

        channel = await connection.channel()
        queue = "consumer-timeout-%s" % uuid.uuid4().hex
        await channel.queue_declare(
            queue, durable=True,
            arguments={
                "x-queue-type": "quorum",
                "x-consumer-timeout": 1000,
            },
        )

        redelivered: asyncio.Queue = asyncio.Queue()
        cancels: asyncio.Queue = asyncio.Queue()
        tasks: list = []

        first_delivery = True

        async def consume(message: DeliveredMessage) -> None:
            nonlocal first_delivery
            if first_delivery:
                # The first delivery stays unacknowledged, so the broker
                # cancels the consumer after x-consumer-timeout.
                first_delivery = False
                return
            await message.channel.basic_ack(message.delivery_tag)
            await redelivered.put(message)

        async def reconsume(consumer_tag: str) -> None:
            await channel.basic_consume(
                queue, consume, consumer_tag=consumer_tag,
            )

        def on_cancel(frame: aiormq.spec.Basic.Cancel) -> None:
            # A robust client consumes again with the same tag. The call
            # runs as a task, because an RPC inside the callback would wait
            # for the blocked channel reader.
            tasks.append(asyncio.ensure_future(reconsume(frame.consumer_tag)))
            cancels.put_nowait(frame)

        channel.on_consumer_cancel_callbacks.add(on_cancel)
        consume_ok = await channel.basic_consume(queue, consume)
        await channel.basic_publish(b"first", routing_key=queue)

        frame = await asyncio.wait_for(cancels.get(), timeout=30)
        assert frame.consumer_tag == consume_ok.consumer_tag
        await asyncio.wait_for(asyncio.gather(*tasks), timeout=30)

        # The broker redelivers the message to the restored consumer, which
        # proves the consumer tag and the connection still work.
        message = await asyncio.wait_for(redelivered.get(), timeout=30)
        assert message.body == b"first"

        assert consume_ok.consumer_tag in channel.consumers
        assert not channel.is_closed
        assert not connection.is_closed

        await channel.basic_cancel(consume_ok.consumer_tag)
        await channel.queue_delete(queue)
