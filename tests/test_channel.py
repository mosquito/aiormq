import asyncio

import pytest

import aiormq


pytestmark = pytest.mark.asyncio


async def test_simple(amqp_channel: aiormq.Channel):
    await amqp_channel.basic_qos(prefetch_count=1)
    assert amqp_channel.number

    queue = asyncio.Queue()

    deaclare_ok = await amqp_channel.queue_declare(auto_delete=True)
    consume_ok = await amqp_channel.basic_consume(deaclare_ok.queue, queue.put)
    await amqp_channel.basic_publish(
        b'foo', routing_key=deaclare_ok.queue,
        properties=aiormq.spec.Basic.Properties(message_id='123')
    )

    message = await queue.get()     # type: DeliveredMessage
    assert message.body == b'foo'

    cancel_ok = await amqp_channel.basic_cancel(consume_ok.consumer_tag)
    assert cancel_ok.consumer_tag == consume_ok.consumer_tag
    await amqp_channel.queue_delete(deaclare_ok.queue)

    deaclare_ok = await amqp_channel.queue_declare(auto_delete=True)
    await amqp_channel.basic_publish(b'foo bar', routing_key=deaclare_ok.queue)

    message = await amqp_channel.basic_get(deaclare_ok.queue, no_ack=True)
    assert message.body == b'foo bar'


async def test_blank_body(amqp_channel: aiormq.Channel):
    await amqp_channel.basic_qos(prefetch_count=1)
    assert amqp_channel.number

    queue = asyncio.Queue()

    deaclare_ok = await amqp_channel.queue_declare(auto_delete=True)
    consume_ok = await amqp_channel.basic_consume(deaclare_ok.queue, queue.put)
    await amqp_channel.basic_publish(
        b'', routing_key=deaclare_ok.queue,
        properties=aiormq.spec.Basic.Properties(message_id='123')
    )

    message = await queue.get()     # type: DeliveredMessage
    assert message.body == b''

    cancel_ok = await amqp_channel.basic_cancel(consume_ok.consumer_tag)
    assert cancel_ok.consumer_tag == consume_ok.consumer_tag
    await amqp_channel.queue_delete(deaclare_ok.queue)

    deaclare_ok = await amqp_channel.queue_declare(auto_delete=True)
    await amqp_channel.basic_publish(b'foo bar', routing_key=deaclare_ok.queue)

    message = await amqp_channel.basic_get(deaclare_ok.queue, no_ack=True)
    assert message.body == b'foo bar'


async def test_bad_consumer(amqp_channel: aiormq.Channel, event_loop):
    channel = amqp_channel      # type: aiormq.Channel
    await channel.basic_qos(prefetch_count=1)

    declare_ok = await channel.queue_declare()

    future = event_loop.create_future()

    await channel.basic_publish(b'urgent', routing_key=declare_ok.queue)

    consumer_tag = event_loop.create_future()

    async def bad_consumer(message):
        await channel.basic_cancel(await consumer_tag)
        future.set_result(message)
        raise Exception

    consume_ok = await channel.basic_consume(
        declare_ok.queue, bad_consumer, no_ack=False
    )

    consumer_tag.set_result(consume_ok.consumer_tag)

    message = await future
    await channel.basic_reject(message.delivery.delivery_tag, requeue=True)
    assert message.body == b'urgent'

    future = event_loop.create_future()

    await channel.basic_consume(
        declare_ok.queue, future.set_result, no_ack=True
    )

    message = await future

    assert message.body == b'urgent'
    await channel.basic_ack(message.delivery.delivery_tag)


async def test_ack_nack_reject(amqp_channel: aiormq.Channel, event_loop):
    channel = amqp_channel                      # type: aiormq.Channel
    await channel.basic_qos(prefetch_count=1)

    declare_ok = await channel.queue_declare(auto_delete=True)
    queue = asyncio.Queue(loop=event_loop)

    await channel.basic_consume(declare_ok.queue, queue.put, no_ack=False)

    await channel.basic_publish(b'rejected', routing_key=declare_ok.queue)
    message = await queue.get()
    assert message.body == b'rejected'
    await channel.basic_reject(message.delivery.delivery_tag, requeue=False)

    await channel.basic_publish(b'nacked', routing_key=declare_ok.queue)
    message = await queue.get()
    assert message.body == b'nacked'
    await channel.basic_nack(message.delivery.delivery_tag, requeue=False)

    await channel.basic_publish(b'acked', routing_key=declare_ok.queue)
    message = await queue.get()
    assert message.body == b'acked'
    await channel.basic_ack(message.delivery.delivery_tag)
