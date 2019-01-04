import asyncio
import os
import pytest

from aiormq import Channel, spec


pytestmark = pytest.mark.asyncio

CERT_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), 'certs'))


pytest.mark.parametrize(
    "connection",
)


async def test_simple(amqp_channel: Channel):
    await amqp_channel.basic_qos(prefetch_count=1)
    assert amqp_channel.number

    queue = asyncio.Queue()

    deaclare_ok = await amqp_channel.queue_declare(auto_delete=True)
    consume_ok = await amqp_channel.basic_consume(deaclare_ok.queue, queue.put)
    await amqp_channel.basic_publish(
        b'foo', routing_key=deaclare_ok.queue,
        properties=spec.Basic.Properties(message_id='123')
    )

    message = await queue.get()     # type: DeliveredMessage
    assert message.body == b'foo'

    cancel_ok = await amqp_channel.basic_cancel(consume_ok.consumer_tag)
    assert cancel_ok.consumer_tag == consume_ok.consumer_tag
    await amqp_channel.queue_delete(deaclare_ok.queue)

    deaclare_ok = await amqp_channel.queue_declare(auto_delete=True)
    await amqp_channel.basic_publish(b'foo bar', routing_key=deaclare_ok.queue)

    message = await amqp_channel.basic_get(deaclare_ok.queue)
    assert message.body == b'foo bar'
