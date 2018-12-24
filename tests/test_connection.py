import asyncio
import os
import uuid

import pytest

from aiormq import exceptions as exc
from aiormq.auth import AuthBase, PlainAuth
from aiormq.connection import Connection

pytestmark = pytest.mark.asyncio

CERT_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), 'certs'))


pytest.mark.parametrize(
    "connection",
)


async def test_simple(amqp_connection: Connection):
    assert amqp_connection.reader is not None
    assert amqp_connection.writer is not None

    channel1 = await amqp_connection.channel()
    await channel1.basic_qos(prefetch_count=1)

    assert channel1.number

    channel2 = await amqp_connection.channel(11)
    assert channel2.number

    await channel1.close()
    await channel2.close()

    channel = await amqp_connection.channel()

    queue = asyncio.Queue()

    deaclare_ok = await channel.queue_declare(auto_delete=True)
    await channel.basic_consume(deaclare_ok.queue, queue.put)
    await channel.basic_publish(b'foo', routing_key=deaclare_ok.queue)

    message = await queue.get()     # type: DeliveredMessage
    assert message.body == b'foo'

    with pytest.raises(exc.DeliveryError) as e:
        await channel.basic_publish(
            b'bar', routing_key=deaclare_ok.queue + 'foo',
            mandatory=True,
        )

    message = e.value.message

    assert message.delivery.routing_key == deaclare_ok.queue + 'foo'
    assert message.body == b'bar'

    await channel.queue_delete(deaclare_ok.queue)

    await amqp_connection.close()
    assert amqp_connection.reader is None
    assert amqp_connection.writer is None


async def test_bad_channel(amqp_connection: Connection):
    with pytest.raises(ValueError):
        await amqp_connection.channel(65536)

    with pytest.raises(ValueError):
        await amqp_connection.channel(-1)

    channel = await amqp_connection.channel(65535)

    with pytest.raises(exc.ChannelNotFoundEntity):
        await channel.queue_bind(
            uuid.uuid4().hex,
            uuid.uuid4().hex
        )

    await asyncio.sleep(0.1)

    assert amqp_connection.channels[channel.number] is None

    with pytest.raises(exc.ConnectionChannelError):
        await amqp_connection.channel(65535)


async def test_properties(amqp_connection):
    assert amqp_connection.connection_tune.channel_max > 0
    assert amqp_connection.connection_tune.heartbeat > 1
    assert amqp_connection.connection_tune.frame_max > 1024
    await amqp_connection.close()


async def test_open(amqp_connection):
    with pytest.raises(RuntimeError):
        await amqp_connection.connect()

    channel = await amqp_connection.channel()
    await channel.close()
    await amqp_connection.close()


async def test_auth_base(amqp_connection):
    with pytest.raises(NotImplementedError):
        AuthBase(amqp_connection).marshal()


async def test_auth_plain(amqp_connection):
    auth = PlainAuth(amqp_connection).marshal()

    assert auth == PlainAuth(amqp_connection).marshal()

    auth_parts = auth.split(b"\x00")
    assert auth_parts == [b'', b'guest', b'guest']

    connection = Connection(
        amqp_connection.url.with_user('foo').with_password('bar')
    )

    auth = PlainAuth(connection).marshal()

    auth_parts = auth.split(b"\x00")
    assert auth_parts == [b'', b'foo', b'bar']


async def test_channel_closed(amqp_connection):

    channel = await amqp_connection.channel()

    with pytest.raises(exc.ChannelNotFoundEntity):
        await channel.basic_consume("foo", lambda x: None)

    await amqp_connection.close()
    return


async def test_bad_credentials(amqp_connection):
    connection = Connection(
        amqp_connection.url.with_password(uuid.uuid4().hex),
    )

    with pytest.raises(exc.ProbableAuthenticationError):
        await connection.connect()


async def test_no_free_channels(amqp_connection):
    for _ in range(amqp_connection.connection_tune.channel_max):
        await amqp_connection.channel()

    with pytest.raises(exc.ConnectionNotAllowed):
        await amqp_connection.channel()
