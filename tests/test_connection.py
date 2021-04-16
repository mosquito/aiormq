import asyncio
import os
import ssl
import uuid
from binascii import hexlify
from typing import Optional

import pytest
from pamqp.commands import Basic

import aiormq
from aiormq.auth import AuthBase, PlainAuth
from aiormq.abc import DeliveredMessage

from .conftest import AMQP_URL, cert_path, skip_when_quick_test


CERT_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), "certs"))


async def test_simple(amqp_connection: aiormq.Connection):
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
    consume_ok = await channel.basic_consume(deaclare_ok.queue, queue.put)
    await channel.basic_publish(b"foo", routing_key=deaclare_ok.queue)

    message = await queue.get()  # type: DeliveredMessage
    assert message.body == b"foo"

    with pytest.raises(aiormq.exceptions.PublishError) as e:
        await channel.basic_publish(
            b"bar", routing_key=deaclare_ok.queue + "foo", mandatory=True,
        )

    message = e.value.message

    assert message.delivery.routing_key == deaclare_ok.queue + "foo"
    assert message.body == b"bar"
    assert "'NO_ROUTE' for routing key" in repr(e.value)

    cancel_ok = await channel.basic_cancel(consume_ok.consumer_tag)
    assert cancel_ok.consumer_tag == consume_ok.consumer_tag
    await channel.queue_delete(deaclare_ok.queue)

    deaclare_ok = await channel.queue_declare(auto_delete=True)
    await channel.basic_publish(b"foo bar", routing_key=deaclare_ok.queue)

    message = await channel.basic_get(deaclare_ok.queue, no_ack=True)
    assert message.body == b"foo bar"

    await amqp_connection.close()

    with pytest.raises(RuntimeError):
        await channel.basic_get(deaclare_ok.queue)

    with pytest.raises(RuntimeError):
        await amqp_connection.channel()


async def test_channel_reuse(amqp_connection: aiormq.Connection):
    for _ in range(10):
        channel = await amqp_connection.channel(channel_number=1)
        await channel.basic_qos(prefetch_count=1)
        await channel.close()
        del channel


async def test_channel_closed_reuse(amqp_connection: aiormq.Connection):
    for _ in range(10):
        channel = await amqp_connection.channel(channel_number=1)
        with pytest.raises(aiormq.ChannelAccessRefused):
            await channel.exchange_declare("", passive=True, auto_delete=True)


async def test_bad_channel(amqp_connection: aiormq.Connection):
    with pytest.raises(ValueError):
        await amqp_connection.channel(65536)

    with pytest.raises(ValueError):
        await amqp_connection.channel(-1)

    channel = await amqp_connection.channel(65535)

    with pytest.raises(aiormq.exceptions.ChannelNotFoundEntity):
        await channel.queue_bind(uuid.uuid4().hex, uuid.uuid4().hex)


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


async def test_channel_close(amqp_connection):
    channel = await amqp_connection.channel()

    assert channel.number in amqp_connection.channels

    await channel.close()

    assert channel.number not in amqp_connection.channels


async def test_conncetion_reject(loop):
    with pytest.raises(ConnectionError):
        await aiormq.connect(
            "amqp://guest:guest@127.0.0.1:59999/", loop=loop,
        )

    connection = aiormq.Connection(
        "amqp://guest:guest@127.0.0.1:59999/", loop=loop,
    )

    with pytest.raises(ConnectionError):
        await loop.create_task(connection.connect())


async def test_auth_base(amqp_connection):
    with pytest.raises(NotImplementedError):
        AuthBase(amqp_connection).marshal()


async def test_auth_plain(amqp_connection, loop):
    auth = PlainAuth(amqp_connection).marshal()

    assert auth == PlainAuth(amqp_connection).marshal()

    auth_parts = auth.split("\x00")
    assert auth_parts == ["", "guest", "guest"]

    connection = aiormq.Connection(
        amqp_connection.url.with_user("foo").with_password("bar"),
        loop=loop,
    )

    auth = PlainAuth(connection).marshal()

    auth_parts = auth.split("\x00")
    assert auth_parts == ["", "foo", "bar"]

    auth = PlainAuth(connection)
    auth.value = "boo"

    assert auth.marshal() == "boo"


async def test_channel_closed(amqp_connection):

    for i in range(10):
        channel = await amqp_connection.channel()

        with pytest.raises(aiormq.exceptions.ChannelNotFoundEntity):
            await channel.basic_consume("foo", lambda x: None)

        channel = await amqp_connection.channel()

        with pytest.raises(aiormq.exceptions.ChannelNotFoundEntity):
            await channel.queue_declare(
                "foo_%s" % i, auto_delete=True, passive=True,
            )

    await amqp_connection.close()


async def test_bad_credentials(amqp_connection, loop):
    connection = aiormq.Connection(
        amqp_connection.url.with_password(uuid.uuid4().hex), loop=loop,
    )

    with pytest.raises(aiormq.exceptions.ProbableAuthenticationError):
        await connection.connect()


async def test_non_publisher_confirms(amqp_connection):
    amqp_connection.server_capabilities["publisher_confirms"] = False

    with pytest.raises(ValueError):
        await amqp_connection.channel(publisher_confirms=True)

    await amqp_connection.channel(publisher_confirms=False)


@skip_when_quick_test
async def test_no_free_channels(amqp_connection: aiormq.Connection):
    await asyncio.wait_for(
        asyncio.gather(
            *[
                amqp_connection.channel(n + 1)
                for n in range(amqp_connection.connection_tune.channel_max)
            ],
        ),
        timeout=120,
    )

    with pytest.raises(aiormq.exceptions.ConnectionNotAllowed):
        await asyncio.wait_for(amqp_connection.channel(), timeout=5)


async def test_huge_message(amqp_connection: aiormq.Connection):
    conn = amqp_connection  # type: aiormq.Connection
    body = os.urandom(int(conn.connection_tune.frame_max * 2.5))
    channel = await conn.channel()  # type: aiormq.Channel

    queue = asyncio.Queue()

    deaclare_ok = await channel.queue_declare(auto_delete=True)
    await channel.basic_consume(deaclare_ok.queue, queue.put)
    await channel.basic_publish(body, routing_key=deaclare_ok.queue)

    message: DeliveredMessage = await queue.get()
    assert message.body == body


async def test_return_message(amqp_connection: aiormq.Connection):
    conn = amqp_connection  # type: aiormq.Connection
    body = os.urandom(512)
    routing_key = hexlify(os.urandom(16)).decode()

    channel: aiormq.Channel = await conn.channel(
        on_return_raises=False,
    )

    result: Optional[Basic.Return] = await channel.basic_publish(
        body, routing_key=routing_key, mandatory=True,
    )

    assert result is not None

    assert result.delivery.name == "Basic.Return"
    assert result.delivery.routing_key == routing_key


async def test_cancel_on_queue_deleted(amqp_connection, loop):
    conn = amqp_connection  # type: aiormq.Connection
    channel = await conn.channel()  # type: aiormq.Channel
    deaclare_ok = await channel.queue_declare(auto_delete=True)
    consume_ok = await channel.basic_consume(deaclare_ok.queue, print)

    assert consume_ok.consumer_tag in channel.consumers

    with pytest.raises(aiormq.DuplicateConsumerTag):
        await channel.basic_consume(
            deaclare_ok.queue, print, consumer_tag=consume_ok.consumer_tag,
        )

    await channel.queue_delete(deaclare_ok.queue)

    await asyncio.sleep(0.1)

    assert consume_ok.consumer_tag not in channel.consumers


URL_VHOSTS = [
    ("amqp:///", "/"),
    ("amqp:////", "/"),
    ("amqp:///test", "test"),
    ("amqp:////test", "/test"),
    ("amqp://localhost/test", "test"),
    ("amqp://localhost//test", "/test"),
    ("amqps://localhost:5678//test", "/test"),
    ("amqps://localhost:5678/-test", "-test"),
    ("amqp://guest:guest@localhost/@test", "@test"),
    ("amqp://guest:guest@localhost//@test", "/@test"),
]


async def test_ssl_verification_fails_without_trusted_ca():
    url = AMQP_URL.with_scheme("amqps")
    with pytest.raises(ConnectionError, match=".*CERTIFICATE_VERIFY_FAILED.*"):
        connection = aiormq.Connection(url)
        await connection.connect()


async def test_ssl_context():
    url = AMQP_URL.with_scheme("amqps")
    context = ssl.create_default_context(
        purpose=ssl.Purpose.SERVER_AUTH,
        cafile=cert_path("ca.pem")
    )
    context.load_cert_chain(cert_path("client.pem"), cert_path("client.key"))
    context.check_hostname = False
    connection = aiormq.Connection(url, context=context)
    await connection.connect()
    await connection.close()


@pytest.mark.parametrize("url,vhost", URL_VHOSTS)
async def test_connection_urls_vhosts(url, vhost, loop):
    assert aiormq.Connection(url, loop=loop).vhost == vhost
