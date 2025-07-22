import asyncio
import itertools
import os
import ssl
import uuid
from binascii import hexlify
from typing import Any, Optional, Tuple

import aiomisc
import pytest
from pamqp.commands import Basic
from yarl import URL

import aiormq
from aiormq.abc import DeliveredMessage, SSLCerts
from aiormq.auth import AuthBase, ExternalAuth, PlainAuth
from aiormq.connection import (
    SSLContextProvider,
    TransportFactory,
    parse_int,
    parse_timeout,
    parse_bool
)

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

    message: DeliveredMessage = await queue.get()
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


class _TcpTransportFactory(TransportFactory):
    async def create(
            self,
            url: URL,
            **kwargs: Any,
    ) -> Tuple[asyncio.StreamReader, asyncio.StreamWriter]:
        ssl_context_provider = kwargs.pop("ssl_context_provider")
        assert isinstance(ssl_context_provider, SSLContextProvider)

        loop = asyncio.get_event_loop()
        reader = asyncio.StreamReader(loop=loop)
        protocol = asyncio.StreamReaderProtocol(reader, loop=loop)
        if url.scheme == "amqps":
            ssl = await ssl_context_provider.get_context()
        else:
            ssl = None

        transport, _ = await loop.create_connection(
            lambda: protocol, url.host, url.port, ssl=ssl,
        )
        writer = asyncio.StreamWriter(transport, protocol, reader, loop)
        return reader, writer


async def test_open_with_transport_factory(amqp_url):
    amqp_connection = await aiormq.connect(
        amqp_url,
        transport_factory=_TcpTransportFactory(),
    )

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


async def test_auth_external(loop):

    url = AMQP_URL.with_scheme("amqps")
    url.update_query(auth="external")

    connection = aiormq.Connection

    auth = ExternalAuth(connection).marshal()

    auth = ExternalAuth(connection)
    auth.value = ""

    assert auth.marshal() == ""


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


async def test_timeout_default(loop):
    connection = aiormq.Connection(AMQP_URL, loop=loop)
    await connection.connect()
    assert connection.timeout == 60
    await connection.close()


async def test_timeout_1000(loop):
    url = AMQP_URL.update_query(timeout=1000)
    connection = aiormq.Connection(url, loop=loop)
    await connection.connect()
    assert connection.timeout
    await connection.close()


async def test_heartbeat_0(loop):
    url = AMQP_URL.update_query(heartbeat=0)
    connection = aiormq.Connection(url, loop=loop)
    await connection.connect()
    assert connection.connection_tune.heartbeat == 0
    await connection.close()


async def test_heartbeat_default(loop):
    connection = aiormq.Connection(AMQP_URL, loop=loop)
    await connection.connect()
    assert connection.connection_tune.heartbeat == 60
    await connection.close()


async def test_heartbeat_above_range(loop):
    url = AMQP_URL.update_query(heartbeat=70000)
    connection = aiormq.Connection(url, loop=loop)
    await connection.connect()
    assert connection.connection_tune.heartbeat == 0
    await connection.close()


async def test_heartbeat_under_range(loop):
    url = AMQP_URL.update_query(heartbeat=-1)
    connection = aiormq.Connection(url, loop=loop)
    await connection.connect()
    assert connection.connection_tune.heartbeat == 0
    await connection.close()


async def test_heartbeat_not_int(loop):
    url = AMQP_URL.update_query(heartbeat="None")
    connection = aiormq.Connection(url, loop=loop)
    await connection.connect()
    assert connection.connection_tune.heartbeat == 0
    await connection.close()


async def test_bad_credentials(amqp_url: URL):
    with pytest.raises(aiormq.exceptions.ProbableAuthenticationError):
        await aiormq.connect(amqp_url.with_password(uuid.uuid4().hex))


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
    conn: aiormq.Connection = amqp_connection
    body = os.urandom(int(conn.connection_tune.frame_max * 2.5))
    channel: aiormq.Channel = await conn.channel()

    queue = asyncio.Queue()

    deaclare_ok = await channel.queue_declare(auto_delete=True)
    await channel.basic_consume(deaclare_ok.queue, queue.put)
    await channel.basic_publish(body, routing_key=deaclare_ok.queue)

    message: DeliveredMessage = await queue.get()
    assert message.body == body


async def test_return_message(amqp_connection: aiormq.Connection):
    conn: aiormq.Connection = amqp_connection
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
    conn: aiormq.Connection = amqp_connection
    channel: aiormq.Channel = await conn.channel()
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
        cafile=cert_path("ca.pem"),
    )
    context.load_cert_chain(cert_path("client.pem"), cert_path("client.key"))
    context.check_hostname = False
    context.verify_mode = ssl.CERT_NONE
    connection = aiormq.Connection(url, context=context)
    await connection.connect()
    await connection.close()


@pytest.mark.parametrize("url,vhost", URL_VHOSTS)
async def test_connection_urls_vhosts(url, vhost, loop):
    assert aiormq.Connection(url, loop=loop).vhost == vhost


async def test_update_secret(amqp_connection, amqp_url: URL):
    respone = await amqp_connection.update_secret(
        amqp_url.password, timeout=1,
    )
    assert isinstance(respone, aiormq.spec.Connection.UpdateSecretOk)


@aiomisc.timeout(20)
async def test_connection_stuck(proxy, amqp_url: URL):
    url = amqp_url.with_host(
        proxy.proxy_host,
    ).with_port(
        proxy.proxy_port,
    ).update_query(heartbeat="1")

    connection = await aiormq.connect(url)

    async with connection:
        # delay the delivery of each packet by 5 seconds, which
        # is more than the heartbeat
        with proxy.slowdown(50, 50):
            while connection.is_opened:
                await asyncio.sleep(1)

        writer_task: asyncio.Task = connection._writer_task      # type: ignore

        assert writer_task.done()

        with pytest.raises(asyncio.CancelledError):
            assert writer_task.result()

        reader_task: asyncio.Task = connection._reader_task      # type: ignore

        assert reader_task.done()

        with pytest.raises(asyncio.CancelledError):
            assert reader_task.result()


class BadNetwork:
    def __init__(self, proxy, stair: int, disconnect_time: float):
        self.proxy = proxy
        self.stair = stair
        self.disconnect_time = disconnect_time
        self.num_bytes = 0
        self.loop = asyncio.get_event_loop()
        self.lock = asyncio.Lock()

        proxy.set_content_processors(
            self.client_to_server,
            self.server_to_client,
        )

    async def disconnect(self):
        async with self.lock:
            await asyncio.sleep(self.disconnect_time)
            await self.proxy.disconnect_all()
            self.stair *= 2
            self.num_bytes = 0

    async def server_to_client(self, chunk: bytes) -> bytes:
        async with self.lock:
            self.num_bytes += len(chunk)
            if self.num_bytes < self.stair:
                return chunk
            self.loop.create_task(self.disconnect())
            return chunk

    @staticmethod
    def client_to_server(chunk: bytes) -> bytes:
        return chunk


DISCONNECT_OFFSETS = [2 << i for i in range(1, 10)]
STAIR_STEPS = list(
    itertools.product([0.0, 0.005, 0.05, 0.1], DISCONNECT_OFFSETS),
)
STAIR_STEPS_IDS = [
    f"[{i // len(DISCONNECT_OFFSETS)}] {t}-{s}"
    for i, (t, s) in enumerate(STAIR_STEPS)
]


@aiomisc.timeout(30)
@pytest.mark.parametrize(
    "disconnect_time,stair", STAIR_STEPS,
    ids=STAIR_STEPS_IDS,
)
async def test_connection_close_stairway(
    disconnect_time: float, stair: int, proxy, amqp_url: URL,
):
    url = amqp_url.with_host(
        proxy.proxy_host,
    ).with_port(
        proxy.proxy_port,
    ).update_query(heartbeat="1")

    BadNetwork(proxy, stair, disconnect_time)

    async def run():
        connection = await aiormq.connect(url)
        queue = asyncio.Queue()
        channel = await connection.channel()
        declare_ok = await channel.queue_declare(auto_delete=True)
        await channel.basic_consume(
            declare_ok.queue, queue.put, no_ack=True,
        )

        while True:
            await channel.basic_publish(
                b"test", routing_key=declare_ok.queue,
            )
            message: DeliveredMessage = await queue.get()
            assert message.body == b"test"

    for _ in range(5):
        with pytest.raises(aiormq.AMQPError):
            await run()


async def test_ssl_context_provider_static(loop):
    certs = SSLCerts(
        cert=None,
        key=None,
        capath=None,
        cafile=None,
        cadata=None,
        verify=False,
    )

    static_context = ssl.create_default_context()
    provider = SSLContextProvider(
        ssl_context=static_context,
        ssl_certs=certs,
        loop=loop
    )

    provided_context = await provider.get_context()
    assert provided_context is static_context


async def test_ssl_context_provider_created(loop):
    certs = SSLCerts(
        cert=cert_path("client.pem"),
        key=cert_path("client.key"),
        capath=None,
        cafile=cert_path("ca.pem"),
        cadata=None,
        verify=True,
    )

    default_context = ssl.create_default_context()

    provider = SSLContextProvider(
        ssl_context=None,
        ssl_certs=certs,
        loop=loop
    )

    provided_context = await provider.get_context()
    assert provided_context != default_context

    second_provided_context = await provider.get_context()
    assert provided_context is second_provided_context


PARSE_INT_PARAMS = (
    (1, 1),
    ("1", 1),
    ("0.1", 0),
    ("-1", -1),
)


@pytest.mark.parametrize("value,expected", PARSE_INT_PARAMS)
def test_parse_int(value, expected):
    assert parse_int(value) == expected


PARSE_TIMEOUT_PARAMS = (
    (1, 1),
    (1.0, 1),
    ("0", 0),
    ("0.0", 0),
    ("0.111", 0.111),
)


@pytest.mark.parametrize("value,expected", PARSE_TIMEOUT_PARAMS)
def test_parse_timeout(value, expected):
    assert parse_timeout(value) == expected


PARSE_BOOL_PARAMS = (
    ("no", False),
    ("nope", False),
    ("do not do it bro", False),
    ("YES", True),
    ("yes", True),
    ("yeS", True),
    ("yEs", True),
    ("True", True),
    ("true", True),
    ("TRUE", True),
    ("1", True),
    ("ENABLE", True),
    ("ENAbled", True),
    ("y", True),
    ("Y", True),
)


@pytest.mark.parametrize("value,expected", PARSE_BOOL_PARAMS)
def test_parse_bool(value, expected):
    assert parse_bool(value) == expected
