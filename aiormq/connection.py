import asyncio
import logging
import platform
import ssl
import typing
from base64 import b64decode
from contextlib import suppress

import pamqp.frame
from pamqp import ProtocolHeader
from pamqp import specification as spec
from pamqp.heartbeat import Heartbeat
from yarl import URL

from . import exceptions as exc
from .auth import AuthMechanism
from .base import Base, task
from .channel import Channel
from .tools import censor_url
from .types import (
    ArgumentsType, SSLCerts,
    URLorStr
)
from .version import __version__

log = logging.getLogger(__name__)


try:
    from yarl import DEFAULT_PORTS

    DEFAULT_PORTS['amqp'] = 5672
    DEFAULT_PORTS['amqps'] = 5671
except ImportError:
    pass


PRODUCT = 'aiormq'
PLATFORM = '{} {} ({} build {})'.format(
    platform.python_implementation(),
    platform.python_version(),
    *platform.python_build()
)


def parse_bool(v: str):
    return v == '1' or v.lower() in ('true', 'yes', 'y', 'enable', 'on')


def parse_int(v: str):
    try:
        return int(v)
    except ValueError:
        return 0


class Connection(Base):
    FRAME_BUFFER = 10
    # Interval between sending heartbeats based on the heartbeat(timeout)
    HEARTBEAT_INTERVAL_MULTIPLIER = 0.5
    # Allow two missed heartbeats (based on heartbeat(timeout)
    HEARTBEAT_GRACE_MULTIPLIER = 3
    _HEARTBEAT = pamqp.frame.marshal(Heartbeat(), 0)

    @staticmethod
    def _parse_ca_data(data) -> typing.Optional[bytes]:
        return b64decode(data) if data else data

    def __init__(self, url: URLorStr, *, parent=None,
                 loop: asyncio.AbstractEventLoop = None):

        super().__init__(
            loop=loop or asyncio.get_event_loop(),
            parent=parent
        )

        self.url = URL(url)

        if self.url.path == '/' or not self.url.path:
            self.vhost = '/'
        else:
            self.vhost = self.url.path[1:]

        self.reader = None  # type: asyncio.StreamReader
        self.writer = None  # type: asyncio.StreamWriter
        self.ssl_certs = SSLCerts(
            cafile=self.url.query.get('cafile'),
            capath=self.url.query.get('capath'),
            cadata=self._parse_ca_data(self.url.query.get('cadata')),
            key=self.url.query.get('keyfile'),
            cert=self.url.query.get('certfile'),
            verify=self.url.query.get('no_verify_ssl', '0') == '0'
        )

        self.started = False
        self.__lock = asyncio.Lock(loop=self.loop)
        self.__drain_lock = asyncio.Lock(loop=self.loop)

        self.channels = {}  # type: typing.Dict[int, typing.Optional[Channel]]

        self.server_properties = None   # type: spec.Connection.OpenOk
        self.connection_tune = None  # type: spec.Connection.TuneOk

        self.last_channel = 1

        self.heartbeat_monitoring = parse_bool(self.url.query.get(
            'heartbeat_monitoring', '1'
        ))
        self.heartbeat_timeout = parse_int(self.url.query.get(
            'heartbeat', '0'
        ))
        self.heartbeat_last_received = 0
        self.last_channel_lock = asyncio.Lock(loop=self.loop)
        self.connected = asyncio.Event(loop=self.loop)

    @property
    def lock(self):
        if self.is_closed:
            raise RuntimeError('%r closed' % self)

        return self.__lock

    async def drain(self):
        if not self.writer:
            raise RuntimeError("Writer is %r" % self.writer)

        async with self.__drain_lock:
            return await self.writer.drain()

    @property
    def is_opened(self):
        return self.writer is not None and not self.is_closed

    def __str__(self):
        return str(censor_url(self.url))

    def _get_ssl_context(self):
        context = ssl.create_default_context(
            (
                ssl.Purpose.SERVER_AUTH
                if self.ssl_certs.key
                else ssl.Purpose.CLIENT_AUTH
            ),
            capath=self.ssl_certs.capath,
            cafile=self.ssl_certs.cafile,
            cadata=self.ssl_certs.cadata,
        )

        if self.ssl_certs.key:
            context.load_cert_chain(
                self.ssl_certs.cert,
                self.ssl_certs.key,
            )

        if not self.ssl_certs.verify:
            context.check_hostname = False
            context.verify_mode = ssl.CERT_NONE

        return context

    @staticmethod
    def _client_capabilities():
        return {
            'platform': PLATFORM,
            'version': __version__,
            'product': PRODUCT,
            'capabilities': {
                'authentication_failure_close': True,
                'basic.nack': True,
                'connection.blocked': False,
                'consumer_cancel_notify': True,
                'publisher_confirms': True
            },
            'information': 'See https://github.com/mosquito/aiormq/',
        }

    @staticmethod
    def _credentials_class(start_frame: spec.Connection.Start):
        for mechanism in start_frame.mechanisms.decode().split():
            with suppress(KeyError):
                return AuthMechanism[mechanism]

        raise exc.AuthenticationError(
            start_frame.mechanisms,
            [m.name for m in AuthMechanism]
        )

    async def __rpc(self, request: spec.Frame, wait_response=True):
        self.writer.write(pamqp.frame.marshal(request, 0))

        if not wait_response:
            return

        _, _, frame = await self.__receive_frame()

        if request.synchronous and frame.name not in request.valid_responses:
            raise spec.AMQPInternalError(frame, frame)
        elif isinstance(frame, spec.Connection.Close):
            if frame.reply_code == 403:
                raise exc.ProbableAuthenticationError(frame.reply_text)

            raise exc.ConnectionClosed(frame.reply_code, frame.reply_text)
        return frame

    @task
    async def connect(self):
        if self.writer is not None:
            raise RuntimeError("Already connected")

        ssl_context = None

        if self.url.scheme == 'amqps':
            ssl_context = await self.loop.run_in_executor(
                None, self._get_ssl_context
            )

        try:
            self.reader, self.writer = await asyncio.open_connection(
                self.url.host, self.url.port, ssl=ssl_context,
                loop=self.loop
            )
        except OSError as e:
            raise ConnectionError(*e.args) from e

        try:
            protocol_header = ProtocolHeader()
            self.writer.write(protocol_header.marshal())

            res = await self.__receive_frame()
            _, _, frame = res   # type: spec.Connection.Start
            self.heartbeat_last_received = self.loop.time()
        except EOFError as e:
            raise exc.IncompatibleProtocolError(*e.args) from e

        credentials = self._credentials_class(frame)

        self.server_properties = frame.server_properties

        # noinspection PyTypeChecker
        self.connection_tune = await self.__rpc(spec.Connection.StartOk(
            client_properties=self._client_capabilities(),
            mechanism=credentials.name,
            response=credentials.value(self).marshal()
        ))      # type: spec.Connection.Tune

        if self.heartbeat_timeout > 0:
            self.connection_tune.heartbeat = self.heartbeat_timeout

        await self.__rpc(spec.Connection.TuneOk(
            channel_max=self.connection_tune.channel_max,
            frame_max=self.connection_tune.frame_max,
            heartbeat=self.connection_tune.heartbeat,
        ), wait_response=False)

        await self.__rpc(spec.Connection.Open(virtual_host=self.vhost))

        # noinspection PyAsyncCall
        self.create_task(self.__reader())

        # noinspection PyAsyncCall
        self.create_task(self.__heartbeat_task())
        self.loop.call_soon(self.connected.set)

        return True

    async def __heartbeat_task(self):
        if not self.connection_tune.heartbeat:
            return

        heartbeat_interval = (
            self.connection_tune.heartbeat * self.HEARTBEAT_INTERVAL_MULTIPLIER
        )
        heartbeat_grace_timeout = (
            self.connection_tune.heartbeat * self.HEARTBEAT_GRACE_MULTIPLIER
        )

        while True:
            await asyncio.sleep(heartbeat_interval)

            # Send heartbeat to server unconditionally
            self.writer.write(self._HEARTBEAT)

            if not self.heartbeat_monitoring:
                continue

            # Check if the server sent us something
            # within the heartbeat grace period
            last_heartbeat = self.loop.time() - self.heartbeat_last_received

            if last_heartbeat <= heartbeat_grace_timeout:
                continue

            await self.close(
                ConnectionError(
                    'Server connection probably hang, last heartbeat '
                    'received %.3f seconds ago' % last_heartbeat
                )
            )

            return

    async def __receive_frame(self) -> typing.Tuple[int, int, spec.Frame]:
        async with self.lock:
            frame_header = await self.reader.readexactly(1)

            if frame_header == b'\0x00':
                raise spec.AMQPFrameError(await self.reader.read())

            frame_header += await self.reader.readexactly(6)

            if not self.started and frame_header.startswith(b'AMQP'):
                raise spec.AMQPSyntaxError
            else:
                self.started = True

            frame_type, _, frame_length = pamqp.frame.frame_parts(
                frame_header
            )

            frame_payload = await self.reader.readexactly(
                frame_length + 1
            )

        return pamqp.frame.unmarshal(frame_header + frame_payload)

    @staticmethod
    def __exception_by_code(frame: spec.Connection.Close):
        if frame.reply_code == 501:
            return exc.ConnectionFrameError(frame.reply_text)
        elif frame.reply_code == 502:
            return exc.ConnectionSyntaxError(frame.reply_text)
        elif frame.reply_code == 503:
            return exc.ConnectionCommandInvalid(frame.reply_text)
        elif frame.reply_code == 504:
            return exc.ConnectionChannelError(frame.reply_text)
        elif frame.reply_code == 505:
            return exc.ConnectionUnexpectedFrame(frame.reply_text)
        elif frame.reply_code == 506:
            return exc.ConnectionResourceError(frame.reply_text)
        elif frame.reply_code == 530:
            return exc.ConnectionNotAllowed(frame.reply_text)
        elif frame.reply_code == 540:
            return exc.ConnectionNotImplemented(frame.reply_text)
        elif frame.reply_code == 541:
            return exc.ConnectionInternalError(frame.reply_text)
        else:
            return exc.ConnectionClosed(frame.reply_code, frame.reply_text)

    @task
    async def __reader(self):
        try:
            while not self.reader.at_eof():
                weight, channel, frame = await self.__receive_frame()

                self.heartbeat_last_received = self.loop.time()

                if channel == 0:
                    if isinstance(frame, spec.Connection.Close):
                        return await self.close(self.__exception_by_code(frame))
                    elif isinstance(frame, Heartbeat):
                        continue

                    log.error('Unexpected frame %r', frame)
                    continue

                if self.channels.get(channel) is None:
                    log.exception(
                        "Got frame for closed channel %d: %r", channel, frame
                    )
                    continue

                ch = self.channels[channel]

                channel_close_responses = (
                    spec.Channel.Close,
                    spec.Channel.CloseOk
                )

                if isinstance(frame, channel_close_responses):
                    self.channels[channel] = None

                await ch.frames.put((weight, frame))
        except asyncio.CancelledError as e:
            log.debug("Reader task cancelled:", exc_info=e)
        except asyncio.IncompleteReadError as e:
            log.debug("Can not read bytes from server:", exc_info=e)
            await self.close(ConnectionError(*e.args))
        except Exception as e:
            log.debug("Reader task exited because:", exc_info=e)
            await self.close(e)

    async def _on_close(self, exc=exc.ConnectionClosed(0, 'normal closed')):
        writer = self.writer
        self.reader = None
        self.writer = None

        # noinspection PyShadowingNames
        writer.close()
        return await writer.wait_closed()

    @property
    def server_capabilities(self) -> ArgumentsType:
        return self.server_properties['capabilities']

    @property
    def basic_nack(self) -> bool:
        return self.server_capabilities.get('basic.nack')

    @property
    def consumer_cancel_notify(self) -> bool:
        return self.server_capabilities.get('consumer_cancel_notify')

    @property
    def exchange_exchange_bindings(self) -> bool:
        return self.server_capabilities.get('exchange_exchange_bindings')

    @property
    def publisher_confirms(self):
        return self.server_capabilities.get('publisher_confirms')

    async def channel(self, channel_number: int = None,
                      publisher_confirms=True,
                      frame_buffer=FRAME_BUFFER, **kwargs) -> Channel:

        await self.connected.wait()

        if self.is_closed:
            raise RuntimeError('%r closed' % self)

        if not self.publisher_confirms and publisher_confirms:
            raise ValueError("Server doesn't support publisher_confirms")

        if channel_number is None:
            async with self.last_channel_lock:
                while self.last_channel in self.channels.keys():
                    self.last_channel += 1

                    if self.last_channel > 65535:
                        log.warning("Resetting channel number for %r", self)
                        self.last_channel = 1
                        # switching context for prevent blocking event-loop
                        await asyncio.sleep(0)

                channel_number = self.last_channel
        elif channel_number in self.channels:
            raise ValueError("Channel %d already used" % channel_number)

        if channel_number < 0 or channel_number > 65535:
            raise ValueError('Channel number too large')

        channel = Channel(
            self, channel_number, frame_buffer=frame_buffer,
            publisher_confirms=publisher_confirms, **kwargs
        )

        self.channels[channel_number] = channel

        try:
            await channel.open()
        except Exception:
            self.channels[channel_number] = None
            raise

        return channel

    async def __aenter__(self):
        await self.connect()


async def connect(url, *args, **kwargs) -> Connection:
    connection = Connection(url, *args, **kwargs)

    try:
        await connection.connect()
    except Exception as e:
        await connection.close(e)
        raise

    return connection
