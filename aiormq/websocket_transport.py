from asyncio import streams, transports, get_event_loop, events
import asyncio

import aiohttp
from yarl import URL

import logging

logging.basicConfig(level=logging.DEBUG)

# Create a custom logger
logger = logging.getLogger(__name__)

# Create handlers
c_handler = logging.StreamHandler()
c_handler.setLevel(logging.WARNING)

# Create formatters and add it to handlers
c_format = logging.Formatter('%(name)s - %(levelname)s - %(message)s')
c_handler.setFormatter(c_format)

# Add handlers to the logger
logger.addHandler(c_handler)



class WebsocketTransport(transports.Transport):

    def __init__(self, loop, protocol, url, extra=None):

        if extra is None:
            extra = {}
        self._extra = extra

        self.url = url

        self._loop = loop
        self._protocol = protocol
        self._closing = False  # Set when close() or write_eof() called.
        self._paused = False

        self.task = self._loop.create_task(self.main_loop())

        self.write_queue = asyncio.Queue()
        self.read_queue = asyncio.Queue()
        self.ws = None

    async def sender(self, ws):
        while True:
            data = await self.write_queue.get()
            await ws.send_bytes(data)

    async def receiver(self, ws):
        async for msg in ws:
            if msg.type == aiohttp.WSMsgType.BINARY:
                self._protocol.data_received(msg.data)
            elif msg.type == aiohttp.WSMsgType.ERROR:
                break

        self._protocol.eof_received()

    async def main_loop(self):
        async with aiohttp.ClientSession() as session:
            async with session.ws_connect(self.url) as ws:
                self.ws = ws
                await asyncio.gather(
                    self.sender(ws),
                    self.receiver(ws)
                )

    def get_protocol(self):
        return self._protocol

    def set_protocol(self, protocol):
        return self._protocol

    def is_closing(self):
        return self._closing

    def write(self, data):
        self.write_queue.put_nowait(data)

    def is_reading(self):
        return not self._paused and not self._closing

    def resume_reading(self):
        if self._closing or not self._paused:
            return
        self._paused = False
        if self._loop.get_debug():
            logger.debug("%r resumes reading", self)

    def close(self) -> None:
        self._closing = True
        self.task.cancel()
        self._protocol._closed.set_result(None)


async def create_websocket_connection(protocol_factory, url, *args, **kwargs):
    loop = get_event_loop()
    protocol = protocol_factory()
    transport = WebsocketTransport(loop, protocol, url, **kwargs)
    return transport, protocol


_DEFAULT_LIMIT = 2 ** 16  # 64 KiB


async def open_websocket_connection(url: URL, limit=_DEFAULT_LIMIT, *args, **kwargs):
    loop = events.get_running_loop()
    reader = streams.StreamReader(limit=limit, loop=loop)
    protocol = streams.StreamReaderProtocol(reader, loop=loop)
    factory = lambda: protocol
    transport, _ = await create_websocket_connection(factory, url, *args, **kwargs)
    writer = streams.StreamWriter(transport, protocol, reader, loop)
    return reader, writer
