from asyncio import (streams, transports, get_event_loop, events,
                     AbstractEventLoop, StreamReaderProtocol, BaseProtocol)
import asyncio
from typing import Dict, Optional, Any, Tuple, Callable

import aiohttp
from yarl import URL

import logging

from aiormq.abc import URLorStr
from aiohttp.client_ws import ClientWebSocketResponse

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

    def __init__(self,
                 loop: AbstractEventLoop,
                 protocol: StreamReaderProtocol,
                 url: URLorStr,
                 extra: Optional[Dict] = None) -> None:

        super().__init__(extra)
        self.url = url

        self._loop = loop
        self._protocol = protocol
        self._closing = False  # Set when close() or write_eof() called.
        self._paused = False

        self.task = self._loop.create_task(self.main_loop())

        self.write_queue: asyncio.Queue = asyncio.Queue()
        self.read_queue: asyncio.Queue = asyncio.Queue()

    async def sender(self, ws: ClientWebSocketResponse) -> None:
        while True:
            data = await self.write_queue.get()
            await ws.send_bytes(data)

    async def receiver(self, ws: ClientWebSocketResponse) -> None:
        async for msg in ws:
            if msg.type == aiohttp.WSMsgType.BINARY:
                self._protocol.data_received(msg.data)
            elif msg.type == aiohttp.WSMsgType.ERROR:
                break

        self._protocol.eof_received()

    async def main_loop(self) -> None:
        async with aiohttp.ClientSession() as session:
            async with session.ws_connect(self.url) as ws:
                await asyncio.gather(
                    self.sender(ws),
                    self.receiver(ws)
                )

    def get_protocol(self) -> BaseProtocol:
        return self._protocol

    def set_protocol(self,  # type: ignore[override]
                     protocol: StreamReaderProtocol) -> None:
        self._protocol = protocol

    def is_closing(self) -> bool:
        return self._closing

    def write(self, data: Any) -> None:
        self.write_queue.put_nowait(data)

    def is_reading(self) -> bool:
        return not self._paused and not self._closing

    def resume_reading(self) -> None:
        if self._closing or not self._paused:
            return
        self._paused = False
        if self._loop.get_debug():
            logger.debug("%r resumes reading", self)

    def close(self) -> None:
        self._closing = True
        self.task.cancel()
        self._protocol.connection_lost(None)
        # self._protocol._closed.set_result(None)


async def create_websocket_connection(protocol_factory: Callable,
                                      url: URLorStr,
                                      **kwargs: Dict
                                      ) -> Tuple[WebsocketTransport,
                                                 StreamReaderProtocol]:
    loop = get_event_loop()
    protocol = protocol_factory()
    transport = WebsocketTransport(loop, protocol, url, **kwargs)
    return transport, protocol


_DEFAULT_LIMIT = 2 ** 16  # 64 KiB


async def open_websocket_connection(url: URL,
                                    limit: int = _DEFAULT_LIMIT,
                                    **kwargs: Dict
                                    ) -> Tuple[streams.StreamReader,
                                               streams.StreamWriter]:
    loop = events.get_running_loop()
    reader = streams.StreamReader(limit=limit, loop=loop)
    protocol = streams.StreamReaderProtocol(reader, loop=loop)

    def factory() -> StreamReaderProtocol:
        return protocol

    transport, _ = await create_websocket_connection(
        factory,
        url,
        **kwargs
    )
    writer = streams.StreamWriter(transport, protocol, reader, loop)
    return reader, writer
