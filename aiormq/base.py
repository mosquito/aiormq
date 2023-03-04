import asyncio
from typing import Awaitable, Optional, Set, TypeVar, Any, Coroutine

from .abc import (
    AbstractBase, CoroutineType, ExceptionType,
    TimeoutType,
)


T = TypeVar("T")


class Base(AbstractBase):
    __slots__ = "loop", "closing", "__tasks"

    def __init__(self, *, loop: asyncio.AbstractEventLoop):
        self.__tasks: Set[asyncio.Future] = set()
        self.loop: asyncio.AbstractEventLoop = loop or asyncio.get_event_loop()
        self.closing = self.create_future()

    def create_task(self, coro: CoroutineType) -> asyncio.Task:
        task = self.loop.create_task(coro)
        task.add_done_callback(self.__tasks.discard)
        self.__tasks.add(task)
        return task

    def create_future(self) -> asyncio.Future:
        future = self.loop.create_future()
        future.add_done_callback(self.__tasks.discard)
        self.__tasks.add(future)
        return future

    @property
    def is_closed(self) -> bool:
        return self.closing.done()

    def close(
        self, exc: Optional[BaseException] = asyncio.CancelledError,
        timeout: TimeoutType = None,
    ) -> Awaitable[Any]:
        exc = exc or RuntimeError("Closed")
        futures: Set[asyncio.Future] = set()

        for future in self.__tasks:
            futures.add(future)

            if future.done():
                continue
            elif isinstance(future, asyncio.Task):
                future.cancel()
            else:
                future.set_exception(exc)

        async def closer():
            nonlocal futures
            if not futures:
                return
            await asyncio.gather(*futures, return_exceptions=True)

        return self.loop.create_task(
            asyncio.wait_for(closer(), timeout=timeout)
        )
