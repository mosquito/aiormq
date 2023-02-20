import abc
import asyncio
from contextlib import suppress
from functools import wraps
from typing import Any, Awaitable, Callable, Optional, Set, TypeVar
from weakref import WeakSet

from .abc import (
    AbstractBase, AbstractFutureStore, CoroutineType, ExceptionType,
    TimeoutType,
)
from .tools import Countdown


T = TypeVar("T")


class FutureStore(AbstractFutureStore):
    __slots__ = (
        "futures", "loop", "parent", "__rejecting",
    )

    futures: Set[asyncio.Future]
    weak_futures: WeakSet
    loop: asyncio.AbstractEventLoop

    def __init__(self, loop: asyncio.AbstractEventLoop):
        self.futures = set()
        self.loop = loop
        self.parent: Optional[FutureStore] = None
        self.__rejecting: Optional[ExceptionType] = None

    def add(self, future: asyncio.Future) -> None:
        self.futures.add(future)
        future.add_done_callback(self.futures.discard)
        if self.parent:
            self.parent.add(future)

    def reject_all(self, exception: Optional[ExceptionType]) -> Awaitable[None]:
        self.__rejecting = exception or RuntimeError("Has been rejected")

        tasks = []

        while self.futures:
            future: asyncio.Future = self.futures.pop()

            tasks.append(future)

            if future.done():
                continue
            elif isinstance(future, asyncio.Task):
                future.cancel()
            elif isinstance(future, asyncio.Future):
                future.set_exception(self.__rejecting)

        async def wait_rejected() -> None:
            nonlocal tasks
            try:
                if not tasks:
                    return
                await asyncio.gather(*tasks, return_exceptions=True)
            finally:
                self.__rejecting = None
        return self.loop.create_task(wait_rejected())

    async def __task_wrapper(self, coro: CoroutineType) -> Any:
        try:
            return await coro
        except asyncio.CancelledError as e:
            if self.__rejecting is None:
                raise
            raise self.__rejecting from e

    def create_task(self, coro: CoroutineType) -> asyncio.Task:
        task: asyncio.Task = self.loop.create_task(self.__task_wrapper(coro))
        self.add(task)
        return task

    def create_future(self, weak: bool = False) -> asyncio.Future:
        future = self.loop.create_future()
        self.add(future)
        return future

    def get_child(self) -> "FutureStore":
        store = FutureStore(self.loop)
        store.parent = self
        return store


class Base(AbstractBase):
    __slots__ = "loop", "__future_store", "closing"

    def __init__(
        self, *, loop: asyncio.AbstractEventLoop,
        parent: Optional[AbstractBase] = None,
    ):
        self.loop: asyncio.AbstractEventLoop = loop

        if parent:
            self.__future_store = parent._future_store_child()
        else:
            self.__future_store = FutureStore(loop=self.loop)

        self.closing = self._create_closing_future()

    def _create_closing_future(self) -> asyncio.Future:
        future = self.__future_store.create_future()
        future.add_done_callback(lambda x: x.exception())
        return future

    def _cancel_tasks(
        self, exc: Optional[ExceptionType] = None,
    ) -> Awaitable[None]:
        return self.__future_store.reject_all(exc)

    def _future_store_child(self) -> AbstractFutureStore:
        return self.__future_store.get_child()

    def create_task(self, coro: CoroutineType) -> asyncio.Task:
        return self.__future_store.create_task(coro)

    def create_future(self) -> asyncio.Future:
        return self.__future_store.create_future()

    @abc.abstractmethod
    async def _on_close(
        self, exc: Optional[ExceptionType] = None,
    ) -> None:  # pragma: no cover
        return

    async def __closer(self, exc: Optional[ExceptionType]) -> None:
        if self.is_closed:  # pragma: no cover
            return

        with suppress(Exception):
            await self._on_close(exc)

        with suppress(Exception):
            await self._cancel_tasks(exc)

    async def close(
        self, exc: Optional[ExceptionType] = asyncio.CancelledError,
        timeout: TimeoutType = None,
    ) -> None:
        if self.is_closed:
            return None

        countdown = Countdown(timeout)
        await countdown(self.__closer(exc))

    def __repr__(self) -> str:
        cls_name = self.__class__.__name__
        return '<{0}: "{1}" at 0x{2:02x}>'.format(
            cls_name, str(self), id(self),
        )

    @abc.abstractmethod
    def __str__(self) -> str:  # pragma: no cover
        raise NotImplementedError

    @property
    def is_closed(self) -> bool:
        return self.closing.done()


TaskFunctionType = Callable[..., T]


def task(func: TaskFunctionType) -> TaskFunctionType:
    @wraps(func)
    async def wrap(self: Base, *args: Any, **kwargs: Any) -> Any:
        return await self.create_task(func(self, *args, **kwargs))

    return wrap
