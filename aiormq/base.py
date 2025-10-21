import abc
import asyncio
import logging
from contextlib import suppress
from functools import wraps
from typing import Any, Awaitable, Callable, Optional, TypeVar

from .abc import (
    AbstractBase,
    AbstractFutureStore,
    CoroutineType,
    ExceptionType,
    TaskType,
    TaskWrapper,
    TimeoutType,
)
from .tools import Countdown, shield

T = TypeVar("T")
log = logging.getLogger(__name__)


class FutureStore(AbstractFutureStore):
    __slots__ = "futures", "loop", "parent", "reject_future"

    futures: set[asyncio.Future | TaskType] | frozenset[Any]
    parent: Optional["FutureStore"]
    loop: asyncio.AbstractEventLoop

    def __init__(self, loop: asyncio.AbstractEventLoop):
        self.futures = set()
        self.loop = loop
        self.reject_future: asyncio.Future = loop.create_future()
        self.parent: FutureStore | None = None

    def add(self, future: asyncio.Future | TaskWrapper) -> None:
        if self.reject_future.done():
            if isinstance(future, TaskWrapper):
                future.throw(self.reject_future.exception() or Exception)
            elif isinstance(future, asyncio.Future):
                future.set_exception(self.reject_future.exception() or Exception)
            return

        if isinstance(self.futures, set):
            self.futures.add(future)
            # Link to set
            futures = self.futures
            future.add_done_callback(lambda *_: futures.discard(future))

        if self.parent:
            self.parent.add(future)

    @shield
    async def reject_all(self, exception: ExceptionType | None) -> None:
        if self.reject_future.done() or isinstance(self.futures, frozenset):
            log.info("FutureStore already rejected")
            return

        self.reject_future.set_exception(exception or Exception())

        tasks = []

        while self.futures:
            future: TaskType | asyncio.Future = self.futures.pop()

            if future.done():
                continue

            if isinstance(future, TaskWrapper):
                future.throw(exception or Exception)
                tasks.append(future)
            elif asyncio.isfuture(future):
                future.set_exception(exception or Exception)

        # forbid further additions
        self.futures = frozenset()

        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    def create_task(self, coro: CoroutineType) -> TaskType:
        task: TaskWrapper = TaskWrapper(self.loop.create_task(coro))
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
    __slots__ = "__future_store", "closing", "loop"

    def __init__(
        self,
        *,
        loop: asyncio.AbstractEventLoop,
        parent: AbstractBase | None = None,
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

    def _cancel_tasks(self, exc: ExceptionType | None = None) -> Awaitable[None]:
        return self.__future_store.reject_all(exc)

    def _future_store_child(self) -> AbstractFutureStore:
        return self.__future_store.get_child()

    def create_task(self, coro: CoroutineType) -> TaskType:
        return self.__future_store.create_task(coro)

    def create_future(self) -> asyncio.Future:
        return self.__future_store.create_future()

    @abc.abstractmethod
    async def _on_close(self, exc: ExceptionType | None = None) -> None:
        return

    async def __closer(self, exc: ExceptionType | None) -> None:
        if self.is_closed:
            return

        with suppress(Exception):
            await self._on_close(exc)

        with suppress(Exception):
            await self._cancel_tasks(exc)

    async def close(
        self,
        exc: ExceptionType | None = asyncio.CancelledError,
        timeout: TimeoutType = None,
    ) -> None:
        if self.is_closed:
            return None

        countdown = Countdown(timeout)
        await countdown(self.__closer(exc))

    def __repr__(self) -> str:
        cls_name = self.__class__.__name__
        return f'<{cls_name}: "{self!s}" at 0x{id(self):02x}>'

    @abc.abstractmethod
    def __str__(self) -> str:  # pragma: no cover
        raise NotImplementedError

    @property
    def is_closed(self) -> bool:
        return self.closing.done()


TaskFunctionType = Callable[..., T]


def task(closed_exception: ExceptionType | None = None) -> Callable[..., TaskFunctionType]:
    def decorator(func: TaskFunctionType) -> TaskFunctionType:
        @wraps(func)
        async def wrap(self: Base, *args: Any, **kwargs: Any) -> Any:
            if self.closing.done():
                raise (closed_exception or asyncio.CancelledError(f"{self.__class__.__name__} is closing or closed"))
            return await self.create_task(func(self, *args, **kwargs))

        return wrap

    return decorator
