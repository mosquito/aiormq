import abc
import asyncio
from collections.abc import Callable, Coroutine
from contextlib import suppress
from functools import wraps
from typing import Any, Literal, TypeVar
from weakref import WeakSet

from .abc import (
    AbstractBase, AbstractFutureStore, CoroutineType, ExceptionType, TaskType,
    TaskWrapper, TimeoutType,
)
from .tools import Countdown, shield


T = TypeVar("T")


def _retrieve_exception(future: asyncio.Future) -> None:
    if not future.cancelled():
        future.exception()


class FutureStore(AbstractFutureStore):
    __slots__ = "futures", "loop", "parent"

    futures: set[asyncio.Future | TaskType]
    weak_futures: WeakSet
    loop: asyncio.AbstractEventLoop

    def __init__(self, loop: asyncio.AbstractEventLoop):
        self.futures = set()
        self.loop = loop
        # False until reject_all() ran. After that every added future is
        # rejected at once with this reason, so no caller waits forever.
        self.reject_reason: ExceptionType | Literal[False] | None = False
        self.parent: FutureStore | None = None

    def __on_task_done(
        self, future: asyncio.Future | TaskWrapper,
    ) -> Callable[..., Any]:
        def remover(*_: Any) -> None:
            nonlocal future     # noqa
            if future in self.futures:
                self.futures.remove(future)

        return remover

    def add(self, future: asyncio.Future | TaskWrapper) -> None:
        if self.reject_reason is not False:
            if isinstance(future, TaskWrapper):
                future.throw(self.reject_reason or Exception)
            elif isinstance(future, asyncio.Future):
                future.set_exception(self.reject_reason or Exception)

        self.futures.add(future)
        future.add_done_callback(self.__on_task_done(future))

        if self.parent:
            self.parent.add(future)

    @shield
    async def reject_all(self, exception: ExceptionType | None) -> None:
        self.reject_reason = exception
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

        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    def create_task(self, coro: CoroutineType) -> TaskType:
        task: TaskWrapper = TaskWrapper(self.loop.create_task(coro))
        self.add(task)
        return task

    def create_future(self, weak: bool = False) -> asyncio.Future:
        future = self.loop.create_future()
        # A caller can stop waiting for the future before reject_all()
        # sets its exception, for example a publish that failed on the
        # drain future while its confirmation was still pending. Read the
        # exception once, so asyncio does not log "Future exception was
        # never retrieved" when the future is collected.
        future.add_done_callback(_retrieve_exception)
        self.add(future)
        return future

    def get_child(self) -> "FutureStore":
        store = FutureStore(self.loop)
        store.parent = self
        return store


class Base(AbstractBase):
    __slots__ = "loop", "__future_store", "_closing"

    def __init__(
        self, *, loop: asyncio.AbstractEventLoop,
        parent: AbstractBase | None = None,
    ):
        self.loop: asyncio.AbstractEventLoop = loop

        if parent:
            self.__future_store = parent._future_store_child()
        else:
            self.__future_store = FutureStore(loop=self.loop)

        self._closing = self._create_closing_future()

    def _create_closing_future(self) -> asyncio.Future:
        return self.__future_store.create_future()

    @property
    def closing(self) -> asyncio.Future:
        """Return an independent observer of closure while the resource is open.

        Cancelling this future only stops that observer. Use close() to
        shut down the resource.
        """
        if self._closing.done():
            return self._closing

        future = self.loop.create_future()

        def on_close(source: asyncio.Future) -> None:
            if future.done():
                return
            if source.cancelled():
                future.cancel()
            elif (exc := source.exception()) is not None:
                future.set_exception(exc)
            else:
                future.set_result(source.result())

        def on_done(observer: asyncio.Future) -> None:
            self._closing.remove_done_callback(on_close)
            if not observer.cancelled():
                observer.exception()

        self._closing.add_done_callback(on_close)
        future.add_done_callback(on_done)
        return future

    def _cancel_tasks(
        self, exc: ExceptionType | None = None,
    ) -> Coroutine[Any, Any, None]:
        return self.__future_store.reject_all(exc)

    def _future_store_child(self) -> AbstractFutureStore:
        return self.__future_store.get_child()

    def create_task(self, coro: CoroutineType) -> TaskType:
        return self.__future_store.create_task(coro)

    def create_future(self) -> asyncio.Future:
        return self.__future_store.create_future()

    @abc.abstractmethod
    async def _on_close(
        self, exc: ExceptionType | None = None,
    ) -> None:  # pragma: no cover
        return

    async def __closer(self, exc: ExceptionType | None) -> None:
        if self.is_closed:  # pragma: no cover
            return

        with suppress(Exception):
            await self._on_close(exc)

        with suppress(Exception):
            await self._cancel_tasks(exc)

    async def close(
        self, exc: ExceptionType | None = asyncio.CancelledError,
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
        return self._closing.done()


TaskFunctionType = Callable[..., T]


def task(func: TaskFunctionType) -> TaskFunctionType:
    @wraps(func)
    async def wrap(self: Base, *args: Any, **kwargs: Any) -> Any:
        return await self.create_task(func(self, *args, **kwargs))

    return wrap
