import abc
import asyncio
from contextlib import suppress
from functools import wraps
from typing import Any, Callable, Optional, Set, Type, TypeVar, Union
from weakref import WeakSet

from .abc import (
    AbstractBase, AbstractFutureStore, CoroutineType, TaskType, TaskWrapper,
)
from .tools import shield


T = TypeVar("T")


class FutureStore(AbstractFutureStore):
    __slots__ = "futures", "loop", "parent"

    futures: Set[Union[asyncio.Future, TaskType]]
    weak_futures: WeakSet
    loop: asyncio.AbstractEventLoop

    def __init__(self, loop: asyncio.AbstractEventLoop):
        self.futures = set()
        self.loop = loop
        self.parent: Optional[FutureStore] = None

    def __on_task_done(self, future):
        def remover(*_):
            nonlocal future
            if future in self.futures:
                self.futures.remove(future)

        return remover

    def add(self, future: Union[asyncio.Future, TaskWrapper]):
        self.futures.add(future)
        future.add_done_callback(self.__on_task_done(future))

        if self.parent:
            self.parent.add(future)

    @shield
    async def reject_all(self, exception: Exception):
        tasks = []

        while self.futures:
            future: Union[TaskType, asyncio.Future] = self.futures.pop()

            if future.done():
                continue

            if isinstance(future, TaskWrapper):
                future.throw(exception or Exception)
                tasks.append(future)
            elif isinstance(future, asyncio.Future):
                future.set_exception(exception)

        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    def create_task(self, coro: CoroutineType) -> TaskType:
        task: TaskWrapper = TaskWrapper(self.loop.create_task(coro))
        self.add(task)
        return task

    def create_future(self, weak: bool = False):
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
        parent: Optional[AbstractBase] = None
    ):
        self.loop: asyncio.AbstractEventLoop = loop

        if parent:
            self.__future_store = parent._future_store_child()
        else:
            self.__future_store = FutureStore(loop=self.loop)

        self.closing = self._create_closing_future()

    def _create_closing_future(self):
        future = self.__future_store.create_future()
        future.add_done_callback(lambda x: x.exception())
        return future

    def _cancel_tasks(self, exc: Union[Exception, Type[Exception]] = None):
        return self.__future_store.reject_all(exc)

    def _future_store_child(self) -> AbstractFutureStore:
        return self.__future_store.get_child()

    def create_task(self, coro: CoroutineType) -> TaskType:
        return self.__future_store.create_task(coro)

    def create_future(self) -> asyncio.Future:
        return self.__future_store.create_future()

    @abc.abstractmethod
    async def _on_close(self, exc=None):  # pragma: no cover
        return

    async def __closer(self, exc):
        if self.is_closed:  # pragma: no cover
            return

        with suppress(Exception):
            await self._on_close(exc)

        with suppress(Exception):
            await self._cancel_tasks(exc)

    async def close(self, exc=asyncio.CancelledError()) -> None:
        if self.is_closed:
            return None

        await self.loop.create_task(self.__closer(exc))

    def __repr__(self):
        cls_name = self.__class__.__name__
        return '<{0}: "{1}" at 0x{2:02x}>'.format(
            cls_name, str(self), id(self),
        )

    @abc.abstractmethod
    def __str__(self):  # pragma: no cover
        raise NotImplementedError

    @property
    def is_closed(self):
        return self.closing.done()


TaskFunctionType = Callable[..., T]


def task(func: TaskFunctionType) -> TaskFunctionType:
    @wraps(func)
    async def wrap(self: Base, *args, **kwargs) -> Any:
        return await self.create_task(func(self, *args, **kwargs))

    return wrap
