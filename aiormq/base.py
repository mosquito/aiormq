import abc
import asyncio
from contextlib import suppress
from functools import wraps
from typing import Any, Callable, Coroutine, Optional, Set, Type, TypeVar, Union

from .tools import shield


T = TypeVar("T")
CoroutineType = Coroutine[Any, None, Any]


# noinspection PyShadowingNames
class TaskWrapper:
    def __init__(self, task: asyncio.Task):
        self.task = task
        self.exception = asyncio.CancelledError

    def throw(self, exception) -> None:
        self.exception = exception
        self.task.cancel()

    async def __inner(self) -> Any:
        try:
            return await self.task
        except asyncio.CancelledError as e:
            raise self.exception from e

    def __await__(self, *args, **kwargs) -> Any:
        return self.__inner().__await__()

    def cancel(self) -> None:
        return self.throw(asyncio.CancelledError)

    def __getattr__(self, item: str) -> Any:
        return getattr(self.task, item)

    def __repr__(self) -> str:
        return "<%s: %s>" % (self.__class__.__name__, repr(self.task))


TaskType = Union[asyncio.Task, TaskWrapper]


class FutureStore:
    __slots__ = "futures", "loop", "parent"

    futures: Set[Union[asyncio.Future, TaskType]]
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

    def create_future(self):
        future = self.loop.create_future()
        self.add(future)
        return future

    def get_child(self) -> "FutureStore":
        store = FutureStore(self.loop)
        store.parent = self
        return store


class Base:
    __slots__ = "loop", "__future_store", "closing"

    def __init__(self, *, loop, parent: "Base" = None):
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

    def _future_store_child(self):
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
        return '<{0}: "{1}">'.format(cls_name, str(self))

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
