import abc
import asyncio
import typing
from contextlib import suppress
from functools import wraps
from typing import TypeVar, Type, Union

from .tools import shield


T = TypeVar("T")


class TaskWrapper:
    def __init__(self, task: asyncio.Task):
        self.task = task
        self.exception = asyncio.CancelledError

    def throw(self, exception):
        self.exception = exception
        return self.task.cancel()

    async def __inner(self):
        try:
            return await self.task
        except asyncio.CancelledError as e:
            raise self.exception from e

    def __await__(self, *args, **kwargs):
        return self.__inner().__await__()

    def cancel(self):
        return self.throw(asyncio.CancelledError)

    def __getattr__(self, item):
        return getattr(self.task, item)

    def __repr__(self):
        return "<%s: %s>" % (self.__class__.__name__, repr(self.task))


class FutureStore:
    __slots__ = "futures", "important_futures", "loop", "parent"

    def __init__(self, loop: asyncio.AbstractEventLoop):
        self.futures = set()  # type: typing.Set[TaskWrapper]
        self.important_futures = set()  # type: typing.Set[TaskWrapper]
        self.loop = loop  # type: asyncio.AbstractEventLoop
        self.parent = None  # type: FutureStore

    def __on_task_done(self, future):
        def remover(*_):
            nonlocal future
            for future_set in (self.futures, self.important_futures):
                if future in future_set:
                    future_set.remove(future)

        return remover

    def add(self,
            future: typing.Union[asyncio.Future, TaskWrapper],
            important: bool):
        if important:
            self.important_futures.add(future)
        else:
            self.futures.add(future)
        future.add_done_callback(self.__on_task_done(future))

        if self.parent:
            self.parent.add(future, important)

    @shield
    async def reject(self,
                     exception: Exception,
                     all_jobs: bool,
                     important: bool = False):
        tasks = []
        if all_jobs:
            future_set = self.important_futures.union(self.futures)
        else:
            future_set = self.important_futures if important else self.futures

        while future_set:
            future = future_set.pop()  # type: TaskWrapper

            if future.done():
                continue

            if isinstance(future, TaskWrapper):
                future.throw(exception or Exception)
                tasks.append(future)
            elif isinstance(future, asyncio.Future):
                future.set_exception(exception)

        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    def create_task(self, coro: T, important: bool = False) -> T:
        task = TaskWrapper(self.loop.create_task(coro))
        self.add(task, important)
        return task

    def create_future(self, important: bool = False):
        future = self.loop.create_future()
        self.add(future, important)
        return future

    def get_child(self) -> "FutureStore":
        store = FutureStore(self.loop)
        store.parent = self
        return store


class Base:
    __slots__ = "loop", "__future_store", "closing"

    def __init__(self, *, loop, parent: "Base" = None):
        self.loop = loop  # type: asyncio.AbstractEventLoop

        if parent:
            self.__future_store = parent._future_store_child()
        else:
            self.__future_store = FutureStore(loop=self.loop)

        self.closing = self._create_closing_future()

    def _create_closing_future(self):
        future = self.__future_store.create_future(True)
        future.add_done_callback(lambda x: x.exception())
        return future

    def _cancel_tasks(self,
                      exc: Union[Exception, Type[Exception]] = None,
                      all_tasks: bool = True,
                      important: bool = False):
        if all_tasks:
            return self.__future_store.reject(exc, all_jobs=True)
        return self.__future_store.reject(
            exc,
            all_jobs=False,
            important=important
        )

    def _future_store_child(self):
        return self.__future_store.get_child()

    # noinspection PyShadowingNames
    def create_task(self, coro, important: bool = False) -> asyncio.Future:
        return self.__future_store.create_task(coro, important)

    def create_future(self, important: bool = False) -> asyncio.Future:
        return self.__future_store.create_future(important)

    @abc.abstractmethod
    async def _on_close(self, exc=None):  # pragma: no cover
        return

    async def __closer(self, exc):
        if self.is_closed:  # pragma: no cover
            return

        with suppress(Exception):
            await self._cancel_tasks(exc, all_tasks=False, important=False)

        with suppress(Exception):
            await self._on_close(exc)

        with suppress(Exception):
            await self._cancel_tasks(exc, all_tasks=False, important=True)

    async def close(self, exc=asyncio.CancelledError()):
        if self.is_closed:
            return

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


def task(important: bool = False):
    def wrapper(func: T) -> T:
        @wraps(func)
        async def wrap(self: "Base", *args, **kwargs):
            # noinspection PyCallingNonCallable
            return await self.create_task(
                func(self, *args, **kwargs),
                important
            )
        return wrap
    return wrapper
