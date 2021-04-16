import asyncio
from functools import wraps
from typing import AsyncContextManager, Awaitable, TypeVar

from yarl import URL

from aiormq.abc import TimeoutType


T = TypeVar("T")


def censor_url(url: URL):
    if url.password is not None:
        return url.with_password("******")
    return url


def shield(func):
    @wraps(func)
    def wrap(*args, **kwargs):
        return asyncio.shield(awaitable(func)(*args, **kwargs))

    return wrap


def awaitable(func):
    # Avoid python 3.8+ warning
    if asyncio.iscoroutinefunction(func):
        return func

    @wraps(func)
    async def wrap(*args, **kwargs):
        result = func(*args, **kwargs)

        if hasattr(result, "__await__"):
            return await result
        if asyncio.iscoroutine(result) or asyncio.isfuture(result):
            return await result

        return result

    return wrap


def _inspect_await_method():
    async def _test():
        pass

    coro = _test()
    method_await = getattr(coro, "__await__", None)
    method_iter = getattr(coro, "__iter__", None)

    for _ in (method_await or method_iter)():
        pass

    return bool(method_await)


HAS_AWAIT_METHOD = _inspect_await_method()


class Countdown:
    def __init__(self, timeout: TimeoutType = None):
        self.loop: asyncio.AbstractEventLoop = asyncio.get_event_loop()
        self.deadline: TimeoutType = None

        if timeout is not None:
            self.deadline = self.loop.time() + timeout

    def get_timeout(self) -> TimeoutType:
        if self.deadline is None:
            return None

        current = self.loop.time()
        if current >= self.deadline:
            raise asyncio.TimeoutError

        return self.deadline - current

    def __call__(self, coro: Awaitable[T]) -> Awaitable[T]:
        if self.deadline is None:
            return coro
        return asyncio.wait_for(coro, timeout=self.get_timeout())

    def enter_context(
        self, ctx: AsyncContextManager[T],
    ) -> AsyncContextManager[T]:
        return CountdownContext(self, ctx)


class CountdownContext(AsyncContextManager):
    def __init__(self, countdown: Countdown, ctx: AsyncContextManager):
        self.countdown = countdown
        self.ctx = ctx

    def __aenter__(self):
        if self.countdown.deadline is None:
            return self.ctx.__aenter__()
        return self.countdown(self.ctx.__aenter__())

    def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.countdown.deadline is None:
            return self.ctx.__aexit__(exc_type, exc_val, exc_tb)

        return self.countdown(self.ctx.__aexit__(exc_type, exc_val, exc_tb))
