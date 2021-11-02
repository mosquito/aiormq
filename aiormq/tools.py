import asyncio
from functools import wraps
from types import TracebackType
from typing import (
    Any, AsyncContextManager, Awaitable, Callable, Coroutine, Optional, Type,
    TypeVar, Union,
)

from yarl import URL

from aiormq.abc import TimeoutType


T = TypeVar("T")


def censor_url(url: URL) -> URL:
    if url.password is not None:
        return url.with_password("******")
    return url


def shield(func: Callable[..., Awaitable[T]]) -> Callable[..., Awaitable[T]]:
    @wraps(func)
    def wrap(*args: Any, **kwargs: Any) -> Awaitable[T]:
        return asyncio.shield(func(*args, **kwargs))

    return wrap


def awaitable(
    func: Callable[..., Union[T, Awaitable[T]]],
) -> Callable[..., Coroutine[Any, Any, T]]:
    # Avoid python 3.8+ warning
    if asyncio.iscoroutinefunction(func):
        return func     # type: ignore

    @wraps(func)
    async def wrap(*args: Any, **kwargs: Any) -> T:
        result = func(*args, **kwargs)

        if hasattr(result, "__await__"):
            return await result     # type: ignore
        if asyncio.iscoroutine(result) or asyncio.isfuture(result):
            return await result     # type: ignore

        return result               # type: ignore

    return wrap


class Countdown:
    __slots__ = "loop", "deadline"

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

    def __aenter__(self) -> Awaitable[T]:
        if self.countdown.deadline is None:
            return self.ctx.__aenter__()
        return self.countdown(self.ctx.__aenter__())

    def __aexit__(
        self, exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException], exc_tb: Optional[TracebackType],
    ) -> Awaitable[Any]:
        if self.countdown.deadline is None:
            return self.ctx.__aexit__(exc_type, exc_val, exc_tb)

        return self.countdown(self.ctx.__aexit__(exc_type, exc_val, exc_tb))
