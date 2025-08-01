import asyncio
import platform
import time
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
            return await result

        return result               # type: ignore

    return wrap


def legacy_timeout(func: Callable[..., Coroutine[Any, Any, T]]
                   ) -> Callable[..., Coroutine[Any, Any, T]]:
    @wraps(func)
    async def wrapper(*args: Any, timeout: TimeoutType = None, **kwargs: Any
                      ) -> T:
        if timeout is None:
            return await func(*args, **kwargs)
        else:
            return await asyncio.wait_for(func(*args, **kwargs),
                                          timeout=timeout)
    return wrapper
