import asyncio
import sys
from functools import wraps

from yarl import URL


def censor_url(url: URL):
    if url.password is not None:
        return url.with_password('******')
    return url


def shield(func):
    async def awaiter(future):
        return await future

    @wraps(func)
    def wrap(*args, **kwargs):
        return wraps(func)(awaiter)(asyncio.shield(func(*args, **kwargs)))

    return wrap


class LazyCoroutine:
    __slots__ = '__func', '__args', '__kwargs'

    def __init__(self, func, *args, **kwargs):
        self.__func = func
        self.__args = args
        self.__kwargs = kwargs

    def __repr__(self):
        return "<%s: %s(args: %r, kwargs: %r)>" % (
            self.__class__.__name__, self.__func.__name__,
            self.__args, self.__kwargs
        )

    def __call__(self):
        return self.__func(*self.__args, **self.__kwargs)

    def __iter__(self):
        return (yield from self().__iter__())

    if sys.version_info >= (3, 7):
        def __await__(self):
            return (yield from self().__await__())
    else:
        def __await__(self):
            return (yield from self().__iter__())
