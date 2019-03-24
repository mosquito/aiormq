import asyncio
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


async def _test():
    pass


HAS_AWAIT_METHOD = hasattr(_test(), '__await__')


class LazyCoroutine:
    __slots__ = '__func', '__args', '__kwargs', '__instance'

    def __init__(self, func, *args, **kwargs):
        self.__func = func
        self.__args = args
        self.__kwargs = kwargs
        self.__instance = None

    def __repr__(self):
        return "<%s: %s(args: %r, kwargs: %r)>" % (
            self.__class__.__name__, self.__func.__name__,
            self.__args, self.__kwargs
        )

    def __call__(self):
        if self.__instance is None:
            self.__instance = self.__func(*self.__args, **self.__kwargs)

        return self.__instance

    def __iter__(self):
        return (yield from self().__iter__())

    if HAS_AWAIT_METHOD:
        def __await__(self):
            return (yield from self().__await__())
    else:
        def __await__(self):
            return (yield from self().__iter__())
