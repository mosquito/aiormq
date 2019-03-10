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
