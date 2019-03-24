from aiormq.tools import LazyCoroutine


async def test_lazy_coroutine_simple():
    async def foo():
        return 42

    bar = LazyCoroutine(foo)

    assert await bar == 42
