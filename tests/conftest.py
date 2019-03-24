import asyncio
import gc
import logging
import os
import tracemalloc

import pamqp
import pytest

from async_generator import yield_, async_generator
from yarl import URL

import aiormq
from aiormq import Connection


POLICIES = [asyncio.DefaultEventLoopPolicy]
POLICY_IDS = ['asyncio']

try:
    import uvloop
    POLICIES.append(uvloop.EventLoopPolicy)
    POLICY_IDS.append('uvloop')
except ImportError:
    pass


@pytest.fixture(params=POLICIES, ids=POLICY_IDS)
def event_loop(request):
    policy = request.param()

    asyncio.get_event_loop().close()
    asyncio.set_event_loop_policy(policy)

    loop = asyncio.new_event_loop()     # type: asyncio.AbstractEventLoop
    loop.set_debug(True)
    asyncio.set_event_loop(loop)

    original = asyncio.get_event_loop

    def getter_mock():
        raise RuntimeError('asyncio.get_event_loop() call forbidden')

    asyncio.get_event_loop = getter_mock

    nocatch_marker = any(
        marker.name == 'no_catch_loop_exceptions'
        for marker in request.node.own_markers
    )

    exceptions = list()
    if not nocatch_marker:
        loop.set_exception_handler(lambda l, c: exceptions.append(c))

    try:
        yield loop

        if exceptions:
            raise RuntimeError(exceptions)

    finally:
        asyncio.get_event_loop = original
        asyncio.set_event_loop_policy(None)
        del policy

        loop.close()
        del loop


def cert_path(*args):
    return os.path.join(
        os.path.abspath(os.path.dirname(__file__)), 'certs', *args
    )


amqp_urls = [
    URL(os.getenv('AMQP_URL', 'amqp://guest:guest@localhost/')),
    URL(os.getenv(
        'AMQP_URL', 'amqp://guest:guest@localhost/'
    )).with_scheme('amqps').with_query({
        "cafile": cert_path('ca.pem'),
        "no_verify_ssl": 1,
    }),
    URL(os.getenv(
        'AMQP_URL', 'amqp://guest:guest@localhost/'
    )).with_scheme('amqps').with_query({
        "cafile": cert_path('ca.pem'),
        "keyfile": cert_path('client.key'),
        "certfile": cert_path('client.pem'),
        "no_verify_ssl": 1,
    })
]


@pytest.fixture(params=amqp_urls, ids=['amqp', 'amqps', 'amqps-client'])
@async_generator
async def amqp_connection(request, event_loop):
    connection = Connection(request.param, loop=event_loop)

    await connection.connect()

    try:
        await yield_(connection)
    finally:
        await connection.close()


channel_params = [
    dict(channel_number=None, frame_buffer=10, publisher_confirms=True),
    dict(channel_number=None, frame_buffer=1, publisher_confirms=True),
    dict(channel_number=None, frame_buffer=10, publisher_confirms=False),
    dict(channel_number=None, frame_buffer=1, publisher_confirms=False),
]


@pytest.fixture(params=channel_params)
@async_generator
async def amqp_channel(request, amqp_connection):
    try:
        await yield_(await amqp_connection.channel(**request.param))
    finally:
        await amqp_connection.close()


skip_when_quick_test = pytest.mark.skipif(
    os.getenv("TEST_QUICK") is not None,
    reason='quick test'
)


@pytest.fixture(autouse=True)
def memory_tracer():
    tracemalloc.start()
    tracemalloc.clear_traces()

    filters = (
        tracemalloc.Filter(True, aiormq.__file__),
        tracemalloc.Filter(True, pamqp.__file__),
        tracemalloc.Filter(True, asyncio.__file__),
    )

    snapshot_before = tracemalloc.take_snapshot().filter_traces(filters)

    def format_stat(stats):
        items = [
            "TOP STATS:",
            "%-90s %6s %6s %6s" % ("Traceback", "line", "size", "count")
        ]

        for stat in stats:
            fname = stat.traceback[0].filename
            lineno = stat.traceback[0].lineno
            items.append(
                "%-90s %6s %6s %6s" % (
                    fname,
                    lineno,
                    stat.size_diff,
                    stat.count_diff
                )
            )

        return "\n".join(items)

    try:
        yield

        gc.collect()

        snapshot_after = tracemalloc.take_snapshot().filter_traces(filters)

        top_stats = snapshot_after.compare_to(
            snapshot_before, 'lineno', cumulative=True
        )

        if top_stats:
            logging.error(format_stat(top_stats))
            raise AssertionError("Possible memory leak")
    finally:
        tracemalloc.stop()
