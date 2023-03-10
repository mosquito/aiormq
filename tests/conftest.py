import asyncio
import gc
import logging
import os
import tracemalloc

import pamqp
import pytest
from aiomisc_pytest import TCPProxy
from yarl import URL

from aiormq import Connection


def cert_path(*args):
    return os.path.join(
        os.path.abspath(os.path.dirname(__file__)), "certs", *args,
    )


AMQP_URL = URL(os.getenv("AMQP_URL", "amqp://guest:guest@localhost/"))

amqp_urls = {
    "amqp": AMQP_URL,
    "amqp-named": AMQP_URL.update_query(name="pytest"),
    "amqps": AMQP_URL.with_scheme("amqps").with_query(
        {"cafile": cert_path("ca.pem"), "no_verify_ssl": 1},
    ),
    "amqps-client": AMQP_URL.with_scheme("amqps").with_query(
        {
            "cafile": cert_path("ca.pem"),
            "keyfile": cert_path("client.key"),
            "certfile": cert_path("client.pem"),
            "no_verify_ssl": 1,
        },
    ),
}


amqp_url_list, amqp_url_ids = [], []

for name, url in amqp_urls.items():
    amqp_url_list.append(url)
    amqp_url_ids.append(name)


@pytest.fixture(params=amqp_url_list, ids=amqp_url_ids)
async def amqp_url(request):
    return request.param


@pytest.fixture
async def amqp_connection(amqp_url, loop):
    connection = Connection(amqp_url, loop=loop)
    async with connection:
        yield connection


channel_params = [
    dict(channel_number=None, frame_buffer_size=10, publisher_confirms=True),
    dict(channel_number=None, frame_buffer_size=1, publisher_confirms=True),
    dict(channel_number=None, frame_buffer_size=10, publisher_confirms=False),
    dict(channel_number=None, frame_buffer_size=1, publisher_confirms=False),
]


@pytest.fixture(params=channel_params)
async def amqp_channel(request, amqp_connection):
    try:
        yield await amqp_connection.channel(**request.param)
    finally:
        await amqp_connection.close()


skip_when_quick_test = pytest.mark.skipif(
    os.getenv("TEST_QUICK") is not None, reason="quick test",
)


@pytest.fixture(autouse=True)
def memory_tracer():
    tracemalloc.start()
    tracemalloc.clear_traces()

    filters = (
        tracemalloc.Filter(True, pamqp.__file__),
        tracemalloc.Filter(True, asyncio.__file__),
    )

    snapshot_before = tracemalloc.take_snapshot().filter_traces(filters)

    def format_stat(stats):
        items = [
            "TOP STATS:",
            "%-90s %6s %6s %6s" % ("Traceback", "line", "size", "count"),
        ]

        for stat in stats:
            fname = stat.traceback[0].filename
            lineno = stat.traceback[0].lineno
            items.append(
                "%-90s %6s %6s %6s"
                % (fname, lineno, stat.size_diff, stat.count_diff),
            )

        return "\n".join(items)

    try:
        yield

        gc.collect()

        snapshot_after = tracemalloc.take_snapshot().filter_traces(filters)

        top_stats = snapshot_after.compare_to(
            snapshot_before, "lineno", cumulative=True,
        )

        if top_stats:
            logging.error(format_stat(top_stats))
            raise AssertionError("Possible memory leak")
    finally:
        tracemalloc.stop()


@pytest.fixture()
async def proxy(tcp_proxy, localhost, amqp_url: URL):
    port = amqp_url.port or 5672 if amqp_url.scheme == "amqp" else 5671
    async with tcp_proxy(amqp_url.host, port) as proxy:
        yield proxy


@pytest.fixture
async def proxy_connection(proxy: TCPProxy, amqp_url: URL, loop):
    url = amqp_url.with_host(
        "localhost",
    ).with_port(
        proxy.proxy_port,
    )
    connection = Connection(url, loop=loop)

    await connection.connect()

    try:
        yield connection
    finally:
        await connection.close()
