import asyncio
import atexit
import gc
import logging
import os
import socket
import tracemalloc
from contextlib import suppress
from time import sleep
from typing import Any, Callable, Generator

import pamqp
import pytest
from aiomisc_pytest import TCPProxy
from yarl import URL

from aiormq import Connection

from .docker_client import (
    ContainerInfo,
    DockerClient,
    DockerHostInfo,
    DockerNotAvailableError,
    check_docker_available,
)

# Cached docker host info from pytest_configure
_docker_host_info: DockerHostInfo | None = None

# Global registry for atexit cleanup
_docker_client: DockerClient | None = None
_docker_containers: set[str] = set()


def _atexit_kill_containers() -> None:
    """Kill all containers on exit (handles crashes/interrupts)."""
    if _docker_client is None:
        return
    for container_id in _docker_containers:
        with suppress(Exception):
            _docker_client.kill(container_id)
        with suppress(Exception):
            _docker_client.remove(container_id)
    _docker_containers.clear()


atexit.register(_atexit_kill_containers)


def cert_path(*args):
    return os.path.join(
        os.path.abspath(os.path.dirname(__file__)), "certs", *args,
    )


def pytest_configure(config: pytest.Config) -> None:
    """Check Docker availability before running tests."""
    global _docker_host_info
    if os.environ.get("AMQP_URL"):
        return
    try:
        _docker_host_info = check_docker_available()
    except DockerNotAvailableError as e:
        raise pytest.UsageError(str(e)) from e


@pytest.fixture(scope="session")
def docker() -> Generator[Callable[..., ContainerInfo], Any, Any]:
    global _docker_client
    _docker_client = DockerClient(_docker_host_info)

    def docker_run(
        image: str, ports: list[str],
        environment: dict[str, str] | None = None,
    ) -> ContainerInfo:
        info = _docker_client.run(image, ports, environment=environment)
        _docker_containers.add(info.id)
        return info

    try:
        yield docker_run
    finally:
        for container_id in list(_docker_containers):
            with suppress(Exception):
                _docker_client.kill(container_id)
            with suppress(Exception):
                _docker_client.remove(container_id)
            _docker_containers.discard(container_id)


@pytest.fixture(scope="session")
def rabbitmq_container(docker) -> ContainerInfo:
    amqp_url = os.environ.get("AMQP_URL")
    if amqp_url:
        url = URL(amqp_url)
        return ContainerInfo(
            id="ci-service",
            ports={
                "5672/tcp": url.port or 5672,
                "5671/tcp": 5671,
                "15672/tcp": 15672,
                "15671/tcp": 15671,
            },
            host=url.host or "localhost",
        )
    info = docker(
        "mosquito/aiormq-rabbitmq",
        ["5672/tcp", "5671/tcp", "15672/tcp", "15671/tcp"],
    )
    # Readiness probe - wait for RabbitMQ to be ready
    while True:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            try:
                sock.connect((info.host, info.ports["5672/tcp"]))
                sock.send(b"AMQP\x00\x00\x09\x01")
                data = sock.recv(4)
                if len(data) == 4:
                    return info
            except ConnectionError:
                pass
        sleep(0.3)


@pytest.fixture(scope="session")
def amqp_direct_url(rabbitmq_container: ContainerInfo) -> URL:
    return URL.build(
        scheme="amqp", user="guest", password="guest", path="//",
        host=rabbitmq_container.host,
        port=rabbitmq_container.ports["5672/tcp"],
    )


amqp_url_ids = ["amqp", "amqp-named", "amqps", "amqps-client"]


@pytest.fixture(params=amqp_url_ids, ids=amqp_url_ids)
async def amqp_url(request, amqp_direct_url, rabbitmq_container):
    urls = {
        "amqp": amqp_direct_url,
        "amqp-named": amqp_direct_url.update_query(name="pytest"),
        "amqps": amqp_direct_url.with_scheme("amqps").with_port(
            rabbitmq_container.ports["5671/tcp"],
        ).with_query(
            {"cafile": cert_path("ca.pem"), "no_verify_ssl": 1},
        ),
        "amqps-client": amqp_direct_url.with_scheme("amqps").with_port(
            rabbitmq_container.ports["5671/tcp"],
        ).with_query(
            {
                "cafile": cert_path("ca.pem"),
                "keyfile": cert_path("client.key"),
                "certfile": cert_path("client.pem"),
                "no_verify_ssl": 1,
            },
        ),
    }
    return urls[request.param]


@pytest.fixture
async def amqp_connection(amqp_url, event_loop):
    connection = Connection(amqp_url, loop=event_loop)
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
async def proxy_connection(proxy: TCPProxy, amqp_url: URL, event_loop):
    url = amqp_url.with_host(
        "localhost",
    ).with_port(
        proxy.proxy_port,
    )
    connection = Connection(url, loop=event_loop)

    await connection.connect()

    try:
        yield connection
    finally:
        await connection.close()
