"""Session fixtures shared by tests/ and README.md.

The RabbitMQ broker comes from AMQP_URL when it is set (CI service
container). Otherwise a docker container is started for the session.
"""

import asyncio
import atexit
import os
import socket
from contextlib import suppress
from time import monotonic, sleep
from typing import Any, Awaitable, Callable, Generator

import pytest
from yarl import URL

from tests.docker_client import (
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


def wait_for_broker(host: str, port: int, timeout: float = 60.0) -> None:
    """Block until the broker answers the AMQP protocol header."""
    deadline = monotonic() + timeout
    while True:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.settimeout(5)
            try:
                sock.connect((host, port))
                sock.send(b"AMQP\x00\x00\x09\x01")
                if len(sock.recv(4)) == 4:
                    return
            except OSError:
                pass
        if monotonic() > deadline:
            pytest.fail(f"RabbitMQ at {host}:{port} is not ready")
        sleep(0.3)


@pytest.fixture(scope="session")
def rabbitmq_container(
    docker: Callable[..., ContainerInfo],
) -> ContainerInfo:
    amqp_url = os.environ.get("AMQP_URL")
    if amqp_url:
        url = URL(amqp_url)
        info = ContainerInfo(
            id="ci-service",
            ports={
                "5672/tcp": url.port or 5672,
                "5671/tcp": 5671,
                "15672/tcp": 15672,
                "15671/tcp": 15671,
            },
            host=url.host or "localhost",
        )
    else:
        info = docker(
            "mosquito/aiormq-rabbitmq",
            ["5672/tcp", "5671/tcp", "15672/tcp", "15671/tcp"],
        )
    # A CI service container can accept TCP before the broker listens.
    wait_for_broker(info.host, info.ports["5672/tcp"])
    return info


@pytest.fixture(scope="session")
def amqp_direct_url(rabbitmq_container: ContainerInfo) -> URL:
    return URL.build(
        scheme="amqp", user="guest", password="guest", path="//",
        host=rabbitmq_container.host,
        port=rabbitmq_container.ports["5672/tcp"],
    )


@pytest.fixture(scope="session")
def amqp_url(amqp_direct_url: URL) -> str:
    """Broker URL for README.md examples. tests/ override this fixture."""
    return str(amqp_direct_url)


@pytest.fixture
def wait_for_output(
    capsys: pytest.CaptureFixture[str],
) -> Callable[..., Awaitable[None]]:
    """Return a coroutine function that waits for text in captured stdout.

    README.md consumer examples print from callbacks. The hidden test
    blocks use this helper to wait for the callback instead of a sleep.
    """
    async def wait(text: str, timeout: float = 10.0) -> None:
        deadline = monotonic() + timeout
        output = ""
        while text not in output:
            output += capsys.readouterr().out
            if monotonic() > deadline:
                pytest.fail(f"{text!r} not found in output: {output!r}")
            await asyncio.sleep(0.05)

    return wait
