import uuid
from typing import Any, Callable, Coroutine, Dict, NamedTuple, Optional

import aiomisc
import pytest
from pamqp import commands as spec

from aiormq.abc import AbstractChannel, AbstractConnection


def unique(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex}"


class Targets(NamedTuple):
    queue: str
    source: str
    destination: str


async def make_targets(channel: AbstractChannel) -> Targets:
    """Declare the queue and the exchanges which the no-wait calls change."""
    queue = unique("nowait-queue")
    source = unique("nowait-source")
    destination = unique("nowait-destination")

    await channel.queue_declare(queue, exclusive=True)
    for exchange in (source, destination):
        await channel.exchange_declare(
            exchange, exchange_type="fanout", auto_delete=True,
        )
    await channel.exchange_bind(destination, source)
    await channel.queue_bind(queue, source)
    return Targets(queue=queue, source=source, destination=destination)


def make_call(
    channel: AbstractChannel, method: str, targets: Targets, nowait: bool,
) -> Callable[[], Coroutine[Any, Any, Any]]:
    calls: Dict[str, Callable[[], Coroutine[Any, Any, Any]]] = {
        "queue_declare": lambda: channel.queue_declare(
            unique("nowait-declare"), exclusive=True, nowait=nowait,
        ),
        "queue_bind": lambda: channel.queue_bind(
            targets.queue, targets.source, routing_key="key", nowait=nowait,
        ),
        "queue_purge": lambda: channel.queue_purge(
            targets.queue, nowait=nowait,
        ),
        "queue_delete": lambda: channel.queue_delete(
            targets.queue, nowait=nowait,
        ),
        "exchange_declare": lambda: channel.exchange_declare(
            unique("nowait-exchange"), auto_delete=True, nowait=nowait,
        ),
        "exchange_bind": lambda: channel.exchange_bind(
            targets.destination, targets.source,
            routing_key="key", nowait=nowait,
        ),
        "exchange_unbind": lambda: channel.exchange_unbind(
            targets.destination, targets.source, nowait=nowait,
        ),
        "exchange_delete": lambda: channel.exchange_delete(
            targets.destination, nowait=nowait,
        ),
    }
    return calls[method]


EXPECTED_RESPONSES: Dict[str, Any] = {
    "queue_declare": spec.Queue.DeclareOk,
    "queue_bind": spec.Queue.BindOk,
    "queue_purge": spec.Queue.PurgeOk,
    "queue_delete": spec.Queue.DeleteOk,
    "exchange_declare": spec.Exchange.DeclareOk,
    "exchange_bind": spec.Exchange.BindOk,
    "exchange_unbind": spec.Exchange.UnbindOk,
    "exchange_delete": spec.Exchange.DeleteOk,
}


@pytest.mark.parametrize("method", sorted(EXPECTED_RESPONSES))
@pytest.mark.parametrize("nowait", [False, True])
@aiomisc.timeout(20)
async def test_nowait_returns_without_reply(
    amqp_connection: AbstractConnection, method: str, nowait: bool,
) -> None:
    channel = await amqp_connection.channel()
    targets = await make_targets(channel)

    result = await make_call(channel, method, targets, nowait)()

    if nowait:
        # The broker sends no reply for a no-wait call.
        assert result is None
    else:
        assert isinstance(result, EXPECTED_RESPONSES[method])

    # The call must release the RPC lock and keep the channel usable.
    await channel.basic_qos(prefetch_count=1)
    assert not channel.is_closed
    assert not amqp_connection.is_closed


@pytest.mark.parametrize("nowait", [False, True])
@aiomisc.timeout(20)
async def test_basic_cancel_nowait(
    amqp_connection: AbstractConnection, nowait: bool,
) -> None:
    channel = await amqp_connection.channel()
    queue = unique("nowait-consume")
    await channel.queue_declare(queue, exclusive=True)

    async def callback(message: Any) -> None:  # pragma: no cover
        await message.channel.basic_ack(message.delivery_tag)

    first = await channel.basic_consume(queue, callback)
    result = await channel.basic_cancel(first.consumer_tag, nowait=nowait)

    if nowait:
        assert result is None
    else:
        assert isinstance(result, spec.Basic.CancelOk)

    # The channel forgets a cancelled consumer, with or without a reply.
    assert first.consumer_tag not in channel.consumers

    # A second cancel on the same channel must also complete.
    second = await channel.basic_consume(queue, callback)
    await channel.basic_cancel(second.consumer_tag, nowait=nowait)
    assert second.consumer_tag not in channel.consumers

    declare_ok = await channel.queue_declare(queue, passive=True)
    assert declare_ok.consumer_count == 0
    assert not channel.is_closed
    assert not amqp_connection.is_closed


@pytest.mark.parametrize("nowait", [False, True])
@aiomisc.timeout(20)
async def test_basic_recover_nowait(
    amqp_connection: AbstractConnection, nowait: bool,
) -> None:
    channel = await amqp_connection.channel()

    if nowait:
        # A no-wait recover sends Basic.RecoverAsync. AMQP 0-9-1 deprecates
        # that command, and the broker sends no reply for it.
        with pytest.warns(DeprecationWarning):
            result = await channel.basic_recover(requeue=True, nowait=True)
        assert result is None
    else:
        assert isinstance(
            await channel.basic_recover(requeue=True), spec.Basic.RecoverOk,
        )

    await channel.basic_qos(prefetch_count=1)
    assert not channel.is_closed
    assert not amqp_connection.is_closed


@pytest.mark.parametrize("nowait", [False, True])
@aiomisc.timeout(20)
async def test_confirm_delivery_nowait(
    amqp_connection: AbstractConnection, nowait: bool,
) -> None:
    channel = await amqp_connection.channel(publisher_confirms=False)

    result: Optional[Any] = await channel.confirm_delivery(nowait=nowait)

    if nowait:
        assert result is None
    else:
        assert isinstance(result, spec.Confirm.SelectOk)

    await channel.basic_qos(prefetch_count=1)
    assert not channel.is_closed
    assert not amqp_connection.is_closed
