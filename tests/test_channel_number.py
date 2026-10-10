import asyncio
from uuid import uuid4

import pytest

import aiormq


@pytest.mark.parametrize("owner", ["old", "new", "absent"])
async def test_local_channel_number_cleanup(
    event_loop: asyncio.AbstractEventLoop, owner: str,
) -> None:
    connection = aiormq.Connection(
        "amqp://guest:guest@localhost/", loop=event_loop,
    )
    connection.connection_tune = aiormq.spec.Connection.Tune(frame_max=131072)
    channel = aiormq.Channel(connection, number=1, publisher_confirms=False)
    replacement = object()
    if owner == "old":
        connection.channels[1] = channel
    elif owner == "new":
        connection.channels[1] = replacement

    try:
        channel._close_locally()

        if owner == "new":
            assert connection.channels[1] is replacement
        else:
            assert 1 not in connection.channels
    finally:
        connection.channels.clear()
        await channel.close()
        await connection.close()


async def test_close_ok_after_channel_number_reuse(
    amqp_connection: aiormq.Connection, monkeypatch: pytest.MonkeyPatch,
) -> None:
    channel = await amqp_connection.channel()
    received = asyncio.Event()
    resume = asyncio.Event()
    handled = asyncio.Event()
    original = channel._on_close_ok_frame

    async def delayed_close_ok(frame: aiormq.spec.Channel.CloseOk) -> None:
        received.set()
        await resume.wait()
        try:
            await original(frame)
        finally:
            handled.set()

    monkeypatch.setattr(channel, "_on_close_ok_frame", delayed_close_ok)
    closing = asyncio.create_task(channel.close())
    try:
        await asyncio.wait_for(received.wait(), timeout=5)
        assert channel.number not in amqp_connection.channels
        other = await amqp_connection.channel(
            channel_number=channel.number, timeout=5,
        )
        resume.set()
        await asyncio.wait_for(handled.wait(), timeout=5)
        await asyncio.wait_for(closing, timeout=5)
        assert amqp_connection.channels[channel.number] is other
        await other.queue_declare(auto_delete=True, timeout=5)
        await other.close()
        assert not amqp_connection.is_closed
    finally:
        resume.set()
        await asyncio.wait_for(closing, timeout=5)


async def test_channel_number_reuse_after_broker_close(
    amqp_connection: aiormq.Connection, monkeypatch: pytest.MonkeyPatch,
) -> None:
    channel = await amqp_connection.channel()
    received = asyncio.Event()
    resume = asyncio.Event()
    original = channel._on_close_frame

    async def delayed_close(frame: aiormq.spec.Channel.Close) -> None:
        received.set()
        await resume.wait()
        await original(frame)

    monkeypatch.setattr(channel, "_on_close_frame", delayed_close)
    declaration = asyncio.create_task(
        channel.queue_declare(str(uuid4()), passive=True, timeout=5),
    )
    try:
        await asyncio.wait_for(received.wait(), timeout=5)
        assert channel.number not in amqp_connection.channels
    finally:
        resume.set()
        with pytest.raises(aiormq.exceptions.ChannelNotFoundEntity):
            await asyncio.wait_for(declaration, timeout=5)

    other = await amqp_connection.channel(
        channel_number=channel.number, timeout=5,
    )
    await other.queue_declare(auto_delete=True, timeout=5)
    await other.close()
    assert not amqp_connection.is_closed


async def test_concurrent_channel_number_reuse(
    amqp_connection: aiormq.Connection,
) -> None:
    persistent = await amqp_connection.channel()

    async def churn() -> None:
        for _ in range(50):
            await asyncio.sleep(0)
            channel = await amqp_connection.channel(timeout=5)
            await channel.close(timeout=5)

    await asyncio.wait_for(
        asyncio.gather(*(churn() for _ in range(10))), timeout=20,
    )
    assert amqp_connection.channels == {persistent.number: persistent}
    await persistent.queue_declare(auto_delete=True, timeout=5)
    assert not amqp_connection.is_closed
