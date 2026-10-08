import asyncio

import pamqp.frame
import pytest
from pamqp import commands as spec

from aiormq.channel import Channel
from aiormq.connection import Connection, FrameReceiver
from aiormq.exceptions import ChannelClosed


@pytest.fixture
async def local_connection(event_loop):
    connection = Connection(
        "amqp://guest:guest@localhost/?heartbeat=0", loop=event_loop,
    )
    connection.connection_tune = spec.Connection.Tune(
        channel_max=10, frame_max=131072, heartbeat=0,
    )
    try:
        yield connection
    finally:
        await asyncio.wait_for(connection.close(), 2)


async def close_channel(channel, close_frame):
    if close_frame == "local":
        channel._close_locally()
    elif close_frame == "close":
        with pytest.raises(ChannelClosed):
            await channel._on_close_frame(spec.Channel.Close(
                reply_code=200, class_id=0, method_id=0,
            ))
    else:
        with pytest.raises(ChannelClosed):
            await channel._on_close_ok_frame(spec.Channel.CloseOk())


@pytest.mark.parametrize("close_frame", ["local", "close", "close-ok"])
@pytest.mark.parametrize("owner", ["self", "replacement", "absent"])
async def test_channel_cleanup_respects_slot_owner(
    local_connection, close_frame, owner,
):
    connection = local_connection
    old = Channel(connection, 1, publisher_confirms=False)
    replacement = Channel(connection, 1, publisher_confirms=False)
    connection.channels[1] = old
    if owner != "self":
        connection.channels.pop(1)
    if owner == "replacement":
        connection.channels[1] = replacement

    await close_channel(old, close_frame)

    assert connection.write_queue.qsize() == (close_frame == "close")
    if owner == "replacement":
        assert connection.channels.get(1) is replacement
    else:
        assert 1 not in connection.channels


@pytest.mark.parametrize("close_frame", [
    spec.Channel.Close(reply_code=200, class_id=0, method_id=0),
    spec.Channel.CloseOk(),
])
async def test_reader_routes_frames_after_channel_number_reuse(
    local_connection, close_frame,
):
    connection = local_connection
    old = Channel(connection, 1, publisher_confirms=False)
    connection.channels[1] = old
    delivery_started = asyncio.Event()
    release_delivery = asyncio.Event()

    class DelayedQueue(asyncio.Queue):
        async def put(self, frame):
            delivery_started.set()
            await release_delivery.wait()
            await super().put(frame)

    old.frames = DelayedQueue()
    stream = asyncio.StreamReader()
    stream.feed_data(pamqp.frame.marshal(close_frame, 1))
    reader = asyncio.create_task(
        connection._Connection__reader(FrameReceiver(stream)),
    )
    try:
        await asyncio.wait_for(delivery_started.wait(), 2)
        # The real connection reader already released the broker-closed slot.
        assert 1 not in connection.channels
        replacement = Channel(connection, 1, publisher_confirms=False)
        connection.channels[1] = replacement
        release_delivery.set()
        with pytest.raises(ChannelClosed):
            await asyncio.wait_for(old.closing, 2)

        assert connection.channels.get(1) is replacement
        stream.feed_data(pamqp.frame.marshal(spec.Basic.QosOk(), 1))
        response = await asyncio.wait_for(replacement.rpc_frames.get(), 2)
        assert isinstance(response, spec.Basic.QosOk)
    finally:
        release_delivery.set()
        reader.cancel()
        await asyncio.gather(reader, return_exceptions=True)


async def test_broker_close_ok_cleanup_keeps_reused_channel(
    amqp_connection, monkeypatch,
):
    connection = amqp_connection
    old = await connection.channel(timeout=5)
    handler_started = asyncio.Event()
    release_handler = asyncio.Event()
    on_close_ok = old._on_close_ok_frame

    async def delayed_close_ok(frame):
        handler_started.set()
        await release_handler.wait()
        await on_close_ok(frame)

    monkeypatch.setattr(old, "_on_close_ok_frame", delayed_close_ok)
    closing = asyncio.create_task(old.close())
    replacement = None
    try:
        await asyncio.wait_for(handler_started.wait(), 5)
        replacement = await connection.channel(
            channel_number=old.number, timeout=5,
        )
        release_handler.set()
        await asyncio.wait_for(closing, 5)

        assert connection.channels.get(old.number) is replacement
        await replacement.basic_qos(prefetch_count=1, timeout=5)
        assert not connection.is_closed
    finally:
        release_handler.set()
        await asyncio.wait_for(
            asyncio.gather(closing, return_exceptions=True), 5,
        )
        if replacement is not None:
            await replacement.close(timeout=5)
