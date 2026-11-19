import asyncio

import pamqp.frame
from pamqp import commands as spec

from aiormq.connection import Connection, FrameReceiver


async def _deliver_frames(connection, *frames):
    reader = asyncio.StreamReader()
    reader.feed_data(
        b"".join(
            pamqp.frame.marshal(frame, 0)
            for frame in frames
        )
    )
    reader.feed_eof()

    await asyncio.wait_for(
        connection._Connection__reader(FrameReceiver(reader)),
        timeout=5,
    )


async def test_blocked_callbacks_receive_frames_and_isolate_errors(caplog):
    connection = Connection("amqp://guest:guest@localhost/?heartbeat=0")
    blocked_frame = spec.Connection.Blocked(reason="low on memory")
    unblocked_frame = spec.Connection.Unblocked()

    async_frames = asyncio.Queue()
    sync_frames = []
    unblocked_frames = []
    broken_called = asyncio.Event()
    sync_called = asyncio.Event()
    unblocked_called = asyncio.Event()

    async def on_blocked_async(frame):
        await async_frames.put(frame)

    def on_blocked_broken(frame):
        broken_called.set()
        raise RuntimeError("callback failure")

    def on_blocked_sync(frame):
        sync_frames.append(frame)
        sync_called.set()

    def on_unblocked(frame):
        unblocked_frames.append(frame)
        unblocked_called.set()

    connection.on_blocked_callbacks.update({
        on_blocked_async, on_blocked_broken, on_blocked_sync,
    })
    connection.on_unblocked_callbacks.add(on_unblocked)

    await _deliver_frames(connection, blocked_frame, unblocked_frame)
    received = await asyncio.wait_for(async_frames.get(), timeout=5)
    await asyncio.wait_for(broken_called.wait(), timeout=5)
    await asyncio.wait_for(sync_called.wait(), timeout=5)
    await asyncio.wait_for(unblocked_called.wait(), timeout=5)

    assert isinstance(received, spec.Connection.Blocked)
    assert received.reason == blocked_frame.reason
    assert len(sync_frames) == 1
    assert isinstance(sync_frames[0], spec.Connection.Blocked)
    assert sync_frames[0].reason == blocked_frame.reason
    assert len(unblocked_frames) == 1
    assert isinstance(unblocked_frames[0], spec.Connection.Unblocked)
    assert "Unhandled connection block callback exception" in caplog.text


async def test_slow_blocked_callback_does_not_delay_unblocked():
    connection = Connection("amqp://guest:guest@localhost/?heartbeat=0")
    blocked_started = asyncio.Event()
    blocked_done = asyncio.Event()
    unblocked_called = asyncio.Event()
    release_blocked = asyncio.Event()

    async def slow_blocked(frame):
        blocked_started.set()
        await release_blocked.wait()
        blocked_done.set()

    def on_unblocked(frame):
        unblocked_called.set()

    connection.on_blocked_callbacks.add(slow_blocked)
    connection.on_unblocked_callbacks.add(on_unblocked)

    try:
        await _deliver_frames(
            connection,
            spec.Connection.Blocked(reason="low on memory"),
            spec.Connection.Unblocked(),
        )
        await asyncio.wait_for(blocked_started.wait(), timeout=5)
        await asyncio.wait_for(unblocked_called.wait(), timeout=5)

        assert not blocked_done.is_set()
    finally:
        release_blocked.set()

    await asyncio.wait_for(blocked_done.wait(), timeout=5)
