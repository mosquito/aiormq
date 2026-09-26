import asyncio
from unittest.mock import AsyncMock, Mock

import pytest
from pamqp.commands import Connection as ConnectionFrames
from pamqp.exceptions import UnmarshalingException
from pamqp.heartbeat import Heartbeat

from aiormq import AMQPError, InvalidFrameError, ProtocolSyntaxError
from aiormq.abc import TaskWrapper
from aiormq.connection import Connection, FrameReceiver


@pytest.mark.parametrize("payload", [
    b"\x09\x00\x00\x00\x00\x00\x01\x00\xce",  # Unknown frame type.
    b"\x03\x00\x01\x00\x00\x00\x01\x00\x00",  # Invalid terminator.
])
async def test_decode_error_belongs_to_aiormq_hierarchy(payload):
    reader = asyncio.StreamReader()
    reader.feed_data(payload)
    reader.feed_eof()

    with pytest.raises(AMQPError) as caught:
        await FrameReceiver(reader).get_frame()

    assert isinstance(caught.value, InvalidFrameError)
    assert isinstance(caught.value.__cause__, UnmarshalingException)
    assert str(caught.value.__cause__) in str(caught.value)


async def test_protocol_header_belongs_to_aiormq_hierarchy():
    reader = asyncio.StreamReader()
    reader.feed_data(b"AMQP\x00\x00\x09\x01")
    reader.feed_eof()

    with pytest.raises(AMQPError) as caught:
        await FrameReceiver(reader).get_frame()

    assert isinstance(caught.value, ProtocolSyntaxError)


async def test_unexpected_handshake_response_belongs_to_aiormq_hierarchy():
    writer = Mock(drain=AsyncMock())
    receiver = Mock(get_frame=AsyncMock(return_value=(8, 0, Heartbeat())))

    with pytest.raises(AMQPError) as caught:
        await Connection._rpc(ConnectionFrames.Open(), writer, receiver)

    assert isinstance(caught.value, InvalidFrameError)
    assert "Connection.OpenOk" in str(caught.value)


async def test_shutdown_preserves_protocol_error_cause():
    cause = UnmarshalingException("Unknown", "Invalid frame")
    try:
        raise InvalidFrameError(str(cause)) from cause
    except InvalidFrameError as error:
        reason = error

    task = TaskWrapper(asyncio.create_task(asyncio.sleep(10)))
    task.throw(reason)
    with pytest.raises(AMQPError) as caught:
        await task

    assert caught.value is reason
    assert caught.value.__cause__ is cause
