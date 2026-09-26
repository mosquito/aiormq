import asyncio
import logging

import aiomisc
import pytest
from pamqp.commands import Basic


@pytest.mark.parametrize("failure", ["cancel", "timeout"])
@aiomisc.timeout(15)
async def test_late_confirm_after_cancel(
    amqp_connection, monkeypatch, caplog, failure,
):
    channel = await amqp_connection.channel()
    queue = await channel.queue_declare(exclusive=True)
    received = asyncio.get_running_loop().create_future()
    on_confirm = channel._confirm_delivery

    def hold_confirm(delivery_tag, frame):
        received.set_result(frame)

    with monkeypatch.context() as patch:
        patch.setattr(channel, "_confirm_delivery", hold_confirm)
        publish = asyncio.create_task(channel.basic_publish(
            b"sent", routing_key=queue.queue, wait=False,
            timeout=0.2 if failure == "timeout" else None,
        ))
        frame = await received
        confirmation = channel.confirmations[1]
        if failure == "cancel":
            publish.cancel()
        with pytest.raises(
            TimeoutError if failure == "timeout" else asyncio.CancelledError,
        ):
            await publish

    assert confirmation.cancelled()
    with caplog.at_level(logging.DEBUG, logger="aiormq.channel"):
        # Release the actual broker confirmation after its waiter has gone.
        on_confirm(frame.delivery_tag, frame)
        await asyncio.sleep(0)
    assert not channel.confirmations
    ignored = [r for r in caplog.records if "was ignored" in r.getMessage()]
    assert len(ignored) == 1
    assert ignored[0].levelno == logging.DEBUG

    result = await channel.basic_publish(b"next", routing_key=queue.queue)
    assert isinstance(result, Basic.Ack)
    assert result.delivery_tag == 2
    for body in (b"sent", b"next"):
        message = await channel.basic_get(queue.queue, no_ack=True)
        assert message.body == body


@pytest.mark.parametrize("cancelled", [False, True])
async def test_multiple_confirm_with_completed_waiter(
    amqp_connection, caplog, cancelled,
):
    channel = await amqp_connection.channel()
    completed = channel.create_future()
    pending = channel.create_future()
    channel.confirmations.update({1: completed, 2: pending})
    if cancelled:
        completed.cancel()
    else:
        completed.set_result(Basic.Ack(delivery_tag=1))
    frame = Basic.Ack(delivery_tag=2, multiple=True)
    with caplog.at_level(logging.DEBUG, logger="aiormq.channel"):
        await channel._on_confirm_frame(frame)
        assert await pending is frame
    assert not channel.confirmations
    ignored = [r for r in caplog.records if "was ignored" in r.getMessage()]
    assert len(ignored) == 1
    assert ignored[0].levelno == (
        logging.DEBUG if cancelled else logging.WARNING
    )
