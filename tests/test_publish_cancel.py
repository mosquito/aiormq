import asyncio
import uuid
from dataclasses import replace

import aiomisc
import pamqp.frame
import pytest

import aiormq


@pytest.mark.parametrize("failure", ["cancel", "timeout", "marshal"])
@pytest.mark.parametrize("confirms", [False, True])
@pytest.mark.parametrize("wait", [False, True])
@aiomisc.timeout(15)
async def test_publish_failure_before_send(
    amqp_connection, monkeypatch, failure, confirms, wait,
):
    channel = await amqp_connection.channel(publisher_confirms=confirms)
    queue = await channel.queue_declare(exclusive=True)
    original_queue = channel.write_queue
    entered = asyncio.Event()
    futures = []
    create_future = channel.create_future

    def record_future():
        future = create_future()
        futures.append(future)
        return future

    class BlockingQueue:
        async def put(self, item):
            entered.set()
            await asyncio.Event().wait()

    marshal = pamqp.frame.marshal

    def broken_marshal(frame, *args, **kwargs):
        if isinstance(frame, aiormq.spec.Basic.Publish):
            raise ValueError("Cannot encode publish")
        return marshal(frame, *args, **kwargs)

    with monkeypatch.context() as patch:
        patch.setattr(channel, "create_future", record_future)
        if failure == "marshal":
            patch.setattr(pamqp.frame, "marshal", broken_marshal)
        else:
            patch.setattr(channel, "write_queue", BlockingQueue())
        publish = asyncio.create_task(channel.basic_publish(
            b"not sent", routing_key=queue.queue,
            properties=aiormq.spec.Basic.Properties(message_id="retry"),
            wait=wait, timeout=0.1 if failure == "timeout" else None,
        ))
        if failure != "marshal":
            await entered.wait()
        if failure == "cancel":
            publish.cancel()
        error = {
            "cancel": asyncio.CancelledError,
            "timeout": TimeoutError,
            "marshal": ValueError,
        }[failure]
        with pytest.raises(error):
            await publish

    assert channel.write_queue is original_queue
    assert channel.delivery_tag == 0
    assert not channel.confirmations
    assert not channel.message_id_delivery_tag
    assert all(future.cancelled() for future in futures)

    # Reusing the message ID must not let an old cleanup callback remove
    # the new publication's mapping. Return handling needs this mapping.
    if confirms:
        with pytest.raises(aiormq.exceptions.PublishError):
            await channel.basic_publish(
                b"returned", routing_key=uuid.uuid4().hex, mandatory=True,
                properties=aiormq.spec.Basic.Properties(message_id="retry"),
                timeout=2,
            )

    result = await channel.basic_publish(
        b"next", routing_key=queue.queue, timeout=2,
    )
    if confirms:
        assert isinstance(result, aiormq.spec.Basic.Ack)
        assert result.delivery_tag == channel.delivery_tag
    message = await channel.basic_get(queue.queue, no_ack=True, timeout=2)
    assert message.body == b"next"
    await channel.close()


@pytest.mark.parametrize(
    "failure", ["cancel", "timeout", "cancel_at_enqueue"],
)
@aiomisc.timeout(15)
async def test_publish_failure_after_enqueue(
    amqp_connection, monkeypatch, failure,
):
    channel = await amqp_connection.channel()
    queue = await channel.queue_declare(exclusive=True)
    original_queue = channel.write_queue
    enqueued = asyncio.Event()

    class DelayedDrainQueue:
        async def put(self, item):
            # Deliver the frame to the real broker, but leave the caller
            # waiting for drain, after its sequence number has been used.
            await original_queue.put(replace(item, drain_future=None))
            enqueued.set()
            if failure == "cancel_at_enqueue":
                publish.cancel()

    with monkeypatch.context() as patch:
        patch.setattr(channel, "write_queue", DelayedDrainQueue())
        publish = asyncio.create_task(channel.basic_publish(
            b"sent", routing_key=queue.queue,
            timeout=0.1 if failure == "timeout" else 5,
        ))
        await enqueued.wait()
        if failure == "cancel":
            publish.cancel()
        with pytest.raises(
            TimeoutError if failure == "timeout" else asyncio.CancelledError,
        ):
            await publish

    assert channel.delivery_tag == 1
    result = await channel.basic_publish(
        b"next", routing_key=queue.queue, timeout=2,
    )
    assert result.delivery_tag == 2
    for body in (b"sent", b"next"):
        message = await channel.basic_get(queue.queue, no_ack=True, timeout=2)
        assert message.body == body
    await channel.close()
