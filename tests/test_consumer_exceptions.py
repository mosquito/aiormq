import asyncio
import gc
import logging

import aiomisc
import pytest

from aiormq.exceptions import AMQPConnectionError


@pytest.mark.parametrize(
    "outcome", ["error", "cancel", "success", "disconnect"],
)
@aiomisc.timeout(20)
async def test_consumer_exception_is_reported_once(
    proxy_connection, proxy, monkeypatch, caplog, outcome,
):
    connection = proxy_connection
    setup = await connection.channel()
    channel = await connection.channel()
    queue = await setup.queue_declare(exclusive=True)
    finished = asyncio.Event()
    entered = asyncio.Event()
    retained = []
    error = ValueError("consumer failed")
    create_task = channel.create_task

    def capture_task(coro):
        task = create_task(coro)
        if coro.__name__ == "callback":
            # Keep the task alive: error reporting must not depend on GC.
            retained.append(task)
            task.add_done_callback(lambda _: finished.set())
        return task

    async def callback(message):
        if outcome == "disconnect":
            entered.set()
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                raise error
        if outcome == "error":
            raise error
        if outcome == "cancel":
            raise asyncio.CancelledError
        await channel.basic_ack(message.delivery_tag)

    monkeypatch.setattr(channel, "create_task", capture_task)
    with caplog.at_level(logging.ERROR, logger="aiormq.channel"):
        consume = await channel.basic_consume(queue.queue, callback)
        await setup.basic_publish(b"input", routing_key=queue.queue)
        if outcome == "disconnect":
            await entered.wait()
            await proxy.disconnect_all()
            with pytest.raises(AMQPConnectionError):
                await connection.closing
        await finished.wait()
        if outcome != "disconnect":
            await setup.basic_qos(prefetch_count=1)
            assert not connection.is_closed
            assert not channel.is_closed
        records = [
            record for record in caplog.records
            if record.getMessage() == "Consumer callback failed"
        ]
        assert len(records) == (1 if outcome in ("error", "disconnect") else 0)
        if records:
            assert records[0].exc_info[1] is error
            # Reporting must not consume the exception for an explicit waiter.
            with pytest.raises(ValueError) as caught:
                await retained[0]
            assert caught.value is error
        retained.clear()
        gc.collect()
        if outcome != "disconnect":
            await channel.basic_cancel(consume.consumer_tag)
            await channel.close()
            message = await setup.basic_get(queue.queue, no_ack=True)
            if outcome == "success":
                assert message.delivery_tag is None
            else:
                assert message.body == b"input"
                assert message.delivery.redelivered
        assert sum(
            record.getMessage() == "Consumer callback failed"
            for record in caplog.records
        ) == len(records)
