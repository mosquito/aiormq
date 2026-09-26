import pytest
from pamqp.commands import Basic
from pamqp.header import ContentHeader

from aiormq.abc import DeliveredMessage
from aiormq.exceptions import DeliveryError, PublishError


@pytest.mark.parametrize("frame, details", [
    (Basic.Nack(delivery_tag=42, multiple=True),
     ("Basic.Nack", "delivery_tag=42", "multiple=True")),
    (Basic.Reject(delivery_tag=7, requeue=False),
     ("Basic.Reject", "delivery_tag=7", "requeue=False")),
    (Basic.Return(reply_code=312, reply_text="NO_ROUTE", routing_key="missing"),
     ("Basic.Return", "reply_code=312", "NO_ROUTE", "missing")),
])
def test_delivery_error_details_preserve_attributes(frame, details):
    error = DeliveryError(None, frame)
    assert error.args == (None, frame)
    assert error.message is None
    assert error.frame is frame
    for detail in details:
        assert detail in str(error)
        assert detail in repr(error)
    assert "object at" not in str(error)


def test_publish_error_details_preserve_attributes():
    frame = Basic.Return(
        reply_code=312, reply_text="NO_ROUTE", exchange="events",
        routing_key="missing",
    )
    message = DeliveredMessage(
        delivery=frame, header=ContentHeader(), body=b"private payload",
        channel=None,
    )
    error = PublishError(message, frame)
    assert error.args == ("NO_ROUTE", "missing")
    assert error.message is message
    assert error.frame is frame
    for detail in ("Basic.Return", "reply_code=312", "NO_ROUTE", "events"):
        assert detail in str(error)
    assert "PublishError" in repr(error)
    assert "private payload" not in repr(error)


async def test_broker_nack_details(amqp_connection):
    channel = await amqp_connection.channel()
    queue = await channel.queue_declare(
        exclusive=True,
        arguments={"x-max-length": 1, "x-overflow": "reject-publish"},
    )
    await channel.basic_publish(b"accepted", routing_key=queue.queue)
    with pytest.raises(DeliveryError) as caught:
        await channel.basic_publish(b"rejected", routing_key=queue.queue)
    error = caught.value
    assert isinstance(error.frame, Basic.Nack)
    assert error.frame.delivery_tag == 2
    assert "Basic.Nack" in str(error)
    assert "delivery_tag=2" in str(error)
    assert error.args == (None, error.frame)
    message = await channel.basic_get(queue.queue, no_ack=True)
    assert message.body == b"accepted"
