import asyncio
import logging
import os
from binascii import hexlify
from collections import OrderedDict
from contextlib import suppress
from functools import partial
from io import BytesIO
from typing import Any, Dict, Optional, Union

import pamqp.frame
from pamqp import ContentHeader
from pamqp import specification as spec
from pamqp.body import ContentBody

from aiormq.tools import LazyCoroutine, awaitable

from .base import Base, task
from .exceptions import (
    ChannelAccessRefused, ChannelClosed, ChannelInvalidStateError,
    ChannelLockedResource, ChannelNotFoundEntity, ChannelPreconditionFailed,
    DeliveryError, DuplicateConsumerTag, InvalidFrameError,
    MethodNotImplemented, PublishError,
)
from .types import (
    ArgumentsType, ConfirmationFrameType, ConsumerCallback, DeliveredMessage,
    DrainResult, FrameType, RpcReturnType, TimeoutType,
)


log = logging.getLogger(__name__)


class Channel(Base):
    # noinspection PyTypeChecker
    CONTENT_FRAME_SIZE = len(pamqp.frame.marshal(ContentBody(b""), 0))
    Returning = object()

    def __init__(
        self,
        connector,
        number,
        publisher_confirms=True,
        frame_buffer=None,
        on_return_raises=True,
    ):

        super().__init__(loop=connector.loop, parent=connector)

        self.connection = connector

        if (
            publisher_confirms and not connector.publisher_confirms
        ):  # pragma: no cover
            raise ValueError("Server does't support publisher confirms")

        self.consumers = {}
        self.confirmations = OrderedDict()
        self.message_id_delivery_tag = dict()

        self.delivery_tag = 0

        self.getter = None  # type: Optional[asyncio.Future]
        self.getter_lock = asyncio.Lock()

        self.frames = asyncio.Queue(maxsize=frame_buffer)

        self.max_content_size = (
            connector.connection_tune.frame_max - self.CONTENT_FRAME_SIZE
        )

        self.__lock = asyncio.Lock()
        self.number = number
        self.publisher_confirms = publisher_confirms
        self.rpc_frames = asyncio.Queue(maxsize=frame_buffer)
        self.writer = connector.writer
        self.on_return_raises = on_return_raises
        self.on_return_callbacks = set()
        self._close_exception = None

        self.create_task(self._reader())
        self.closing.add_done_callback(self.__clean_up_when_writer_close)

    def __clean_up_when_writer_close(self, _):
        self.writer = None

    @property
    def lock(self) -> asyncio.Lock:
        if self.is_closed:
            raise ChannelInvalidStateError("%r closed" % self)

        return self.__lock

    async def _get_frame(self) -> FrameType:
        weight, frame = await self.frames.get()
        self.frames.task_done()
        return frame

    def __str__(self):
        return str(self.number)

    @task
    async def rpc(
        self, frame: spec.Frame,
        timeout: TimeoutType = None
    ) -> RpcReturnType:

        deadline = None
        if timeout is not None:
            deadline = self.loop.time() + timeout

        def get_exceeded(default: TimeoutType = None):
            if deadline is None:
                return None
            value = deadline - self.loop.time()
            if value < 0:
                return default
            return value

        if self.writer is None:
            raise ChannelInvalidStateError("writer is None")

        lock = self.lock

        try:
            await asyncio.wait_for(lock.acquire(), timeout=get_exceeded(0))
        except asyncio.TimeoutError as e:
            raise asyncio.TimeoutError("Unable to lock channel") from e

        try:
            self.writer.write(pamqp.frame.marshal(frame, self.number))

            if not (frame.synchronous or getattr(frame, "nowait", False)):
                return None

            result = await asyncio.wait_for(
                self.rpc_frames.get(), get_exceeded(0),
            )

            self.rpc_frames.task_done()

            if result.name not in frame.valid_responses:  # pragma: no cover
                raise InvalidFrameError(frame)

            return result
        except (asyncio.CancelledError, asyncio.TimeoutError):
            if not self.is_closed:
                log.warning(
                    "Closing channel %r because RPC call %s cancelled",
                    self, frame,
                )
                writer = self.writer
                self.writer = None

                writer.write(
                    pamqp.frame.marshal(
                        spec.Channel.Close(
                            504, "RPC timeout on frame {!s}".format(frame),
                        ),
                        self.number,
                    ),
                )

                # The close method will close all channel related tasks
                # include current task, so I have to suppress
                # ``CancelledError`` for current task.
                with suppress(asyncio.CancelledError):
                    await self.close()
            raise
        finally:
            lock.release()

    async def open(self):
        frame = await self.rpc(spec.Channel.Open())

        if self.publisher_confirms:
            await self.rpc(spec.Confirm.Select())

        if frame is None:  # pragma: no cover
            raise spec.AMQPFrameError(frame)

    async def _read_content(self, frame, header: ContentHeader):
        body = BytesIO()

        content = None

        if header.body_size:
            content = await self._get_frame()  # type: Optional[ContentBody]

        while content and body.tell() < header.body_size:
            body.write(content.value)

            if body.tell() < header.body_size:
                content = await self._get_frame()

        return DeliveredMessage(
            delivery=frame,
            header=header,
            body=body.getvalue(),
            channel=self,
        )

    @staticmethod
    def __exception_by_code(frame: spec.Channel.Close):  # pragma: nocover
        if frame.reply_code == 403:
            return ChannelAccessRefused(frame.reply_text)
        elif frame.reply_code == 404:
            return ChannelNotFoundEntity(frame.reply_text)
        elif frame.reply_code == 405:
            return ChannelLockedResource(frame.reply_text)
        elif frame.reply_code == 406:
            return ChannelPreconditionFailed(frame.reply_text)
        else:
            return ChannelClosed(frame.reply_code, frame.reply_text)

    async def _on_deliver(self, frame: spec.Basic.Deliver):
        # async with self.lock:
        header = await self._get_frame()  # type: ContentHeader
        message = await self._read_content(frame, header)

        consumer = self.consumers.get(frame.consumer_tag)
        if consumer is not None:
            # noinspection PyAsyncCall
            self.create_task(consumer(message))

    async def _on_get(
        self, frame: Union[spec.Basic.GetOk, spec.Basic.GetEmpty]
    ):
        message = None
        if isinstance(frame, spec.Basic.GetOk):
            header = await self._get_frame()  # type: ContentHeader
            message = await self._read_content(frame, header)

        if self.getter is None:
            raise RuntimeError("Getter is None")

        if self.getter.done():
            log.error("Got message but no active getter")
            return

        return self.getter.set_result((frame, message))

    async def _on_return(self, frame: spec.Basic.Return):
        header = await self._get_frame()  # type: ContentHeader
        message = await self._read_content(frame, header)
        message_id = message.header.properties.message_id

        delivery_tag = self.message_id_delivery_tag.get(message_id)

        if delivery_tag is None:  # pragma: nocover
            log.error("Unhandled message %r returning", message)
            return

        confirmation = self.confirmations.pop(delivery_tag, None)
        if confirmation is None:  # pragma: nocover
            return

        self.confirmations[delivery_tag] = self.Returning

        if self.on_return_raises:
            confirmation.set_exception(PublishError(message, frame))
            return

        for cb in self.on_return_callbacks:
            # noinspection PyBroadException
            try:
                cb(message)
            except Exception:
                log.exception("Unhandled return callback exception")

        confirmation.set_result(message)

    def _confirm_delivery(self, delivery_tag, frame: ConfirmationFrameType):
        if delivery_tag not in self.confirmations:
            return

        confirmation = self.confirmations.pop(delivery_tag)

        if confirmation is self.Returning:
            return
        elif confirmation.done():  # pragma: nocover
            log.warning(
                "Delivery tag %r confirmed %r was ignored", delivery_tag, frame,
            )
            return
        elif isinstance(frame, spec.Basic.Ack):
            confirmation.set_result(frame)
            return

        confirmation.set_exception(
            DeliveryError(None, frame),
        )  # pragma: nocover

    async def _on_confirm(self, frame: ConfirmationFrameType):
        if not self.publisher_confirms:  # pragma: nocover
            return

        if frame.delivery_tag not in self.confirmations:
            log.error("Unexpected confirmation frame %r from broker", frame)
            return

        multiple = getattr(frame, "multiple", False)

        if multiple:
            for delivery_tag in self.confirmations.keys():
                if frame.delivery_tag >= delivery_tag:
                    # Should be called later to avoid keys copying
                    self.loop.call_soon(
                        self._confirm_delivery, delivery_tag, frame,
                    )
        else:
            self._confirm_delivery(frame.delivery_tag, frame)

    async def _reader(self):
        while True:
            try:
                frame = await self._get_frame()

                if isinstance(frame, spec.Basic.Deliver):
                    with suppress(Exception):
                        await self._on_deliver(frame)
                    continue
                elif isinstance(
                    frame, (spec.Basic.GetOk, spec.Basic.GetEmpty),
                ):
                    with suppress(Exception):
                        await self._on_get(frame)
                elif isinstance(frame, spec.Basic.Return):
                    with suppress(Exception):
                        await self._on_return(frame)
                    continue
                elif isinstance(frame, spec.Basic.Cancel):
                    self.consumers.pop(frame.consumer_tag, None)
                    continue
                elif isinstance(frame, spec.Basic.CancelOk):
                    self.consumers.pop(frame.consumer_tag, None)
                elif isinstance(
                    frame, (spec.Basic.Ack, spec.Basic.Nack, spec.Basic.Reject),
                ):
                    with suppress(Exception):
                        await self._on_confirm(frame)
                    continue
                elif isinstance(frame, spec.Channel.Close):
                    exc = self.__exception_by_code(frame)
                    self.writer.write(
                        pamqp.frame.marshal(
                            spec.Channel.CloseOk(), self.number,
                        ),
                    )

                    self.connection.channels.pop(self.number, None)
                    return await self._cancel_tasks(exc)

                await self.rpc_frames.put(frame)
            except asyncio.CancelledError:
                return
            except Exception as e:  # pragma: nocover
                log.debug("Channel reader exception %r", exc_info=e)
                await self._cancel_tasks(e)
                raise

    async def _on_close(self, exc=None) -> Optional[spec.Channel.CloseOk]:
        result = None
        if self.writer is not None:
            result = await self.rpc(
                spec.Channel.Close(reply_code=spec.REPLY_SUCCESS),
            )
            self.connection.channels.pop(self.number, None)

        return result

    async def basic_get(
        self, queue: str = "", no_ack: bool = False,
        timeout: Union[int, float] = None
    ) -> DeliveredMessage:

        async with self.getter_lock:
            self.getter = self.create_future()
            await self.rpc(
                spec.Basic.Get(queue=queue, no_ack=no_ack), timeout=timeout,
            )

            if self.getter is None:
                raise RuntimeError("Getter is None")

            frame, message = await self.getter
            self.getter = None

        return message

    async def basic_cancel(
        self, consumer_tag, *, nowait: bool = False,
        timeout: Union[int, float] = None
    ) -> spec.Basic.CancelOk:
        return await self.rpc(
            spec.Basic.Cancel(consumer_tag=consumer_tag, nowait=nowait),
            timeout=timeout,
        )

    async def basic_consume(
        self,
        queue: str,
        consumer_callback: ConsumerCallback,
        *,
        no_ack: bool = False,
        exclusive: bool = False,
        arguments: ArgumentsType = None,
        consumer_tag: str = None,
        timeout: Union[int, float] = None
    ) -> spec.Basic.ConsumeOk:

        consumer_tag = consumer_tag or "ctag%i.%s" % (
            self.number,
            hexlify(os.urandom(16)).decode(),
        )

        if consumer_tag in self.consumers:
            raise DuplicateConsumerTag(self.number)

        self.consumers[consumer_tag] = awaitable(consumer_callback)

        return await self.rpc(
            spec.Basic.Consume(
                queue=queue,
                no_ack=no_ack,
                exclusive=exclusive,
                consumer_tag=consumer_tag,
                arguments=arguments,
            ),
            timeout=timeout,
        )

    def basic_ack(
        self, delivery_tag, multiple=False,
    ) -> DrainResult:
        self.writer.write(
            pamqp.frame.marshal(
                spec.Basic.Ack(delivery_tag=delivery_tag, multiple=multiple),
                self.number,
            ),
        )

        return LazyCoroutine(self.connection.drain)

    def basic_nack(
        self,
        delivery_tag: str = None,
        multiple: bool = False,
        requeue: bool = True,
    ) -> DrainResult:
        if not self.connection.basic_nack:
            raise MethodNotImplemented

        self.writer.write(
            pamqp.frame.marshal(
                spec.Basic.Nack(
                    delivery_tag=delivery_tag,
                    multiple=multiple,
                    requeue=requeue,
                ),
                self.number,
            ),
        )

        return LazyCoroutine(self.connection.drain)

    def basic_reject(self, delivery_tag, *, requeue=True) -> DrainResult:
        self.writer.write(
            pamqp.frame.marshal(
                spec.Basic.Reject(delivery_tag=delivery_tag, requeue=requeue),
                self.number,
            ),
        )

        return LazyCoroutine(self.connection.drain)

    async def basic_publish(
        self,
        body: bytes,
        *,
        exchange: str = "",
        routing_key: str = "",
        properties: spec.Basic.Properties = None,
        mandatory: bool = False,
        immediate: bool = False,
        timeout: Union[int, float] = None
    ) -> Optional[ConfirmationFrameType]:

        frame = spec.Basic.Publish(
            exchange=exchange,
            routing_key=routing_key,
            mandatory=mandatory,
            immediate=immediate,
        )

        content_header = ContentHeader(
            properties=properties or spec.Basic.Properties(delivery_mode=1),
            body_size=len(body),
        )

        if not content_header.properties.message_id:
            # UUID compatible random bytes
            rnd_id = os.urandom(16)
            content_header.properties.message_id = hexlify(rnd_id).decode()

        confirmation = None

        async with self.lock:
            self.delivery_tag += 1

            if self.publisher_confirms:
                message_id = content_header.properties.message_id

                if self.delivery_tag not in self.confirmations:
                    self.confirmations[
                        self.delivery_tag
                    ] = self.create_future()

                confirmation = self.confirmations[self.delivery_tag]

                self.message_id_delivery_tag[message_id] = self.delivery_tag

                confirmation.add_done_callback(
                    lambda _: self.message_id_delivery_tag.pop(
                        message_id, None,
                    ),
                )

            self.writer.write(pamqp.frame.marshal(frame, self.number))

            # noinspection PyTypeChecker
            self.writer.write(pamqp.frame.marshal(content_header, self.number))

            with BytesIO(body) as buf:
                read_chunk = partial(buf.read, self.max_content_size)
                reader = iter(read_chunk, b"")

                for chunk in reader:
                    # noinspection PyTypeChecker
                    self.writer.write(
                        pamqp.frame.marshal(ContentBody(chunk), self.number),
                    )

        if not self.publisher_confirms:
            return

        return await asyncio.wait_for(confirmation, timeout=timeout)

    async def basic_qos(
        self,
        *,
        prefetch_size: int = None,
        prefetch_count: int = None,
        global_: bool = False,
        timeout: Union[int, float] = None
    ) -> spec.Basic.QosOk:
        return await self.rpc(
            spec.Basic.Qos(
                prefetch_size=prefetch_size or 0,
                prefetch_count=prefetch_count or 0,
                global_=global_,
            ),
            timeout=timeout,
        )

    async def basic_recover(
        self, *, nowait: bool = False, requeue=False,
        timeout: Union[int, float] = None
    ) -> spec.Basic.RecoverOk:

        if nowait:
            frame = spec.Basic.RecoverAsync(requeue=requeue)
        else:
            frame = spec.Basic.Recover(requeue=requeue)

        return await self.rpc(frame, timeout=timeout)

    async def exchange_declare(
        self,
        exchange: str = None,
        *,
        exchange_type: str = "direct",
        passive: bool = False,
        durable: bool = False,
        auto_delete: bool = False,
        internal: bool = False,
        nowait: bool = False,
        arguments: Optional[Dict[str, Any]] = None,
        timeout: TimeoutType = None
    ) -> spec.Exchange.DeclareOk:
        return await self.rpc(
            spec.Exchange.Declare(
                exchange=str(exchange),
                exchange_type=str(exchange_type),
                passive=bool(passive),
                durable=bool(durable),
                auto_delete=bool(auto_delete),
                internal=bool(internal),
                nowait=bool(nowait),
                arguments=arguments,
            ),
            timeout=timeout,
        )

    async def exchange_delete(
        self,
        exchange: str = None,
        *,
        if_unused: bool = False,
        nowait: bool = False,
        timeout: TimeoutType = None
    ) -> spec.Exchange.DeleteOk:
        return await self.rpc(
            spec.Exchange.Delete(
                exchange=exchange, nowait=nowait, if_unused=if_unused,
            ),
            timeout=timeout,
        )

    async def exchange_bind(
        self,
        destination: str = None,
        source: str = None,
        routing_key: str = "",
        *,
        nowait: bool = False,
        arguments: dict = None,
        timeout: TimeoutType = None
    ) -> spec.Exchange.BindOk:
        return await self.rpc(
            spec.Exchange.Bind(
                destination=destination,
                source=source,
                routing_key=routing_key,
                nowait=nowait,
                arguments=arguments,
            ),
            timeout=timeout,
        )

    async def exchange_unbind(
        self,
        destination: str = None,
        source: str = None,
        routing_key: str = "",
        *,
        nowait: bool = False,
        arguments: dict = None,
        timeout: TimeoutType = None
    ) -> spec.Exchange.UnbindOk:
        return await self.rpc(
            spec.Exchange.Unbind(
                destination=destination,
                source=source,
                routing_key=routing_key,
                nowait=nowait,
                arguments=arguments,
            ),
            timeout=timeout,
        )

    async def flow(
        self, active: bool,
        timeout: TimeoutType = None
    ) -> spec.Channel.FlowOk:
        return await self.rpc(
            spec.Channel.Flow(active=active),
            timeout=timeout,
        )

    async def queue_bind(
        self,
        queue: str,
        exchange: str,
        routing_key: str = "",
        nowait: bool = False,
        arguments: dict = None,
        timeout: TimeoutType = None
    ) -> spec.Queue.BindOk:
        return await self.rpc(
            spec.Queue.Bind(
                queue=queue,
                exchange=exchange,
                routing_key=routing_key,
                nowait=nowait,
                arguments=arguments,
            ),
            timeout=timeout,
        )

    async def queue_declare(
        self,
        queue: str = "",
        *,
        passive: bool = False,
        durable: bool = False,
        exclusive: bool = False,
        auto_delete: bool = False,
        nowait: bool = False,
        arguments: dict = None,
        timeout: TimeoutType = None
    ) -> spec.Queue.DeclareOk:
        return await self.rpc(
            spec.Queue.Declare(
                queue=queue,
                passive=bool(passive),
                durable=bool(durable),
                exclusive=bool(exclusive),
                auto_delete=bool(auto_delete),
                nowait=bool(nowait),
                arguments=arguments,
            ),
            timeout=timeout,
        )

    async def queue_delete(
        self,
        queue: str = "",
        if_unused: bool = False,
        if_empty: bool = False,
        nowait=False,
        timeout: TimeoutType = None
    ) -> spec.Queue.DeleteOk:
        return await self.rpc(
            spec.Queue.Delete(
                queue=queue,
                if_unused=if_unused,
                if_empty=if_empty,
                nowait=nowait,
            ),
            timeout=timeout,
        )

    async def queue_purge(
        self, queue: str = "", nowait: bool = False,
        timeout: TimeoutType = None
    ) -> spec.Queue.PurgeOk:
        return await self.rpc(
            spec.Queue.Purge(queue=queue, nowait=nowait),
            timeout=timeout,
        )

    async def queue_unbind(
        self,
        queue: str = "",
        exchange: str = None,
        routing_key: str = None,
        arguments: dict = None,
        timeout: TimeoutType = None
    ) -> spec.Queue.UnbindOk:
        return await self.rpc(
            spec.Queue.Unbind(
                routing_key=routing_key,
                arguments=arguments,
                queue=queue,
                exchange=exchange,
            ),
            timeout=timeout,
        )

    async def tx_commit(
        self, timeout: TimeoutType = None
    ) -> spec.Tx.CommitOk:
        return await self.rpc(spec.Tx.Commit(), timeout=timeout)

    async def tx_rollback(
        self, timeout: TimeoutType = None
    ) -> spec.Tx.RollbackOk:
        return await self.rpc(spec.Tx.Rollback(), timeout=timeout)

    async def tx_select(self, timeout: TimeoutType = None) -> spec.Tx.SelectOk:
        return await self.rpc(spec.Tx.Select(), timeout=timeout)

    async def confirm_delivery(
        self, nowait=False,
        timeout: TimeoutType = None
    ):
        return await self.rpc(
            spec.Confirm.Select(nowait=nowait),
            timeout=timeout,
        )
