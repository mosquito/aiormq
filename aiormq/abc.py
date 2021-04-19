import asyncio
from abc import ABC, abstractmethod, abstractproperty
from typing import (
    Any, Awaitable, Callable, Coroutine, Dict, Iterable, NamedTuple, Optional,
    Set, Tuple, Type, Union,
)

from pamqp import commands as spec
from pamqp.base import Frame
from pamqp.body import ContentBody
from pamqp.common import FieldTable, FieldValue, FieldArray
from pamqp.commands import Basic, Channel, Exchange, Queue, Tx
from pamqp.constants import REPLY_SUCCESS
from pamqp.header import ContentHeader
from pamqp.heartbeat import Heartbeat
from yarl import URL



# noinspection PyShadowingNames
class TaskWrapper:
    def __init__(self, task: asyncio.Task):
        self.task = task
        self.exception = asyncio.CancelledError

    def throw(self, exception) -> None:
        self.exception = exception
        self.task.cancel()

    async def __inner(self) -> Any:
        try:
            return await self.task
        except asyncio.CancelledError as e:
            raise self.exception from e

    def __await__(self, *args, **kwargs) -> Any:
        return self.__inner().__await__()

    def cancel(self) -> None:
        return self.throw(asyncio.CancelledError)

    def __getattr__(self, item: str) -> Any:
        return getattr(self.task, item)

    def __repr__(self) -> str:
        return "<%s: %s>" % (self.__class__.__name__, repr(self.task))


TaskType = Union[asyncio.Task, TaskWrapper]
CoroutineType = Coroutine[Any, None, Any]
GetResultType = Union[Basic.GetEmpty, Basic.GetOk]


class DeliveredMessage(NamedTuple):
    delivery: Union[Basic.Deliver, GetResultType]
    header: ContentHeader
    body: bytes
    channel: "AbstractChannel"


ChannelRType = Tuple[int, Channel.OpenOk]

CallbackCoro = Coroutine[Any, None, Any]
ConsumerCallback = Callable[[DeliveredMessage], CallbackCoro]
ReturnCallback = Callable[[], CallbackCoro]

ArgumentsType = Dict[
    str, Union[str, int, bool, Dict[str, Union[str, int, bool]]],
]

ConfirmationFrameType = Union[
    Basic.Ack, Basic.Nack, Basic.Reject,
]


class SSLCerts(NamedTuple):
    cert: Optional[str]
    key: Optional[str]
    capath: Optional[str]
    cafile: Optional[str]
    cadata: Optional[bytes]
    verify: bool


class FrameReceived(NamedTuple):
    channel: int
    frame: str


URLorStr = Union[URL, str]
DrainResult = Awaitable[None]
TimeoutType = Optional[Union[int, float]]
FrameType = Union[Frame, ContentHeader, ContentBody]
RpcReturnType = Optional[
    Union[
        Tx.CommitOk,
        Tx.RollbackOk,
        Tx.SelectOk,
        Basic.RecoverOk,
        Basic.QosOk,
        Basic.CancelOk,
        Channel.CloseOk,
        Basic.ConsumeOk,
        Basic.GetOk,
        Exchange.DeclareOk,
        Exchange.UnbindOk,
        Exchange.BindOk,
        Exchange.DeleteOk,
        Queue.DeleteOk,
        Queue.BindOk,
        Queue.UnbindOk,
        Queue.PurgeOk,
        Queue.DeleteOk,
        Channel.FlowOk,
    ]
]


class ChannelFrame(NamedTuple):
    channel_number: int
    frames: Iterable[Union[FrameType, Heartbeat]]
    drain_future: Optional[asyncio.Future] = None


ExceptionType = Union[Exception, Type[Exception]]


class AbstractFutureStore:
    futures: Set[Union[asyncio.Future, TaskType]]
    loop: asyncio.AbstractEventLoop

    @abstractmethod
    def add(self, future: Union[asyncio.Future, TaskWrapper]):
        raise NotImplementedError

    @abstractmethod
    async def reject_all(self, exception: Optional[ExceptionType]) -> None:
        raise NotImplementedError

    @abstractmethod
    def create_task(self, coro: CoroutineType) -> TaskType:
        raise NotImplementedError

    @abstractmethod
    def create_future(self):
        raise NotImplementedError

    @abstractmethod
    def get_child(self) -> "AbstractFutureStore":
        raise NotImplementedError


class AbstractBase(ABC):
    loop: asyncio.AbstractEventLoop

    @abstractmethod
    def _future_store_child(self) -> AbstractFutureStore:
        raise NotImplementedError

    @abstractmethod
    def create_task(self, coro: CoroutineType) -> TaskType:
        raise NotImplementedError

    def create_future(self) -> asyncio.Future:
        raise NotImplementedError

    @abstractmethod
    async def _on_close(self, exc: Optional[Exception] = None):
        raise NotImplementedError

    @abstractmethod
    async def close(self, exc=asyncio.CancelledError()) -> None:
        raise NotImplementedError

    @abstractmethod
    def __str__(self) -> str:
        raise NotImplementedError

    @abstractproperty
    def is_closed(self) -> bool:
        raise NotImplementedError


class AbstractChannel(AbstractBase):
    frames: asyncio.Queue

    @abstractmethod
    async def open(self):
        pass

    @abstractmethod
    async def basic_get(
        self, queue: str = "", no_ack: bool = False,
        timeout: Union[int, float] = None
    ) -> DeliveredMessage:
        raise NotImplementedError

    @abstractmethod
    async def basic_cancel(
        self, consumer_tag, *, nowait: bool = False,
        timeout: Union[int, float] = None
    ) -> spec.Basic.CancelOk:
        raise NotImplementedError

    @abstractmethod
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
        raise NotImplementedError

    @abstractmethod
    def basic_ack(
        self, delivery_tag, multiple=False,
    ) -> DrainResult:
        raise NotImplementedError

    @abstractmethod
    def basic_nack(
        self,
        delivery_tag: int,
        multiple: bool = False,
        requeue: bool = True,
    ) -> DrainResult:
        raise NotImplementedError

    @abstractmethod
    def basic_reject(self, delivery_tag, *, requeue=True) -> DrainResult:
        raise NotImplementedError

    @abstractmethod
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
        raise NotImplementedError

    @abstractmethod
    async def basic_qos(
        self,
        *,
        prefetch_size: int = None,
        prefetch_count: int = None,
        global_: bool = False,
        timeout: Union[int, float] = None
    ) -> spec.Basic.QosOk:
        raise NotImplementedError

    @abstractmethod
    async def basic_recover(
        self, *, nowait: bool = False, requeue=False,
        timeout: Union[int, float] = None
    ) -> spec.Basic.RecoverOk:
        raise NotImplementedError

    @abstractmethod
    async def exchange_declare(
        self,
        exchange: str = "",
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
        raise NotImplementedError

    @abstractmethod
    async def exchange_delete(
        self,
        exchange: str = "",
        *,
        if_unused: bool = False,
        nowait: bool = False,
        timeout: TimeoutType = None
    ) -> spec.Exchange.DeleteOk:
        raise NotImplementedError

    @abstractmethod
    async def exchange_bind(
        self,
        destination: str = "",
        source: str = "",
        routing_key: str = "",
        *,
        nowait: bool = False,
        arguments: dict = None,
        timeout: TimeoutType = None
    ) -> spec.Exchange.BindOk:
        raise NotImplementedError

    @abstractmethod
    async def exchange_unbind(
        self,
        destination: str = "",
        source: str = "",
        routing_key: str = "",
        *,
        nowait: bool = False,
        arguments: dict = None,
        timeout: TimeoutType = None
    ) -> spec.Exchange.UnbindOk:
        raise NotImplementedError

    @abstractmethod
    async def flow(
        self, active: bool,
        timeout: TimeoutType = None
    ) -> spec.Channel.FlowOk:
        raise NotImplementedError

    @abstractmethod
    async def queue_bind(
        self,
        queue: str,
        exchange: str,
        routing_key: str = "",
        nowait: bool = False,
        arguments: dict = None,
        timeout: TimeoutType = None
    ) -> spec.Queue.BindOk:
        raise NotImplementedError

    @abstractmethod
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
        raise NotImplementedError

    @abstractmethod
    async def queue_delete(
        self,
        queue: str = "",
        if_unused: bool = False,
        if_empty: bool = False,
        nowait=False,
        timeout: TimeoutType = None
    ) -> spec.Queue.DeleteOk:
        raise NotImplementedError

    @abstractmethod
    async def queue_purge(
        self, queue: str = "", nowait: bool = False,
        timeout: TimeoutType = None
    ) -> spec.Queue.PurgeOk:
        raise NotImplementedError

    @abstractmethod
    async def queue_unbind(
        self,
        queue: str = "",
        exchange: str = "",
        routing_key: str = "",
        arguments: dict = None,
        timeout: TimeoutType = None
    ) -> spec.Queue.UnbindOk:
        raise NotImplementedError

    @abstractmethod
    async def tx_commit(
        self, timeout: TimeoutType = None
    ) -> spec.Tx.CommitOk:
        raise NotImplementedError

    @abstractmethod
    async def tx_rollback(
        self, timeout: TimeoutType = None
    ) -> spec.Tx.RollbackOk:
        raise NotImplementedError

    @abstractmethod
    async def tx_select(self, timeout: TimeoutType = None) -> spec.Tx.SelectOk:
        raise NotImplementedError

    @abstractmethod
    async def confirm_delivery(
        self, nowait=False,
        timeout: TimeoutType = None
    ):
        raise NotImplementedError


class AbstractConnection(AbstractBase):
    FRAME_BUFFER: int = 10
    # Interval between sending heartbeats based on the heartbeat(timeout)
    HEARTBEAT_INTERVAL_MULTIPLIER: Union[int, float]

    # Allow three missed heartbeats (based on heartbeat(timeout)
    HEARTBEAT_GRACE_MULTIPLIER: int

    server_properties: ArgumentsType
    connection_tune: spec.Connection.Tune
    channels: Dict[int, Optional[AbstractChannel]]
    write_queue: asyncio.Queue

    @abstractmethod
    def set_close_reason(
        self, reply_code=REPLY_SUCCESS,
        reply_text="normally closed",
        class_id=0, method_id=0,
    ):
        raise NotImplementedError

    @abstractproperty
    def is_opened(self) -> bool:
        raise NotImplementedError

    @abstractmethod
    def __str__(self):
        raise NotImplementedError

    @abstractmethod
    async def connect(self, client_properties: dict = None) -> bool:
        raise NotImplementedError

    @abstractproperty
    def server_capabilities(self) -> ArgumentsType:
        raise NotImplementedError

    @abstractproperty
    def basic_nack(self) -> bool:
        raise NotImplementedError

    @abstractproperty
    def consumer_cancel_notify(self) -> bool:
        raise NotImplementedError

    @abstractproperty
    def exchange_exchange_bindings(self) -> bool:
        raise NotImplementedError

    @abstractproperty
    def publisher_confirms(self):
        raise NotImplementedError

    async def channel(
        self,
        channel_number: int = None,
        publisher_confirms=True,
        frame_buffer: int = FRAME_BUFFER,
        **kwargs
    ) -> AbstractChannel:
        raise NotImplementedError

    @abstractmethod
    async def __aenter__(self):
        raise NotImplementedError


__all__ = (
    "AbstractBase", "AbstractChannel", "AbstractConnection",
    "AbstractFutureStore", "ArgumentsType", "CallbackCoro", "ChannelFrame",
    "ChannelRType", "ConfirmationFrameType", "ConsumerCallback",
    "CoroutineType", "DeliveredMessage", "DrainResult", "ExceptionType",
    "FieldArray", "FieldTable", "FieldValue", "FrameReceived", "FrameType",
    "GetResultType", "ReturnCallback", "RpcReturnType", "SSLCerts",
    "TaskType", "TaskWrapper", "TimeoutType", "URLorStr",
)
