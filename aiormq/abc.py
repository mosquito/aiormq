import asyncio
from abc import ABC, abstractmethod
from typing import (
    Any, Awaitable, Callable, Coroutine, Dict, NamedTuple, Optional, Tuple,
    Union,
)

from pamqp import commands as spec
from pamqp.base import Frame
from pamqp.body import ContentBody
from pamqp.commands import Basic, Channel, Exchange, Queue, Tx
from pamqp.header import ContentHeader
from yarl import URL


GetResultType = Union[Basic.GetEmpty, Basic.GetOk]
DeliveredMessage = NamedTuple(
    "DeliveredMessage",
    [
        ("delivery", Union[Basic.Deliver, GetResultType]),
        ("header", ContentHeader),
        ("body", bytes),
        ("channel", "AbstractChannel"),
    ],
)

ChannelRType = Tuple[int, Channel.OpenOk]

CallbackCoro = Coroutine[DeliveredMessage, None, Any]
ConsumerCallback = Callable[[], CallbackCoro]
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


class AbstractChannel(ABC):
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
