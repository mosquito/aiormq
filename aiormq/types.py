import typing

from pamqp.base import Frame
from pamqp.commands import Channel, Basic, Tx, Exchange, Queue
from pamqp.header import ContentHeader
from pamqp.body import ContentBody
from yarl import URL


ChannelRType = typing.Tuple[int, Channel.OpenOk]
GetResultType = typing.Union[Basic.GetEmpty, Basic.GetOk]
DeliveredMessage = typing.NamedTuple(
    "DeliveredMessage",
    [
        ("delivery", typing.Union[Basic.Deliver, GetResultType]),
        ("header", ContentHeader),
        ("body", bytes),
        ("channel", "aiormq.Channel"),
    ],
)
CallbackCoro = typing.Coroutine[DeliveredMessage, None, typing.Any]
ConsumerCallback = typing.Callable[[], CallbackCoro]
ReturnCallback = typing.Callable[[], CallbackCoro]
ArgumentsType = typing.Dict[str, typing.Union[str, int, bool]]

ConfirmationFrameType = typing.Union[
    Basic.Ack, Basic.Nack, Basic.Reject,
]

SSLCerts = typing.NamedTuple(
    "SSLCerts",
    [
        ("cert", str),
        ("key", str),
        ("capath", str),
        ("cafile", str),
        ("cadata", bytes),
        ("verify", bool),
    ],
)
FrameReceived = typing.NamedTuple(
    "FrameReceived", [("channel", int), ("frame", str)],
)


URLorStr = typing.Union[URL, str]
DrainResult = typing.Awaitable[None]
TimeoutType = typing.Optional[typing.Union[int, float]]
FrameType = typing.Union[Frame, ContentHeader, ContentBody]
RpcReturnType = typing.Optional[
    typing.Union[
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

