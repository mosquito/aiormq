import typing

from pamqp import ContentHeader
from pamqp import specification as spec
from pamqp.body import ContentBody
from yarl import URL


ChannelRType = typing.Tuple[int, spec.Channel.OpenOk]
GetResultType = typing.Union[spec.Basic.GetEmpty, spec.Basic.GetOk]
DeliveredMessage = typing.NamedTuple(
    "DeliveredMessage",
    [
        ("delivery", typing.Union[spec.Basic.Deliver, GetResultType]),
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
    spec.Basic.Ack, spec.Basic.Nack, spec.Basic.Reject,
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
FrameType = typing.Union[spec.Frame, ContentHeader, ContentBody]
RpcReturnType = typing.Optional[
    typing.Union[
        spec.Tx.CommitOk,
        spec.Tx.RollbackOk,
        spec.Tx.SelectOk,
        spec.Basic.RecoverOk,
        spec.Basic.QosOk,
        spec.Basic.CancelOk,
        spec.Channel.CloseOk,
        spec.Basic.ConsumeOk,
        spec.Basic.GetOk,
        spec.Exchange.DeclareOk,
        spec.Exchange.UnbindOk,
        spec.Exchange.BindOk,
        spec.Exchange.DeleteOk,
        spec.Queue.DeleteOk,
        spec.Queue.BindOk,
        spec.Queue.UnbindOk,
        spec.Queue.PurgeOk,
        spec.Queue.DeleteOk,
        spec.Channel.FlowOk,
    ]
]

