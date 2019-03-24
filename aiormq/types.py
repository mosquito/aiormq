import typing

from pamqp import specification as spec, ContentHeader
from yarl import URL


ChannelRType = typing.Tuple[int, spec.Channel.OpenOk]
GetResultType = typing.Union[spec.Basic.GetEmpty, spec.Basic.GetOk]
DeliveredMessage = typing.NamedTuple(
    'DeliveredMessage', [
        ('delivery', typing.Union[spec.Basic.Deliver, GetResultType]),
        ('header', ContentHeader),
        ('body', bytes),
        ('channel', "aiormq.Channel")
    ]
)
CallbackCoro = typing.Coroutine[DeliveredMessage, None, typing.Any]
ConsumerCallback = typing.Callable[[], CallbackCoro]
ReturnCallback = typing.Callable[[], CallbackCoro]
ArgumentsType = typing.Dict[str, typing.Union[str, int, bool]]

ConfirmationFrameType = typing.Union[
    spec.Basic.Ack,
    spec.Basic.Nack,
    spec.Basic.Reject
]

SSLCerts = typing.NamedTuple(
    'SSLCerts', [
        ('cert', str),
        ('key', str),
        ('capath', str),
        ('cafile', str),
        ('cadata', bytes),
        ('verify', bool),
    ]
)
FrameReceived = typing.NamedTuple(
    'FrameReceived', [
        ('channel', int),
        ('frame', str),
    ]
)


URLorStr = typing.Union[URL, str]
DrainResult = typing.Awaitable[None]
