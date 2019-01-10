#!/usr/bin/env python
# encoding: utf-8
import typing

from pamqp import specification as spec, ContentHeader
from yarl import URL


ChannelRType = typing.Tuple[int, spec.Channel.OpenOk]
DeliveredMessage = typing.NamedTuple(
    'DeliveredMessage', [
        ('delivery', spec.Basic.Deliver),
        ('header', ContentHeader),
        ('body', bytes),
        ('channel', "aiormq.Channel")
    ]
)
CallbackCoro = typing.Coroutine[DeliveredMessage, None, typing.Any]
ConsumerCallback = typing.Callable[[], CallbackCoro]
ReturnCallback = typing.Callable[[], CallbackCoro]
ArgumentsType = typing.Dict[str, typing.Union[str, int, bool]]
ConfirmationFrameType = typing.Union[spec.Basic.Ack, spec.Basic.Nack]
GetResultType = typing.Tuple[
    typing.Union[spec.Basic.GetEmpty, spec.Basic.GetOk],
    typing.Optional[DeliveredMessage]
]
SSLCerts = typing.NamedTuple(
    'SSLCerts', [
        ('cert', str),
        ('key', str),
        ('ca', str),
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
