
class AMQPError(Exception):
    message = 'An unspecified AMQP error has occurred'

    def __repr__(self):
        return "<%s: %s>" % (self.__class__.__name__, self.message % self.args)


class AMQPConnectionError(AMQPError):
    message = 'Connection can not be opened'


class IncompatibleProtocolError(AMQPConnectionError):
    message = 'The protocol returned by the server is not supported'


class AuthenticationError(AMQPConnectionError):
    message = (
        'Server and client could not negotiate use of the '
        'authentication mechanisms. Server supports only %r, '
        'but client supports only %r.'
    )


class ProbableAuthenticationError(AMQPConnectionError):
    message = (
        'Client was disconnected at a connection stage indicating a '
        'probable authentication error: %s'
    )


class ProbableAccessDeniedError(AMQPConnectionError):
    message = ('Client was disconnected at a connection stage indicating a '
               'probable denial of access to the specified virtual host')


class ConnectionClosed(AMQPConnectionError):
    message = 'The AMQP connection was closed (%s) %s'


class ConnectionSyntaxError(ConnectionClosed):
    message = ('The sender sent a frame that contained illegal values for '
               'one or more fields. This strongly implies a programming error '
               'in the sending peer: %r')


class ConnectionFrameError(ConnectionClosed):
    message = ('The sender sent a malformed frame that the recipient could '
               'not decode. This strongly implies a programming error '
               'in the sending peer: %r')


class ConnectionCommandInvalid(ConnectionClosed):
    message = ('The client sent an invalid sequence of frames, attempting to '
               'perform an operation that was considered invalid by the server.'
               ' This usually implies a programming error in the client: %r')


class ConnectionChannelError(ConnectionClosed):
    message = ('The client attempted to work with a channel that had not been '
               'correctly opened. This most likely indicates a fault in the '
               'client layer: %r')


class ConnectionUnexpectedFrame(ConnectionClosed):
    message = ("The peer sent a frame that was not expected, usually in the "
               "context of a content header and body. This strongly indicates "
               "a fault in the peer's content processing: %r")


class ConnectionResourceError(ConnectionClosed):
    message = ("The server could not complete the method because it lacked "
               "sufficient resources. This may be due to the client creating "
               "too many of some type of entity: %r")


class ConnectionNotAllowed(ConnectionClosed):
    message = ("The client tried to work with some entity in a manner that is "
               "prohibited by the server, due to security settings or by "
               "some other criteria: %r")


class ConnectionNotImplemented(ConnectionClosed):
    message = ("The client tried to use functionality that is "
               "not implemented in the server: %r")


class ConnectionInternalError(ConnectionClosed):
    message = (" The server could not complete the method because of an "
               "internal error. The server may require intervention by an "
               "operator in order to resume normal operations: %r")


class AMQPChannelError(AMQPError):
    message = 'An unspecified AMQP channel error has occurred'


class ChannelAccessRefused(AMQPChannelError):
    message = ('The client attempted to work with a server entity to '
               'which it has no access due to security settings: %r')


class ChannelNotFoundEntity(AMQPChannelError):
    message = ('The client attempted to work with a server '
               'entity that does not exist: %r')


class ChannelLockedResource(AMQPChannelError):
    message = ('The client attempted to work with a server entity to '
               'which it has no access because another client is working '
               'with it: %r')


class ChannelPreconditionFailed(AMQPChannelError):
    message = ('The client requested a method that was not allowed because '
               'some precondition failed: %r')


class ChannelClosed(AMQPChannelError):
    message = 'The channel was closed (%s) %s'


class DuplicateConsumerTag(AMQPChannelError):
    message = 'The consumer tag specified already exists for this channel: %s'


class ConsumerCancelled(AMQPChannelError):
    message = 'Server cancelled consumer'


class UnroutableError(AMQPChannelError):
    message = '%r unroutable messages returned by broker'


class NackError(UnroutableError):
    pass


class InvalidChannelNumber(AMQPError):
    message = 'An invalid channel number has been specified: %s'


class ProtocolSyntaxError(AMQPError):
    message = 'An unspecified protocol syntax error occurred'


class UnexpectedFrameError(ProtocolSyntaxError):
    message = 'Received a frame out of sequence: %r'


class ProtocolVersionMismatch(ProtocolSyntaxError):
    message = 'Protocol versions did not match: %r vs %r'


class BodyTooLongError(ProtocolSyntaxError):
    message = ('Received too many bytes for a message delivery: '
               'Received %i, expected %i')


class InvalidFrameError(ProtocolSyntaxError):
    message = 'Invalid frame received: %r'


class InvalidFieldTypeException(ProtocolSyntaxError):
    message = 'Unsupported field kind %s'


class UnsupportedAMQPFieldException(ProtocolSyntaxError):
    message = 'Unsupported field kind %s'


class MethodNotImplemented(AMQPError):
    pass


class ChannelError(AMQPError):
    message = 'An unspecified error occurred with the Channel'


class InvalidMinimumFrameSize(ProtocolSyntaxError):
    message = 'AMQP Minimum Frame Size is 4096 Bytes'


class InvalidMaximumFrameSize(ProtocolSyntaxError):
    message = 'AMQP Maximum Frame Size is 131072 Bytes'


class AMQPException(Exception):
    pass


class MessageProcessError(AMQPException):
    pass


class DeliveryError(AMQPException):
    __slots__ = 'message', 'frame'

    def __init__(self, message, frame):
        self.message = message
        self.frame = frame


class QueueEmpty(AMQPException):
    pass


class TransactionClosed(AMQPException):
    pass
