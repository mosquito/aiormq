
class AMQPError(Exception):
    message = 'An unspecified AMQP error has occurred'

    def __repr__(self):
        return "<%s: %s>" % (self.__class__.__name__, self.message % self.args)


# Backward compatibility
AMQPException = AMQPError


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


class ChannelClosed(AMQPChannelError):
    message = 'The channel was closed (%s) %s'


class ChannelAccessRefused(ChannelClosed):
    message = ('The client attempted to work with a server entity to '
               'which it has no access due to security settings: %r')


class ChannelNotFoundEntity(ChannelClosed):
    message = ('The client attempted to work with a server '
               'entity that does not exist: %r')


class ChannelLockedResource(ChannelClosed):
    message = ('The client attempted to work with a server entity to '
               'which it has no access because another client is working '
               'with it: %r')


class ChannelPreconditionFailed(ChannelClosed):
    message = ('The client requested a method that was not allowed because '
               'some precondition failed: %r')


class DuplicateConsumerTag(ChannelClosed):
    message = 'The consumer tag specified already exists for this channel: %s'


class ProtocolSyntaxError(AMQPError):
    message = 'An unspecified protocol syntax error occurred'


class InvalidFrameError(ProtocolSyntaxError):
    message = 'Invalid frame received: %r'


class MethodNotImplemented(AMQPError):
    pass


class DeliveryError(AMQPError):
    __slots__ = 'message', 'frame'

    def __init__(self, message, frame):
        self.message = message
        self.frame = frame
        super().__init__()
