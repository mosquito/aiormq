import abc
from enum import Enum


class AuthBase:
    def __init__(self, connector):
        self.connector = connector
        self.value = None

    @abc.abstractmethod
    def encode(self) -> str:
        raise NotImplementedError

    def marshal(self) -> str:
        if self.value is None:
            self.value = self.encode()
        return self.value


class PlainAuth(AuthBase):
    def encode(self) -> str:
        return (
            "\x00"
            + (self.connector.url.user or "guest")
            + "\x00"
            + (self.connector.url.password or "guest")
        )


class AuthMechanism(Enum):
    PLAIN = PlainAuth
