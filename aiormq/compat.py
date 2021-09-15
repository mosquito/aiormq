import socket
from platform import platform


PLATFORM = platform()


if PLATFORM == 'Windows':
    def set_keepalive(
        sock: socket.socket, keepalive_time: int, interval: int
    ) -> None:
        # Source: https://stackoverflow.com/a/31868203
        sock.ioctl(                         # type: ignore
            socket.SIO_KEEPALIVE_VALS,      # type: ignore
            (
                1,
                int(keepalive_time) * 1000,
                int(interval) * 1000
            )
        )

elif PLATFORM == 'Darwin':
    def set_keepalive(
        sock: socket.socket, keepalive_time: int, interval: int
    ) -> None:
        """ Source: https://stackoverflow.com/a/14855726 """

        # scraped from /usr/include, not exported by python's socket module
        TCP_KEEPALIVE = 0x10
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
        sock.setsockopt(socket.IPPROTO_TCP, TCP_KEEPALIVE, interval)

elif PLATFORM == 'Linux':
    def set_keepalive(
        sock: socket.socket, keepalive_time: int, interval: int
    ) -> None:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPIDLE, interval)
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPINTVL, interval)
        sock.setsockopt(
            socket.IPPROTO_TCP,
            socket.TCP_KEEPCNT,
            round(keepalive_time / interval)
        )

else:
    def set_keepalive(
        sock: socket.socket, keepalive_time: int, interval: int
    ) -> None:
        return
