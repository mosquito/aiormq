import socket
from platform import platform


def set_keepalive_linux(
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


def set_keepalive_osx(
    sock: socket.socket, keepalive_time: int, interval: int
) -> None:
    """ Source: https://stackoverflow.com/a/14855726 """

    # scraped from /usr/include, not exported by python's socket module
    TCP_KEEPALIVE = 0x10
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
    sock.setsockopt(socket.IPPROTO_TCP, TCP_KEEPALIVE, interval)


def set_keepalive_windows(
    sock: socket.socket, keepalive_time: int, interval: int
) -> None:
    # Source: https://stackoverflow.com/a/31868203
    sock.ioctl(socket.SIO_KEEPALIVE_VALS, (
        1,
        int(keepalive_time) * 1000,
        int(interval) * 1000
    ))


PLATFORM = platform()

if PLATFORM == 'Windows':
    set_keepalive = set_keepalive_windows
elif PLATFORM == 'Darwin':
    set_keepalive = set_keepalive_osx
elif PLATFORM == 'Linux':
    set_keepalive = set_keepalive_linux
else:
    def set_keepalive(*_):
        return
