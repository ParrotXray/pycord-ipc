"""
Modern discord.ext.ipc library
Updated for Python 3.10+ and py-cord compatibility
"""

import collections
from typing import NamedTuple

from .client import Client
from .errors import *
from .server import Server


class _VersionInfo(NamedTuple):
    major: int
    minor: int
    micro: int
    release: str
    serial: int


__version__ = "0.1.4"
__version_info__ = _VersionInfo(0, 1, 4, "final", 0)

version = __version__
version_info = __version_info__

__all__ = [
    "Client",
    "Server",
    "IPCError",
    "NoEndpointFoundError",
    "ServerConnectionRefusedError",
    "JSONEncodeError",
    "NotConnected",
    "RateLimited",
    "ConnectionTimeout",
    "AuthenticationError",
    "ServerNotRunning",
    "__version__",
    "__version_info__",
]
