"""
Modern IPC exceptions with detailed error types
"""

from typing import Any, Optional


class IPCError(Exception):
    """Base IPC exception class"""

    def __init__(self, message: str, code: Optional[int] = None) -> None:
        super().__init__(message)
        self.message = message
        self.code = code


class NoEndpointFoundError(IPCError):
    """Raised upon requesting an invalid endpoint"""

    def __init__(self, endpoint: str) -> None:
        message = f"Endpoint '{endpoint}' not found"
        super().__init__(message, 404)
        self.endpoint = endpoint


class ServerConnectionRefusedError(IPCError):
    """Raised upon a server refusing to connect / not being found"""

    def __init__(self, host: str, port: int, reason: str = "Connection refused") -> None:
        message = f"Cannot connect to {host}:{port} - {reason}"
        super().__init__(message, 503)
        self.host = host
        self.port = port
        self.reason = reason


class JSONEncodeError(IPCError):
    """Raised upon un-serializable objects are given to the IPC"""

    def __init__(self, obj_type: str, details: str = "") -> None:
        message = f"Cannot serialize object of type '{obj_type}'"
        if details:
            message += f": {details}"
        super().__init__(message, 400)
        self.obj_type = obj_type


class NotConnected(IPCError):
    """Raised upon websocket not connected"""

    def __init__(self, reason: str = "Not connected to server") -> None:
        super().__init__(reason, 503)


class RateLimited(IPCError):
    """Raised when rate limit is exceeded"""

    def __init__(self, retry_after: float, requests_made: int, window_size: int) -> None:
        message = f"Rate limited: {requests_made} requests in {window_size}s window. Retry after {retry_after:.1f}s"
        super().__init__(message, 429)
        self.retry_after = retry_after
        self.requests_made = requests_made
        self.window_size = window_size


class ConnectionTimeout(IPCError):
    """Raised when connection attempt times out"""

    def __init__(self, timeout: float, operation: str = "connection") -> None:
        message = f"Timeout after {timeout}s during {operation}"
        super().__init__(message, 408)
        self.timeout = timeout
        self.operation = operation


class AuthenticationError(IPCError):
    """Raised when authentication fails"""

    def __init__(self, reason: str = "Invalid or missing secret key") -> None:
        super().__init__(reason, 403)


class ServerNotRunning(IPCError):
    """Raised when attempting operations on a stopped server"""

    def __init__(self, operation: str = "Server operation") -> None:
        message = f"Cannot perform '{operation}': server is not running"
        super().__init__(message, 503)
        self.operation = operation


class EndpointError(IPCError):
    """Raised when an endpoint encounters an error during execution"""

    def __init__(self, endpoint: str, original_error: Exception) -> None:
        message = (
            f"Error in endpoint '{endpoint}': {type(original_error).__name__}: {original_error}"
        )
        super().__init__(message, 500)
        self.endpoint = endpoint
        self.original_error = original_error
