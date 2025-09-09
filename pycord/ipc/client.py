"""
Modern IPC Client with auto-reconnection and rate limiting
"""

import asyncio
import logging
import time
import weakref
from collections import deque
from typing import Any, Dict, Optional, Tuple, Union

import aiohttp

from .errors import *

log = logging.getLogger(__name__)


class RateLimiter:
    """Rate limiter for IPC requests"""

    def __init__(self, max_requests: int, window_seconds: int) -> None:
        self.max_requests = max_requests
        self.window_seconds = window_seconds
        self.requests: deque[float] = deque()

    async def acquire(self) -> None:
        """Wait until request can be made without exceeding rate limit"""
        now = time.time()

        while self.requests and self.requests[0] <= now - self.window_seconds:
            self.requests.popleft()

        if len(self.requests) >= self.max_requests:
            oldest_request = self.requests[0]
            retry_after = (oldest_request + self.window_seconds) - now

            if retry_after > 0:
                raise RateLimited(
                    retry_after=retry_after,
                    requests_made=len(self.requests),
                    window_size=self.window_seconds,
                )

        self.requests.append(now)


class Client:
    """
    Handles webserver side requests to the bot process with modern features.

    Parameters
    ----------
    host: str
        The IP or host of the IPC server, defaults to localhost
    port: Optional[int]
        The port of the IPC server. If not supplied the port will be found automatically
    secret_key: Optional[Union[str, bytes]]
        The secret key for your IPC server. Must match the server secret_key
    multicast_port: int
        The port for multicast discovery, defaults to 20000
    max_requests: int
        Maximum requests per window for rate limiting, defaults to 100
    rate_window: int
        Rate limiting window in seconds, defaults to 60
    auto_reconnect: bool
        Whether to automatically reconnect on connection loss, defaults to True
    reconnect_delay: float
        Delay between reconnection attempts in seconds, defaults to 5.0
    max_reconnect_attempts: int
        Maximum reconnection attempts (0 = infinite), defaults to 10
    connection_timeout: float
        Connection timeout in seconds, defaults to 60.0
    request_timeout: float
        Request timeout in seconds, defaults to 300.0
    heartbeat_interval: float
        Heartbeat interval in seconds for WebSocket, defaults to 60.0
    """

    def __init__(
        self,
        host: str = "localhost",
        port: Optional[int] = None,
        secret_key: Optional[Union[str, bytes]] = None,
        multicast_port: int = 20000,
        max_requests: int = 100,
        rate_window: int = 60,
        auto_reconnect: bool = True,
        reconnect_delay: float = 5.0,
        max_reconnect_attempts: int = 10,
        connection_timeout: float = 60.0,
        request_timeout: float = 300.0,
        heartbeat_interval: float = 60.0,
    ) -> None:
        self.host = host
        self.port = port
        self.secret_key = secret_key
        self.multicast_port = multicast_port
        self.connection_timeout = connection_timeout
        self.request_timeout = request_timeout
        self.heartbeat_interval = heartbeat_interval

        self.rate_limiter = RateLimiter(max_requests, rate_window)

        self.auto_reconnect = auto_reconnect
        self.reconnect_delay = reconnect_delay
        self.max_reconnect_attempts = max_reconnect_attempts
        self._reconnect_attempts = 0

        self.session: Optional[aiohttp.ClientSession] = None
        self.websocket: Optional[aiohttp.ClientWebSocketResponse] = None
        self.multicast: Optional[aiohttp.ClientWebSocketResponse] = None
        self._connected = False
        self._connection_lock = asyncio.Lock()
        self._reconnecting = False
        self._closed = False

        self._active_tasks: weakref.WeakSet = weakref.WeakSet()

    @property
    def url(self) -> str:
        """Get the WebSocket URL"""
        port = self.port if self.port else self.multicast_port
        return f"ws://{self.host}:{port}"

    @property
    def connected(self) -> bool:
        """Check if client is connected"""
        return self._connected and self.websocket and not self.websocket.closed and not self._closed

    async def __aenter__(self) -> "Client":
        """Async context manager entry"""
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        """Async context manager exit"""
        await self.close()

    async def connect(self) -> None:
        """Connect to the IPC server"""
        if self._closed:
            raise NotConnected("Client has been closed")

        async with self._connection_lock:
            if self.connected:
                return

            try:
                await self._init_session()
                await self._init_websocket()
                self._connected = True
                self._reconnect_attempts = 0
                self._reconnecting = False
                log.info("Connected to IPC server at %s", self.url)
            except Exception as e:
                log.error("Failed to connect to IPC server: %s", e)
                await self._cleanup_connection()
                raise

    async def _init_session(self) -> None:
        """Initialize aiohttp session"""
        if self.session and not self.session.closed:
            await self.session.close()

        timeout = aiohttp.ClientTimeout(total=self.connection_timeout)
        self.session = aiohttp.ClientSession(
            timeout=timeout, connector=aiohttp.TCPConnector(limit=10, limit_per_host=10)
        )

    async def _init_websocket(self) -> None:
        """Initialize WebSocket connection with port discovery if needed"""
        if not self.session:
            raise NotConnected("Session not initialized")

        try:
            if not self.port:
                await self._discover_port()

            self.websocket = await self.session.ws_connect(
                self.url,
                autoping=True,
                autoclose=True,
                timeout=self.connection_timeout,
                heartbeat=self.heartbeat_interval,
                max_msg_size=10 * 1024 * 1024,
                compress=15,
            )

        except asyncio.TimeoutError:
            raise ConnectionTimeout(self.connection_timeout, "WebSocket connection")
        except aiohttp.ClientError as e:
            raise ServerConnectionRefusedError(self.host, self.port or self.multicast_port, str(e))

    async def _discover_port(self) -> None:
        """Discover server port via multicast"""
        multicast_url = f"ws://{self.host}:{self.multicast_port}"

        try:
            self.multicast = await self.session.ws_connect(
                multicast_url, autoping=False, timeout=self.connection_timeout
            )

            payload = {"connect": True, "headers": {"Authorization": self.secret_key}}

            await self.multicast.send_json(payload)
            log.debug("Multicast request sent: %r", payload)

            recv = await asyncio.wait_for(self.multicast.receive(), timeout=self.connection_timeout)

            if recv.type in (aiohttp.WSMsgType.CLOSE, aiohttp.WSMsgType.CLOSED):
                raise NotConnected("Multicast server connection failed")

            port_data = recv.json()
            self.port = port_data.get("port")

            if not self.port:
                raise NotConnected("Invalid port response from multicast server")

            log.debug("Discovered server port: %d", self.port)

        except asyncio.TimeoutError:
            raise ConnectionTimeout(self.connection_timeout, "multicast discovery")
        finally:
            if self.multicast:
                await self._safe_close_websocket(self.multicast)
                self.multicast = None

    async def _safe_close_websocket(self, ws: aiohttp.ClientWebSocketResponse) -> None:
        """Safely close a websocket connection"""
        try:
            if not ws.closed:
                await ws.close()
        except Exception as e:
            log.debug("Error closing websocket: %s", e)

    async def _cleanup_connection(self) -> None:
        """Clean up connection resources"""
        self._connected = False

        if self.websocket:
            await self._safe_close_websocket(self.websocket)
            self.websocket = None

        if self.multicast:
            await self._safe_close_websocket(self.multicast)
            self.multicast = None

    async def close(self) -> None:
        """Close the connection and cleanup resources"""
        if self._closed:
            return

        self._closed = True
        self._connected = False
        self._reconnecting = False

        for task in list(self._active_tasks):
            if not task.done():
                task.cancel()

        if self._active_tasks:
            await asyncio.gather(*self._active_tasks, return_exceptions=True)

        await self._cleanup_connection()

        if self.session and not self.session.closed:
            await self.session.close()
            self.session = None

        log.info("IPC client connection closed")

    async def _handle_reconnection(self) -> None:
        """Handle automatic reconnection logic with proper task management"""
        if not self.auto_reconnect or self._closed or self._reconnecting:
            return

        self._reconnecting = True

        try:
            if (
                self.max_reconnect_attempts > 0
                and self._reconnect_attempts >= self.max_reconnect_attempts
            ):
                log.error("Max reconnection attempts (%d) reached", self.max_reconnect_attempts)
                return

            self._reconnect_attempts += 1
            log.info(
                "Attempting reconnection %d/%d in %.1fs",
                self._reconnect_attempts,
                self.max_reconnect_attempts or float("inf"),
                self.reconnect_delay,
            )

            await asyncio.sleep(self.reconnect_delay)

            if self._closed:
                return

            try:
                await self.connect()
                log.info("Reconnection successful")
            except Exception as e:
                log.error("Reconnection failed: %s", e)

                if self.auto_reconnect and not self._closed:
                    task = asyncio.create_task(self._handle_reconnection())
                    self._active_tasks.add(task)
        finally:
            self._reconnecting = False

    async def request(
        self, endpoint: str, timeout: Optional[float] = None, **kwargs
    ) -> Dict[str, Any]:
        """
        Make a request to the IPC server process.

        Parameters
        ----------
        endpoint: str
            The endpoint to request on the server
        timeout: Optional[float]
            Request timeout in seconds. If None, uses default request_timeout
        **kwargs
            The data to send to the endpoint

        Returns
        -------
        Dict[str, Any]
            The response from the server
        """

        if self._closed:
            raise NotConnected("Client has been closed")

        await self.rate_limiter.acquire()

        if not self.connected:
            if self.auto_reconnect and not self._reconnecting:
                await self.connect()
            else:
                raise NotConnected("Not connected to server")

        actual_timeout = timeout if timeout is not None else self.request_timeout

        payload = {
            "endpoint": endpoint,
            "data": kwargs,
            "headers": {"Authorization": self.secret_key},
        }

        log.debug("Sending request to %s: %r (timeout: %ss)", endpoint, kwargs, actual_timeout)

        try:
            await self.websocket.send_json(payload)

            recv = await asyncio.wait_for(self.websocket.receive(), timeout=actual_timeout)

            if recv.type == aiohttp.WSMsgType.PING:
                log.debug("Received PING, sending PONG")
                await self.websocket.ping()
                return await self.request(endpoint, timeout=timeout, **kwargs)

            elif recv.type == aiohttp.WSMsgType.PONG:
                log.debug("Received PONG")
                return await self.request(endpoint, timeout=timeout, **kwargs)

            elif recv.type in (aiohttp.WSMsgType.CLOSE, aiohttp.WSMsgType.CLOSED):
                self._connected = False
                log.warning("WebSocket connection closed unexpectedly")

                if self.auto_reconnect and not self._closed:
                    task = asyncio.create_task(self._handle_reconnection())
                    self._active_tasks.add(task)
                    await task
                    if self.connected:
                        return await self.request(endpoint, timeout=timeout, **kwargs)

                raise NotConnected("WebSocket connection closed")

            elif recv.type == aiohttp.WSMsgType.ERROR:
                raise ServerConnectionRefusedError(
                    self.host, self.port or self.multicast_port, f"WebSocket error: {recv.data}"
                )

            response = recv.json()
            log.debug("Received response: %r", response)

            if isinstance(response, dict) and "error" in response:
                error_code = response.get("code", 500)
                error_msg = response["error"]

                if error_code == 404:
                    raise NoEndpointFoundError(endpoint)
                elif error_code == 403:
                    raise AuthenticationError(error_msg)
                elif error_code == 429:
                    retry_after = response.get("retry_after", 1.0)
                    raise RateLimited(retry_after, 0, 0)
                else:
                    raise IPCError(error_msg, error_code)

            return response

        except asyncio.TimeoutError:
            log.error("Request timeout after %ss for endpoint: %s", actual_timeout, endpoint)
            raise ConnectionTimeout(actual_timeout, f"request to {endpoint}")
        except aiohttp.ClientError as e:
            self._connected = False
            log.error("Client error during request: %s", e)

            if self.auto_reconnect and not self._closed:
                task = asyncio.create_task(self._handle_reconnection())
                self._active_tasks.add(task)
                await task
                if self.connected:
                    return await self.request(endpoint, timeout=timeout, **kwargs)

            raise ServerConnectionRefusedError(self.host, self.port or self.multicast_port, str(e))

    def __del__(self) -> None:
        """Cleanup on deletion"""
        if not self._closed and self.session and not self.session.closed:
            try:
                loop = asyncio.get_event_loop()
                if not loop.is_closed():
                    loop.create_task(self.close())
            except Exception:
                pass
