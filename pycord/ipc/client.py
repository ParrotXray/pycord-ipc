"""
Enhanced IPC Client with request ID matching to prevent response mixing
"""

import asyncio
import logging
import time
import uuid
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
    Enhanced IPC Client with request ID matching to prevent response mixing
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
        
        # Enhanced: Request tracking
        self._pending_requests: Dict[str, asyncio.Future] = {}
        self._message_handler_task: Optional[asyncio.Task] = None

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
                
                # Start message handler
                self._message_handler_task = asyncio.create_task(self._handle_messages())
                self._active_tasks.add(self._message_handler_task)
                
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

    async def _handle_messages(self) -> None:
        """Handle incoming WebSocket messages with request ID matching"""
        try:
            async for message in self.websocket:
                if message.type == aiohttp.WSMsgType.TEXT:
                    try:
                        response = message.json()
                        log.debug("Received response: %r", response)
                        
                        # Extract request ID from response
                        request_id = response.get("request_id")
                        if request_id and request_id in self._pending_requests:
                            future = self._pending_requests.pop(request_id)
                            if not future.done():
                                future.set_result(response)
                        else:
                            log.warning("Received response with unknown or missing request_id: %s", request_id)
                            
                    except ValueError as e:
                        log.error("Failed to parse JSON response: %s", e)
                        
                elif message.type == aiohttp.WSMsgType.PING:
                    log.debug("Received PING, sending PONG")
                    await self.websocket.ping()
                    
                elif message.type == aiohttp.WSMsgType.PONG:
                    log.debug("Received PONG")
                    
                elif message.type in (aiohttp.WSMsgType.CLOSE, aiohttp.WSMsgType.CLOSED):
                    log.warning("WebSocket connection closed")
                    self._connected = False
                    
                    # Cancel all pending requests
                    for request_id, future in self._pending_requests.items():
                        if not future.done():
                            future.set_exception(NotConnected("WebSocket connection closed"))
                    self._pending_requests.clear()
                    
                    if self.auto_reconnect and not self._closed:
                        task = asyncio.create_task(self._handle_reconnection())
                        self._active_tasks.add(task)
                    break
                    
                elif message.type == aiohttp.WSMsgType.ERROR:
                    log.error("WebSocket error: %s", message.data)
                    break
                    
        except Exception as e:
            log.error("Error in message handler: %s", e)
            self._connected = False

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
        
        # Cancel message handler
        if self._message_handler_task and not self._message_handler_task.done():
            self._message_handler_task.cancel()
            try:
                await self._message_handler_task
            except asyncio.CancelledError:
                pass
        
        # Cancel all pending requests
        for request_id, future in self._pending_requests.items():
            if not future.done():
                future.set_exception(NotConnected("Connection lost"))
        self._pending_requests.clear()

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
        """Handle automatic reconnection logic"""
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
        Make a request to the IPC server process with request ID tracking.
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
        
        # Generate unique request ID
        request_id = str(uuid.uuid4())

        payload = {
            "request_id": request_id,  # Add request ID
            "endpoint": endpoint,
            "data": kwargs,
            "headers": {"Authorization": self.secret_key},
        }

        log.debug("Sending request to %s: %r (timeout: %ss, request_id: %s)", 
                 endpoint, kwargs, actual_timeout, request_id)

        # Create future for this request
        future = asyncio.Future()
        self._pending_requests[request_id] = future

        try:
            await self.websocket.send_json(payload)
            
            # Wait for response with timeout
            response = await asyncio.wait_for(future, timeout=actual_timeout)
            
            # Process response
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
            # Clean up pending request
            self._pending_requests.pop(request_id, None)
            log.error("Request timeout after %ss for endpoint: %s", actual_timeout, endpoint)
            raise ConnectionTimeout(actual_timeout, f"request to {endpoint}")
            
        except aiohttp.ClientError as e:
            # Clean up pending request
            self._pending_requests.pop(request_id, None)
            self._connected = False
            log.error("Client error during request: %s", e)

            if self.auto_reconnect and not self._closed:
                task = asyncio.create_task(self._handle_reconnection())
                self._active_tasks.add(task)

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