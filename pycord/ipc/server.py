"""
Modern IPC Server with rate limiting and graceful shutdown
"""

import asyncio
import logging
import time
from collections import defaultdict, deque
from typing import Any, Callable, Dict, Optional, Set, Union

import aiohttp.web
import discord

from .errors import *

log = logging.getLogger(__name__)


def route(name: Optional[str] = None) -> Callable:
    """
    Used to register a coroutine as an endpoint when you don't have
    access to an instance of :class:`.Server`

    Parameters
    ----------
    name: Optional[str]
        The endpoint name. If not provided the method name will be used.
    """

    def decorator(func: Callable) -> Callable:
        endpoint_name = name if name is not None else func.__name__
        Server.ROUTES[endpoint_name] = func
        return func

    return decorator


class IpcServerResponse:
    """Represents a request to an IPC server endpoint"""

    def __init__(self, data: Dict[str, Any]) -> None:
        self._json = data
        self.length = len(str(data))
        self.endpoint: str = data["endpoint"]

        for key, value in data["data"].items():
            setattr(self, key, value)

    def to_json(self) -> Dict[str, Any]:
        """Convert response to JSON-serializable dict"""
        return self._json

    def __repr__(self) -> str:
        return f"<IpcServerResponse endpoint={self.endpoint} length={self.length}>"

    def __str__(self) -> str:
        return self.__repr__()


class ServerRateLimiter:
    """Rate limiter for server endpoints"""

    def __init__(self, max_requests: int, window_seconds: int) -> None:
        self.max_requests = max_requests
        self.window_seconds = window_seconds
        self.client_requests: Dict[str, deque] = defaultdict(deque)

    def check_rate_limit(self, client_id: str) -> Optional[float]:
        """
        Check if client is rate limited

        Returns
        -------
        Optional[float]
            Retry after time in seconds if rate limited, None otherwise
        """
        now = time.time()
        requests = self.client_requests[client_id]

        while requests and requests[0] <= now - self.window_seconds:
            requests.popleft()

        if len(requests) >= self.max_requests:
            oldest_request = requests[0]
            retry_after = (oldest_request + self.window_seconds) - now
            return max(0, retry_after)

        requests.append(now)
        return None


class Server:
    """
    The IPC server for receiving requests from clients.

    Parameters
    ----------
    bot: Any
        Your bot instance (discord.py Bot or similar)
    host: str
        The host to run the IPC Server on, defaults to localhost
    port: int
        The port to run the IPC Server on, defaults to 8765
    secret_key: Optional[Union[str, bytes]]
        A secret key for authentication
    do_multicast: bool
        Turn multicasting on/off, defaults to True
    multicast_port: int
        The port to run the multicasting server on, defaults to 20000
    max_requests: int
        Maximum requests per client per window, defaults to 100
    rate_window: int
        Rate limiting window in seconds, defaults to 60
    websocket_timeout: float
        Timeout for websocket operations in seconds, defaults to 300.0
    websocket_receive_timeout: float
        Timeout for receiving messages in seconds, defaults to 300.0
    websocket_heartbeat: float
        Heartbeat interval in seconds, defaults to 60.0
    max_message_size: int
        Maximum message size in bytes, defaults to 10MB
    """

    ROUTES: Dict[str, Callable] = {}

    def __init__(
        self,
        bot: discord.Client,
        host: str = "localhost",
        port: int = 8765,
        secret_key: Optional[Union[str, bytes]] = None,
        do_multicast: bool = True,
        multicast_port: int = 20000,
        max_requests: int = 100,
        rate_window: int = 60,
        websocket_timeout: float = 300.0,
        websocket_receive_timeout: float = 300.0,
        websocket_heartbeat: float = 60.0,
        max_message_size: int = 10 * 1024 * 1024,
    ) -> None:
        self.bot = bot
        self.loop = bot.loop if hasattr(bot, "loop") else asyncio.get_event_loop()

        self.secret_key = secret_key
        self.host = host
        self.port = port
        self.websocket_timeout = websocket_timeout
        self.websocket_receive_timeout = websocket_receive_timeout
        self.websocket_heartbeat = websocket_heartbeat
        self.max_message_size = max_message_size

        self._server: Optional[aiohttp.web.Application] = None
        self._multicast_server: Optional[aiohttp.web.Application] = None
        self._app_runner: Optional[aiohttp.web.AppRunner] = None
        self._multicast_runner: Optional[aiohttp.web.AppRunner] = None
        self._site: Optional[aiohttp.web.TCPSite] = None
        self._multicast_site: Optional[aiohttp.web.TCPSite] = None
        self._running = False

        self.do_multicast = do_multicast
        self.multicast_port = multicast_port

        self.rate_limiter = ServerRateLimiter(max_requests, rate_window)

        self.endpoints: Dict[str, Callable] = {}

        self._active_connections: Set[aiohttp.web.WebSocketResponse] = set()

    @property
    def running(self) -> bool:
        """Check if server is running"""
        return self._running

    def route(self, name: Optional[str] = None) -> Callable:
        """
        Used to register a coroutine as an endpoint when you have
        access to an instance of :class:`.Server`.

        Parameters
        ----------
        name: Optional[str]
            The endpoint name. If not provided the method name will be used.
        """

        def decorator(func: Callable) -> Callable:
            endpoint_name = name if name is not None else func.__name__
            self.endpoints[endpoint_name] = func
            return func

        return decorator

    def update_endpoints(self) -> None:
        """Called internally to update the server's endpoints for cog routes."""
        self.endpoints = {**self.endpoints, **self.ROUTES}
        self.ROUTES = {}

    async def handle_accept(self, request: aiohttp.web.Request) -> aiohttp.web.WebSocketResponse:
        """
        Handles websocket requests from the client process.
        """
        self.update_endpoints()

        # 修改：增加更長的心跳和更大的消息限制
        websocket = aiohttp.web.WebSocketResponse(
            heartbeat=self.websocket_heartbeat,
            receive_timeout=self.websocket_receive_timeout,
            timeout=self.websocket_timeout,
            max_msg_size=self.max_message_size,
            autoping=True,
            autoclose=True,
            compress=True,
        )
        await websocket.prepare(request)

        self._active_connections.add(websocket)
        client_id = f"{request.remote}:{id(websocket)}"

        log.info("New IPC connection from %s", request.remote)

        try:
            async for message in websocket:
                if message.type == aiohttp.WSMsgType.TEXT:
                    try:
                        request_data = message.json()
                    except ValueError:
                        await websocket.send_json({"error": "Invalid JSON", "code": 400})
                        continue

                    log.debug("IPC Server < %r", request_data)

                    try:
                        response = await asyncio.wait_for(
                            self._process_request(request_data, client_id),
                            timeout=self.websocket_timeout,
                        )
                    except asyncio.TimeoutError:
                        response = {"error": "Request processing timeout", "code": 408}
                        log.error(
                            "Request processing timeout for endpoint: %s",
                            request_data.get("endpoint", "unknown"),
                        )

                    try:
                        await websocket.send_json(response)
                        log.debug("IPC Server > %r", response)
                    except TypeError as error:
                        await self._handle_json_error(websocket, error)

                elif message.type == aiohttp.WSMsgType.ERROR:
                    exception = websocket.exception()
                    log.error(
                        "WebSocket error from %s: %s (%s)",
                        request.remote,
                        exception if exception else "Unknown error",
                        type(exception).__name__ if exception else "Unknown",
                    )
                    break

        except Exception as e:
            log.error(
                "Unexpected error in websocket handler from %s: %s (%s)",
                request.remote,
                e,
                type(e).__name__,
            )
        finally:
            self._active_connections.discard(websocket)
            log.info("IPC connection closed from %s", request.remote)

        return websocket

    async def _process_request(
        self, request_data: Dict[str, Any], client_id: str
    ) -> Dict[str, Any]:
        """Process an individual IPC request"""

        retry_after = self.rate_limiter.check_rate_limit(client_id)
        if retry_after is not None:
            return {"error": "Rate limit exceeded", "code": 429, "retry_after": retry_after}

        headers = request_data.get("headers", {})
        if not headers or headers.get("Authorization") != self.secret_key:
            log.warning("Unauthorized request from %s", client_id)
            return {"error": "Invalid or no token provided", "code": 403}

        endpoint = request_data.get("endpoint")
        if not endpoint or endpoint not in self.endpoints:
            log.warning("Invalid endpoint '%s' requested from %s", endpoint, client_id)
            return {"error": "Invalid or no endpoint given", "code": 404}

        try:
            server_response = IpcServerResponse(request_data)

            try:
                qualname_parts = self.endpoints[endpoint].__qualname__.split(".")
                if len(qualname_parts) > 1:
                    cog_name = qualname_parts[0]
                    attempted_cls = (
                        self.bot.cogs.get(cog_name) if hasattr(self.bot, "cogs") else None
                    )

                    if attempted_cls:
                        arguments = (attempted_cls, server_response)
                    else:
                        arguments = (server_response,)
                else:
                    arguments = (server_response,)
            except AttributeError:
                arguments = (server_response,)

            result = await self.endpoints[endpoint](*arguments)
            return result

        except Exception as error:
            log.error("Error executing endpoint '%s': %s", endpoint, error)

            if hasattr(self.bot, "dispatch"):
                self.bot.dispatch("ipc_error", endpoint, error)

            return {
                "error": f"IPC route raised error of type {type(error).__name__}",
                "code": 500,
                "details": str(error) if log.level <= logging.DEBUG else None,
            }

    async def _handle_json_error(
        self, websocket: aiohttp.web.WebSocketResponse, error: TypeError
    ) -> None:
        """Handle JSON serialization errors"""
        if str(error).startswith("Object of type") and str(error).endswith(
            "is not JSON serializable"
        ):
            error_response = (
                "IPC route returned values which are not able to be sent over sockets. "
                "If you are trying to send a discord.py object, please only send the data you need."
            )
            log.error(error_response)

            response = {"error": error_response, "code": 500}
            await websocket.send_json(response)
            log.debug("IPC Server > %r", response)

            raise JSONEncodeError(str(type(error)), error_response)

    async def handle_multicast(self, request: aiohttp.web.Request) -> aiohttp.web.WebSocketResponse:
        """
        Handles multicasting websocket requests from the client.

        Parameters
        ----------
        request: aiohttp.web.Request
            The request made by the client, parsed by aiohttp.
        """
        log.info("Multicast connection from %s", request.remote)
        websocket = aiohttp.web.WebSocketResponse()
        await websocket.prepare(request)

        try:
            async for message in websocket:
                if message.type == aiohttp.WSMsgType.TEXT:
                    try:
                        request_data = message.json()
                    except ValueError:
                        await websocket.send_json({"error": "Invalid JSON", "code": 400})
                        continue

                    log.debug("Multicast Server < %r", request_data)

                    headers = request_data.get("headers", {})
                    if not headers or headers.get("Authorization") != self.secret_key:
                        response = {"error": "Invalid or no token provided", "code": 403}
                    else:
                        response = {
                            "message": "Connection success",
                            "port": self.port,
                            "code": 200,
                        }

                    log.debug("Multicast Server > %r", response)
                    await websocket.send_json(response)
                    break

                elif message.type == aiohttp.WSMsgType.ERROR:
                    exception = websocket.exception()
                    log.error(
                        "Multicast WebSocket error from %s: %s (%s)",
                        request.remote,
                        exception if exception else "Unknown error",
                        type(exception).__name__ if exception else "Unknown",
                    )
                    break

        except Exception as e:
            log.error("Unexpected error in multicast handler: %s", e)

        return websocket

    async def start(self) -> None:
        """Start the IPC server"""
        if self._running:
            raise ServerNotRunning("Server is already running")

        log.info("Starting IPC server on %s:%d", self.host, self.port)

        self._server = aiohttp.web.Application()
        self._server.router.add_route("GET", "/", self.handle_accept)

        if self.do_multicast:
            log.info("Starting multicast server on %s:%d", self.host, self.multicast_port)
            self._multicast_server = aiohttp.web.Application()
            self._multicast_server.router.add_route("GET", "/", self.handle_multicast)

        try:
            if self.do_multicast and self._multicast_server:
                self._multicast_runner = aiohttp.web.AppRunner(self._multicast_server)
                await self._multicast_runner.setup()
                self._multicast_site = aiohttp.web.TCPSite(
                    self._multicast_runner, self.host, self.multicast_port
                )
                await self._multicast_site.start()

            self._app_runner = aiohttp.web.AppRunner(self._server)
            await self._app_runner.setup()
            self._site = aiohttp.web.TCPSite(self._app_runner, self.host, self.port)
            await self._site.start()

            self._running = True

            if hasattr(self.bot, "dispatch"):
                self.bot.dispatch("ipc_ready")

            log.info("IPC server started successfully")

        except Exception as e:
            log.error("Failed to start IPC server: %s", e)
            await self.stop()
            raise ServerConnectionRefusedError(self.host, self.port, f"Failed to start server: {e}")

    async def stop(self) -> None:
        """Stop the IPC server gracefully"""
        if not self._running:
            return

        log.info("Stopping IPC server...")
        self._running = False

        if self._active_connections:
            log.info("Closing %d active connections", len(self._active_connections))
            close_tasks = []
            for ws in self._active_connections.copy():
                if not ws.closed:
                    close_tasks.append(
                        ws.close(code=aiohttp.WSMsgType.CLOSE, message=b"Server shutting down")
                    )

            if close_tasks:
                await asyncio.gather(*close_tasks, return_exceptions=True)

            self._active_connections.clear()

        cleanup_tasks = []

        if self._site:
            cleanup_tasks.append(self._site.stop())

        if self._multicast_site:
            cleanup_tasks.append(self._multicast_site.stop())

        if cleanup_tasks:
            await asyncio.gather(*cleanup_tasks, return_exceptions=True)

        if self._app_runner:
            await self._app_runner.cleanup()
            self._app_runner = None

        if self._multicast_runner:
            await self._multicast_runner.cleanup()
            self._multicast_runner = None

        self._site = None
        self._multicast_site = None
        self._server = None
        self._multicast_server = None

        log.info("IPC server stopped")

    async def __aenter__(self) -> "Server":
        """Async context manager entry"""
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        """Async context manager exit"""
        await self.stop()

    def __del__(self) -> None:
        """Cleanup on deletion"""
        if self._running and self.loop and not self.loop.is_closed():
            try:
                self.loop.create_task(self.stop())
            except Exception:
                pass
