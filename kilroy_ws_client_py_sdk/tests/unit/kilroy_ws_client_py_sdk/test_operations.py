import asyncio
from socket import socket
from typing import Awaitable, Callable, List

import pytest
import pytest_asyncio
import websockets
from websockets.exceptions import WebSocketException
from websockets.legacy.server import WebSocketServerProtocol, serve

from kilroy_ws_client_py_sdk import AppError, JSON, ProtocolError, operations
from kilroy_ws_client_py_sdk.messages import (
    AppErrorMessage,
    DataMessage,
    ReplyMessage,
    RequestMessage,
    StreamEndMessage,
)


class TestServer:
    def __init__(
        self, handler: Callable[[WebSocketServerProtocol], Awaitable[None]]
    ) -> None:
        self._host = "localhost"
        self._server = serve(handler, self._host)

    @property
    def host(self) -> str:
        return self._host

    @property
    def port(self) -> int:
        return next(iter(self._server.ws_server.sockets)).getsockname()[1]

    @property
    def url(self) -> str:
        return f"ws://{self.host}:{self.port}"

    async def __aenter__(self):
        await self._server.__aenter__()
        while True:
            try:
                async with websockets.connect(self.url):
                    pass
            except ConnectionError:
                pass
            except websockets.InvalidStatusCode:
                break
            else:
                break
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return await self._server.__aexit__(exc_type, exc, tb)


@pytest.fixture
def busy_port() -> int:
    s = socket()
    s.bind(("", 0))
    port = s.getsockname()[1]
    yield port
    s.close()


class TestGet:
    @pytest.fixture
    def payload(self) -> JSON:
        return {"foo": "bar"}

    @pytest_asyncio.fixture
    async def working_server(self, payload: JSON) -> TestServer:
        async def handler(websocket: WebSocketServerProtocol) -> None:
            data = DataMessage(payload=payload).json()
            await websocket.send(data)

        async with TestServer(handler) as server:
            yield server

    @pytest_asyncio.fixture
    async def error_message_server(self) -> TestServer:
        async def handler(websocket: WebSocketServerProtocol) -> None:
            data = AppErrorMessage(code=123, reason="foo").json()
            await websocket.send(data)

        async with TestServer(handler) as server:
            yield server

    @pytest_asyncio.fixture
    async def invalid_message_server(self) -> TestServer:
        async def handler(websocket: WebSocketServerProtocol) -> None:
            await websocket.send("foo")

        async with TestServer(handler) as server:
            yield server

    @pytest.mark.asyncio
    async def test_get_returns_correct_payload(
        self, working_server: TestServer, payload: JSON
    ) -> None:
        server_payload = await operations.get(working_server.url)
        assert server_payload == payload

    @pytest.mark.asyncio
    async def test_get_raises_on_connection_refused(
        self, busy_port: int
    ) -> None:
        # noinspection PyTypeChecker
        with pytest.raises((OSError, asyncio.TimeoutError)):
            await operations.get(f"ws://localhost:{busy_port}")

    @pytest.mark.asyncio
    async def test_get_raises_on_invalid_uri(self) -> None:
        with pytest.raises(WebSocketException):
            await operations.get("foo")

    @pytest.mark.asyncio
    async def test_get_raises_on_error_message(
        self, error_message_server: TestServer
    ) -> None:
        with pytest.raises(AppError):
            await operations.get(error_message_server.url)

    @pytest.mark.asyncio
    async def test_get_raises_on_invalid_message(
        self, invalid_message_server: TestServer
    ) -> None:
        with pytest.raises(ProtocolError):
            await operations.get(invalid_message_server.url)


class TestSubscribe:
    @pytest.fixture
    def payloads(self) -> List[JSON]:
        return [{"foo": "bar"}, {"bar": "foo"}]

    @pytest_asyncio.fixture
    async def working_server(self, payloads: List[JSON]) -> TestServer:
        async def handler(websocket: WebSocketServerProtocol) -> None:
            for payload in payloads:
                data = DataMessage(payload=payload).json()
                await websocket.send(data)

        async with TestServer(handler) as server:
            yield server

    @pytest_asyncio.fixture
    async def error_message_server(self) -> TestServer:
        async def handler(websocket: WebSocketServerProtocol) -> None:
            data = AppErrorMessage(code=123, reason="foo").json()
            await websocket.send(data)

        async with TestServer(handler) as server:
            yield server

    @pytest_asyncio.fixture
    async def invalid_message_server(self) -> TestServer:
        async def handler(websocket: WebSocketServerProtocol) -> None:
            await websocket.send("foo")

        async with TestServer(handler) as server:
            yield server

    @pytest.mark.asyncio
    async def test_subscribe_returns_correct_payloads(
        self, working_server: TestServer, payloads: List[JSON]
    ) -> None:
        server_payloads = [
            payload
            async for payload in operations.subscribe(working_server.url)
        ]
        assert server_payloads == payloads

    @pytest.mark.asyncio
    async def test_subscribe_raises_on_connection_refused(
        self, busy_port: int
    ) -> None:
        # noinspection PyTypeChecker
        with pytest.raises((OSError, asyncio.TimeoutError)):
            async for _ in operations.subscribe(f"ws://localhost:{busy_port}"):
                pass

    @pytest.mark.asyncio
    async def test_subscribe_raises_on_invalid_uri(self) -> None:
        with pytest.raises(WebSocketException):
            async for _ in operations.subscribe("foo"):
                pass

    @pytest.mark.asyncio
    async def test_subscribe_raises_on_error_message(
        self, error_message_server: TestServer
    ) -> None:
        with pytest.raises(AppError):
            async for _ in operations.subscribe(error_message_server.url):
                pass

    @pytest.mark.asyncio
    async def test_subscribe_raises_on_invalid_message(
        self, invalid_message_server: TestServer
    ) -> None:
        with pytest.raises(ProtocolError):
            async for _ in operations.subscribe(invalid_message_server.url):
                pass


class TestRequest:
    @pytest.fixture
    def payload(self) -> JSON:
        return {"foo": "bar"}

    @pytest_asyncio.fixture
    async def working_server(self, payload: JSON) -> TestServer:
        async def handler(websocket: WebSocketServerProtocol) -> None:
            incoming_data = await websocket.recv()
            request = RequestMessage.parse_raw(incoming_data)
            reply = ReplyMessage(request=request.id, payload=payload)
            outgoing_data = reply.json()
            await websocket.send(outgoing_data)

        async with TestServer(handler) as server:
            yield server

    @pytest_asyncio.fixture
    async def error_message_server(self) -> TestServer:
        async def handler(websocket: WebSocketServerProtocol) -> None:
            await websocket.recv()
            data = AppErrorMessage(code=123, reason="foo").json()
            await websocket.send(data)

        async with TestServer(handler) as server:
            yield server

    @pytest_asyncio.fixture
    async def invalid_message_server(self) -> TestServer:
        async def handler(websocket: WebSocketServerProtocol) -> None:
            await websocket.recv()
            await websocket.send("foo")

        async with TestServer(handler) as server:
            yield server

    @pytest.mark.asyncio
    async def test_request_returns_correct_payload(
        self, working_server: TestServer, payload: JSON
    ) -> None:
        reply_payload = await operations.request(
            working_server.url, payload={}
        )
        assert reply_payload == payload

    @pytest.mark.asyncio
    async def test_request_raises_on_connection_refused(
        self, busy_port: int
    ) -> None:
        # noinspection PyTypeChecker
        with pytest.raises((OSError, asyncio.TimeoutError)):
            await operations.request(f"ws://localhost:{busy_port}", payload={})

    @pytest.mark.asyncio
    async def test_request_raises_on_invalid_uri(self) -> None:
        with pytest.raises(WebSocketException):
            await operations.request("foo", payload={})

    @pytest.mark.asyncio
    async def test_request_raises_on_error_message(
        self, error_message_server: TestServer
    ) -> None:
        with pytest.raises(AppError):
            await operations.request(error_message_server.url, payload={})

    @pytest.mark.asyncio
    async def test_request_raises_on_invalid_message(
        self, invalid_message_server: TestServer
    ) -> None:
        with pytest.raises(ProtocolError):
            await operations.request(invalid_message_server.url, payload={})


class TestRequestStream:
    @pytest.fixture
    def payloads(self) -> List[JSON]:
        return [{"foo": "bar"}, {"bar": "foo"}]

    @pytest_asyncio.fixture
    async def working_server(self, payloads: List[JSON]) -> TestServer:
        async def handler(websocket: WebSocketServerProtocol) -> None:
            incoming_data = await websocket.recv()
            request = RequestMessage.parse_raw(incoming_data)
            for payload in payloads:
                reply = ReplyMessage(request=request.id, payload=payload)
                outgoing_data = reply.json()
                await websocket.send(outgoing_data)
            await websocket.send(StreamEndMessage().json())

        async with TestServer(handler) as server:
            yield server

    @pytest_asyncio.fixture
    async def error_message_server(self) -> TestServer:
        async def handler(websocket: WebSocketServerProtocol) -> None:
            await websocket.recv()
            data = AppErrorMessage(code=123, reason="foo").json()
            await websocket.send(data)

        async with TestServer(handler) as server:
            yield server

    @pytest_asyncio.fixture
    async def invalid_message_server(self) -> TestServer:
        async def handler(websocket: WebSocketServerProtocol) -> None:
            await websocket.recv()
            await websocket.send("foo")

        async with TestServer(handler) as server:
            yield server

    @pytest.mark.asyncio
    async def test_request_stream_returns_correct_payloads(
        self, working_server: TestServer, payloads: List[JSON]
    ) -> None:
        reply_payloads = [
            payload
            async for payload in operations.request_stream(
                working_server.url, payload={}
            )
        ]
        assert reply_payloads == payloads

    @pytest.mark.asyncio
    async def test_request_stream_raises_on_connection_refused(
        self, busy_port: int
    ) -> None:
        # noinspection PyTypeChecker
        with pytest.raises((OSError, asyncio.TimeoutError)):
            async for _ in operations.request_stream(
                f"ws://localhost:{busy_port}", payload={}
            ):
                pass

    @pytest.mark.asyncio
    async def test_request_stream_raises_on_invalid_uri(self) -> None:
        with pytest.raises(WebSocketException):
            async for _ in operations.request_stream("foo", payload={}):
                pass

    @pytest.mark.asyncio
    async def test_request_stream_raises_on_error_message(
        self, error_message_server: TestServer
    ) -> None:
        with pytest.raises(AppError):
            async for _ in operations.request_stream(
                error_message_server.url, payload={}
            ):
                pass

    @pytest.mark.asyncio
    async def test_request_stream_raises_on_invalid_message(
        self, invalid_message_server: TestServer
    ) -> None:
        with pytest.raises(ProtocolError):
            async for _ in operations.request_stream(
                invalid_message_server.url, payload={}
            ):
                pass
