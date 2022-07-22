import asyncio
from contextlib import asynccontextmanager
from socket import socket
from typing import Awaitable, Callable, List
from uuid import UUID

import pytest
import pytest_asyncio
import websockets
from pydantic import ValidationError
from websockets.exceptions import WebSocketException
from websockets.legacy.server import WebSocketServerProtocol, serve

from kilroy_ws_client_py_sdk import AppError, Client, JSON, ProtocolError
from kilroy_ws_client_py_sdk.messages import (
    AppErrorMessage,
    DataMessage,
    ProtocolErrorMessage,
    StartMessage,
    StopMessage,
    StreamEndMessage,
)
from kilroy_ws_client_py_sdk.protocol import parse_message


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


async def receive_start_message(
    websocket: websockets.WebSocketServerProtocol,
) -> StartMessage:
    data = await websocket.recv()
    return parse_message(data, StartMessage)


async def send_stop_message(
    websocket: websockets.WebSocketServerProtocol, chat_id: UUID
) -> None:
    await websocket.send(StopMessage(chat_id=chat_id).json())


@asynccontextmanager
async def chat(websocket: websockets.WebSocketServerProtocol) -> UUID:
    try:
        start = await receive_start_message(websocket)
    except ProtocolError as e:
        await websocket.send(ProtocolErrorMessage(reason=str(e)))
        return

    yield start.chat_id

    await send_stop_message(websocket, start.chat_id)


@pytest.fixture
def busy_port() -> int:
    s = socket()
    s.bind(("", 0))
    port = s.getsockname()[1]
    yield port
    s.close()


@pytest_asyncio.fixture
async def error_message_server() -> TestServer:
    async def handler(websocket: WebSocketServerProtocol) -> None:
        async with chat(websocket) as chat_id:
            data = AppErrorMessage(
                chat_id=chat_id, code=123, reason="foo"
            ).json()
            await websocket.send(data)

    async with TestServer(handler) as server:
        yield server


@pytest_asyncio.fixture
async def invalid_message_server() -> TestServer:
    async def handler(websocket: WebSocketServerProtocol) -> None:
        async with chat(websocket):
            await websocket.send("foo")

    async with TestServer(handler) as server:
        yield server


class TestGet:
    @pytest.fixture
    def payload(self) -> JSON:
        return {"foo": "bar"}

    @pytest_asyncio.fixture
    async def working_server(self, payload: JSON) -> TestServer:
        async def handler(websocket: WebSocketServerProtocol) -> None:
            async with chat(websocket) as chat_id:
                data = DataMessage(chat_id=chat_id, payload=payload).json()
                await websocket.send(data)

        async with TestServer(handler) as server:
            yield server

    @pytest.mark.asyncio
    async def test_get_returns_correct_payload(
        self, working_server: TestServer, payload: JSON
    ) -> None:
        client = Client(working_server.url)
        server_payload = await client.get("/")
        assert server_payload == payload

    @pytest.mark.asyncio
    async def test_get_raises_on_connection_refused(
        self, busy_port: int
    ) -> None:
        # noinspection PyTypeChecker
        with pytest.raises((OSError, asyncio.TimeoutError)):
            client = Client(f"ws://localhost:{busy_port}")
            await client.get("/")

    @pytest.mark.asyncio
    async def test_get_raises_on_invalid_uri(self) -> None:
        with pytest.raises(WebSocketException):
            client = Client("foo")
            await client.get("foo")

    # noinspection PyShadowingNames
    @pytest.mark.asyncio
    async def test_get_raises_on_error_message(
        self, error_message_server: TestServer
    ) -> None:
        with pytest.raises(AppError):
            client = Client(error_message_server.url)
            await client.get("/")

    # noinspection PyShadowingNames
    @pytest.mark.asyncio
    async def test_get_raises_on_invalid_message(
        self, invalid_message_server: TestServer
    ) -> None:
        with pytest.raises(ProtocolError):
            client = Client(invalid_message_server.url)
            await client.get("/")


class TestSubscribe:
    @pytest.fixture
    def payloads(self) -> List[JSON]:
        return [{"foo": "bar"}, {"bar": "foo"}]

    @pytest_asyncio.fixture
    async def working_server(self, payloads: List[JSON]) -> TestServer:
        async def handler(websocket: WebSocketServerProtocol) -> None:
            async with chat(websocket) as chat_id:
                for payload in payloads:
                    data = DataMessage(chat_id=chat_id, payload=payload).json()
                    await websocket.send(data)
                await websocket.send(StreamEndMessage(chat_id=chat_id).json())

        async with TestServer(handler) as server:
            yield server

    @pytest.mark.asyncio
    async def test_subscribe_returns_correct_payloads(
        self, working_server: TestServer, payloads: List[JSON]
    ) -> None:
        client = Client(working_server.url)
        server_payloads = [payload async for payload in client.subscribe("/")]
        assert server_payloads == payloads

    @pytest.mark.asyncio
    async def test_subscribe_raises_on_connection_refused(
        self, busy_port: int
    ) -> None:
        # noinspection PyTypeChecker
        with pytest.raises((OSError, asyncio.TimeoutError)):
            client = Client(f"ws://localhost:{busy_port}")
            async for _ in client.subscribe("/"):
                pass

    @pytest.mark.asyncio
    async def test_subscribe_raises_on_invalid_uri(self) -> None:
        with pytest.raises(WebSocketException):
            async for _ in Client("foo").subscribe("foo"):
                pass

    # noinspection PyShadowingNames
    @pytest.mark.asyncio
    async def test_subscribe_raises_on_error_message(
        self, error_message_server: TestServer
    ) -> None:
        with pytest.raises(AppError):
            async for _ in Client(error_message_server.url).subscribe("/"):
                pass

    # noinspection PyShadowingNames
    @pytest.mark.asyncio
    async def test_subscribe_raises_on_invalid_message(
        self, invalid_message_server: TestServer
    ) -> None:
        with pytest.raises(ProtocolError):
            async for _ in Client(invalid_message_server.url).subscribe("/"):
                pass


class TestRequest:
    @pytest.fixture
    def payload(self) -> JSON:
        return {"foo": "bar"}

    @pytest_asyncio.fixture
    async def working_server(self, payload: JSON) -> TestServer:
        async def handler(websocket: WebSocketServerProtocol) -> None:
            async with chat(websocket) as chat_id:
                await websocket.recv()
                reply = DataMessage(chat_id=chat_id, payload=payload)
                await websocket.send(reply.json())

        async with TestServer(handler) as server:
            yield server

    @pytest.mark.asyncio
    async def test_request_returns_correct_payload(
        self, working_server: TestServer, payload: JSON
    ) -> None:
        client = Client(working_server.url)
        reply_payload = await client.request("/", data={})
        assert reply_payload == payload

    @pytest.mark.asyncio
    async def test_request_raises_on_connection_refused(
        self, busy_port: int
    ) -> None:
        # noinspection PyTypeChecker
        with pytest.raises((OSError, asyncio.TimeoutError)):
            await Client(f"ws://localhost:{busy_port}").request("/", data={})

    @pytest.mark.asyncio
    async def test_request_raises_on_invalid_uri(self) -> None:
        with pytest.raises(WebSocketException):
            await Client("foo").request("/", data={})

    # noinspection PyShadowingNames
    @pytest.mark.asyncio
    async def test_request_raises_on_error_message(
        self, error_message_server: TestServer
    ) -> None:
        with pytest.raises(AppError):
            await Client(error_message_server.url).request("/", data={})

    # noinspection PyShadowingNames
    @pytest.mark.asyncio
    async def test_request_raises_on_invalid_message(
        self, invalid_message_server: TestServer
    ) -> None:
        with pytest.raises(ProtocolError):
            client = Client(invalid_message_server.url)
            await client.request("/", data={})


class TestRequestStreamIn:
    @pytest.fixture
    def payload(self) -> JSON:
        return {"foo": "bar"}

    @pytest_asyncio.fixture
    async def working_server(self, payload: JSON) -> TestServer:
        async def handler(websocket: WebSocketServerProtocol) -> None:
            async with chat(websocket) as chat_id:
                async for data in websocket:
                    try:
                        DataMessage.parse_raw(data)
                    except ValidationError:
                        break
                reply = DataMessage(chat_id=chat_id, payload=payload)
                await websocket.send(reply.json())

        async with TestServer(handler) as server:
            yield server

    @pytest.mark.asyncio
    async def test_request_stream_in_returns_correct_payload(
        self, working_server: TestServer, payload: JSON
    ) -> None:
        client = Client(working_server.url)
        reply_payload = await client.request_stream_in("/", data=[{}])
        assert reply_payload == payload

    @pytest.mark.asyncio
    async def test_request_stream_in_raises_on_connection_refused(
        self, busy_port: int
    ) -> None:
        # noinspection PyTypeChecker
        with pytest.raises((OSError, asyncio.TimeoutError)):
            client = Client(f"ws://localhost:{busy_port}")
            await client.request_stream_in("/", data=[{}])

    @pytest.mark.asyncio
    async def test_request_stream_in_raises_on_invalid_uri(self) -> None:
        with pytest.raises(WebSocketException):
            client = Client("foo")
            await client.request_stream_in("/", data=[{}])

    # noinspection PyShadowingNames
    @pytest.mark.asyncio
    async def test_request_stream_in_raises_on_error_message(
        self, error_message_server: TestServer
    ) -> None:
        with pytest.raises(AppError):
            client = Client(error_message_server.url)
            await client.request_stream_in("/", data=[{}])

    # noinspection PyShadowingNames
    @pytest.mark.asyncio
    async def test_request_stream_in_raises_on_invalid_message(
        self, invalid_message_server: TestServer
    ) -> None:
        with pytest.raises(ProtocolError):
            client = Client(invalid_message_server.url)
            await client.request_stream_in("/", data=[{}])


class TestRequestStreamOut:
    @pytest.fixture
    def payloads(self) -> List[JSON]:
        return [{"foo": "bar"}, {"bar": "foo"}]

    @pytest_asyncio.fixture
    async def working_server(self, payloads: List[JSON]) -> TestServer:
        async def handler(websocket: WebSocketServerProtocol) -> None:
            async with chat(websocket) as chat_id:
                await websocket.recv()
                for payload in payloads:
                    reply = DataMessage(chat_id=chat_id, payload=payload)
                    await websocket.send(reply.json())
                await websocket.send(StreamEndMessage(chat_id=chat_id).json())

        async with TestServer(handler) as server:
            yield server

    @pytest.mark.asyncio
    async def test_request_stream_out_returns_correct_payloads(
        self, working_server: TestServer, payloads: List[JSON]
    ) -> None:
        reply_payloads = [
            payload
            async for payload in Client(working_server.url).request_stream_out(
                "/", data={}
            )
        ]
        assert reply_payloads == payloads

    @pytest.mark.asyncio
    async def test_request_stream_out_raises_on_connection_refused(
        self, busy_port: int
    ) -> None:
        # noinspection PyTypeChecker
        with pytest.raises((OSError, asyncio.TimeoutError)):
            async for _ in Client(
                f"ws://localhost:{busy_port}"
            ).request_stream_out("/", data={}):
                pass

    @pytest.mark.asyncio
    async def test_request_stream_out_raises_on_invalid_uri(self) -> None:
        with pytest.raises(WebSocketException):
            async for _ in Client("foo").request_stream_out("/", data={}):
                pass

    # noinspection PyShadowingNames
    @pytest.mark.asyncio
    async def test_request_stream_out_raises_on_error_message(
        self, error_message_server: TestServer
    ) -> None:
        with pytest.raises(AppError):
            async for _ in Client(error_message_server.url).request_stream_out(
                "/", data={}
            ):
                pass

    # noinspection PyShadowingNames
    @pytest.mark.asyncio
    async def test_request_stream_out_raises_on_invalid_message(
        self, invalid_message_server: TestServer
    ) -> None:
        with pytest.raises(ProtocolError):
            client = Client(invalid_message_server.url)
            async for _ in client.request_stream_out("/", data={}):
                pass


class TestRequestStreamInOut:
    @pytest.fixture
    def payloads(self) -> List[JSON]:
        return [{"foo": "bar"}, {"bar": "foo"}]

    @pytest_asyncio.fixture
    async def working_server(self, payloads: List[JSON]) -> TestServer:
        async def handler(websocket: WebSocketServerProtocol) -> None:
            async with chat(websocket) as chat_id:
                async for data in websocket:
                    try:
                        DataMessage.parse_raw(data)
                    except ValidationError:
                        break
                for payload in payloads:
                    reply = DataMessage(chat_id=chat_id, payload=payload)
                    await websocket.send(reply.json())
                await websocket.send(StreamEndMessage(chat_id=chat_id).json())

        async with TestServer(handler) as server:
            yield server

    @pytest.mark.asyncio
    async def test_request_stream_out_returns_correct_payloads(
        self, working_server: TestServer, payloads: List[JSON]
    ) -> None:
        client = Client(working_server.url)
        reply_payloads = [
            payload
            async for payload in client.request_stream_in_out("/", data=[{}])
        ]
        assert reply_payloads == payloads

    @pytest.mark.asyncio
    async def test_request_stream_in_out_raises_on_connection_refused(
        self, busy_port: int
    ) -> None:
        # noinspection PyTypeChecker
        with pytest.raises((OSError, asyncio.TimeoutError)):
            client = Client(f"ws://localhost:{busy_port}")
            async for _ in client.request_stream_in_out("/", data=[{}]):
                pass

    @pytest.mark.asyncio
    async def test_request_stream_in_out_raises_on_invalid_uri(self) -> None:
        with pytest.raises(WebSocketException):
            client = Client("foo")
            async for _ in client.request_stream_in_out("/", data=[{}]):
                pass

    # noinspection PyShadowingNames
    @pytest.mark.asyncio
    async def test_request_stream_in_out_raises_on_error_message(
        self, error_message_server: TestServer
    ) -> None:
        with pytest.raises(AppError):
            client = Client(error_message_server.url)
            async for _ in client.request_stream_in_out("/", data=[{}]):
                pass

    # noinspection PyShadowingNames
    @pytest.mark.asyncio
    async def test_request_stream_in_out_raises_on_invalid_message(
        self, invalid_message_server: TestServer
    ) -> None:
        with pytest.raises(ProtocolError):
            client = Client(invalid_message_server.url)
            async for _ in client.request_stream_in_out("/", data=[{}]):
                pass
