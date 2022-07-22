from contextlib import asynccontextmanager
from types import TracebackType
from typing import AsyncIterable, Iterable, Optional, Type, Union

import websockets

from kilroy_ws_client_py_sdk.protocol import chat
from kilroy_ws_client_py_sdk.receivers import (
    Receiver,
    ReturnType,
    SingleReceiver,
    StreamReceiver,
)
from kilroy_ws_client_py_sdk.senders import (
    DataType,
    NullSender,
    Sender,
    SingleSender,
    StreamSender,
)
from kilroy_ws_client_py_sdk.types import JSON
from kilroy_ws_client_py_sdk.utils import lead, untrail


class Client:
    def __init__(self, url: str) -> None:
        self._url = untrail(url)

    @asynccontextmanager
    async def call(
        self,
        path: str,
        sender: Sender[DataType],
        receiver: Receiver[ReturnType],
        data: DataType,
        *args,
        **kwargs,
    ) -> ReturnType:
        url = self._url + lead(path)
        async with websockets.connect(url, *args, **kwargs) as websocket:
            async with chat(websocket) as chat_id:
                yield receiver.chain(
                    sender.send(websocket, chat_id, data), websocket, chat_id
                )

    async def get(self, path: str, *args, **kwargs) -> JSON:
        async with self.call(
            path, NullSender(), SingleReceiver(), None, *args, **kwargs
        ) as result:
            return await result

    async def subscribe(
        self, path: str, *args, **kwargs
    ) -> AsyncIterable[JSON]:
        async with self.call(
            path, NullSender(), StreamReceiver(), None, *args, **kwargs
        ) as results:
            async for result in results:
                yield result

    async def request(self, path: str, data: JSON, *args, **kwargs) -> JSON:
        async with self.call(
            path, SingleSender(), SingleReceiver(), data, *args, **kwargs
        ) as result:
            return await result

    async def request_stream_in(
        self,
        path: str,
        data: Union[Iterable[JSON], AsyncIterable[JSON]],
        *args,
        **kwargs,
    ) -> JSON:
        async with self.call(
            path, StreamSender(), SingleReceiver(), data, *args, **kwargs
        ) as result:
            return await result

    async def request_stream_out(
        self, path: str, data: JSON, *args, **kwargs
    ) -> AsyncIterable[JSON]:
        async with self.call(
            path, SingleSender(), StreamReceiver(), data, *args, **kwargs
        ) as results:
            async for result in results:
                yield result

    async def request_stream_in_out(
        self,
        path: str,
        data: Union[Iterable[JSON], AsyncIterable[JSON]],
        *args,
        **kwargs,
    ) -> AsyncIterable[JSON]:
        async with self.call(
            path, StreamSender(), StreamReceiver(), data, *args, **kwargs
        ) as results:
            async for result in results:
                yield result

    async def __aenter__(self) -> "Client":
        return self

    async def __aexit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc: Optional[BaseException],
        traceback: Optional[TracebackType],
    ) -> Optional[bool]:
        return None
