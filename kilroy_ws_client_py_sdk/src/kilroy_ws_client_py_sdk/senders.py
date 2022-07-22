from abc import ABC, abstractmethod
from typing import AsyncIterable, Generic, Iterable, TypeVar, Union
from uuid import UUID

import websockets

from kilroy_ws_client_py_sdk.protocol import (
    send_data_message,
    send_data_message_stream,
)
from kilroy_ws_client_py_sdk.types import JSON

DataType = TypeVar(
    "DataType", None, JSON, Union[Iterable[JSON], AsyncIterable[JSON]]
)


class Sender(ABC, Generic[DataType]):
    @abstractmethod
    async def send(
        self,
        websocket: websockets.WebSocketServerProtocol,
        chat_id: UUID,
        data: DataType,
    ) -> None:
        pass


class NullSender(Sender[None]):
    async def send(
        self,
        websocket: websockets.WebSocketServerProtocol,
        chat_id: UUID,
        data: None,
    ) -> None:
        return None


class SingleSender(Sender[JSON]):
    async def send(
        self,
        websocket: websockets.WebSocketServerProtocol,
        chat_id: UUID,
        data: JSON,
    ) -> None:
        await send_data_message(websocket, chat_id, data)


class StreamSender(Sender[Union[Iterable[JSON], AsyncIterable[JSON]]]):
    async def send(
        self,
        websocket: websockets.WebSocketServerProtocol,
        chat_id: UUID,
        data: Union[Iterable[JSON], AsyncIterable[JSON]],
    ) -> None:
        await send_data_message_stream(websocket, chat_id, data)
