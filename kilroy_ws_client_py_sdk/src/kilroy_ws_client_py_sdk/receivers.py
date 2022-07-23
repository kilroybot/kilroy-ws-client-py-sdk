import asyncio
from abc import ABC, abstractmethod
from typing import AsyncIterable, Awaitable, Coroutine, Generic, TypeVar
from uuid import UUID

import websockets

from kilroy_ws_client_py_sdk.protocol import (
    receive_data_message,
    receive_data_message_stream,
)
from kilroy_ws_client_py_sdk.types import JSON

ReturnType = TypeVar(
    "ReturnType", Awaitable[None], Awaitable[JSON], AsyncIterable[JSON]
)


class Receiver(ABC, Generic[ReturnType]):
    @abstractmethod
    async def receive(
        self,
        websocket: websockets.WebSocketServerProtocol,
        chat_id: UUID,
    ) -> ReturnType:
        pass

    @abstractmethod
    def chain(
        self,
        sending: Coroutine,
        websocket: websockets.WebSocketServerProtocol,
        chat_id: UUID,
    ) -> ReturnType:
        pass


class NullReceiver(Receiver[Awaitable[None]]):
    async def receive(
        self,
        websocket: websockets.WebSocketServerProtocol,
        chat_id: UUID,
    ) -> None:
        return None

    async def chain(
        self,
        sending: Coroutine,
        websocket: websockets.WebSocketServerProtocol,
        chat_id: UUID,
    ) -> None:
        await sending


class SingleReceiver(Receiver[Awaitable[JSON]]):
    async def receive(
        self,
        websocket: websockets.WebSocketServerProtocol,
        chat_id: UUID,
    ) -> JSON:
        message = await receive_data_message(websocket, chat_id)
        return message.payload

    async def chain(
        self,
        sending: Coroutine,
        websocket: websockets.WebSocketServerProtocol,
        chat_id: UUID,
    ) -> JSON:
        sending_task = asyncio.create_task(sending)
        data = await self.receive(websocket, chat_id)

        sending_task.cancel()
        try:
            await sending_task
        except asyncio.CancelledError:
            pass

        return data


class StreamReceiver(Receiver[AsyncIterable[JSON]]):
    async def receive(
        self,
        websocket: websockets.WebSocketServerProtocol,
        chat_id: UUID,
    ) -> AsyncIterable[JSON]:
        async for message in receive_data_message_stream(websocket, chat_id):
            yield message.payload

    async def chain(
        self,
        sending: Coroutine,
        websocket: websockets.WebSocketServerProtocol,
        chat_id: UUID,
    ) -> AsyncIterable[JSON]:
        sending_task = asyncio.create_task(sending)

        async for data in self.receive(websocket, chat_id):
            yield data

        sending_task.cancel()
        try:
            await sending_task
        except asyncio.CancelledError:
            pass
