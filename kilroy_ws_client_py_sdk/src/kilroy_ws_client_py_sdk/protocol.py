from contextlib import asynccontextmanager
from functools import partial
from json import JSONDecodeError
from typing import AsyncIterable, Iterable, Type, TypeVar, Union
from uuid import UUID, uuid4

import websockets
from pydantic import ValidationError

from kilroy_ws_client_py_sdk.errors import (
    AppError,
    CONVERSATION_ERROR,
    INVALID_MESSAGE_ERROR,
    ProtocolError,
)
from kilroy_ws_client_py_sdk.messages import (
    AppErrorMessage,
    BaseMessage,
    DataMessage,
    JSON,
    ProtocolErrorMessage,
    StartMessage,
    StopMessage,
    StreamEndMessage,
)
from kilroy_ws_client_py_sdk.utils import asyncify

MessageType = TypeVar("MessageType", bound=BaseMessage)


def parse_message(data: str, message_type: Type[MessageType]) -> MessageType:
    try:
        return message_type.parse_raw(data)
    except ValidationError as e:
        try:
            error = ProtocolErrorMessage.parse_raw(data)
            raise ProtocolError(str(error))
        except ValidationError:
            try:
                error = AppErrorMessage.parse_raw(data)
                raise AppError(code=error.code, reason=error.reason)
            except ValidationError:
                raise INVALID_MESSAGE_ERROR from e


parse_stop_message = partial(parse_message, message_type=StopMessage)
parse_data_message = partial(parse_message, message_type=DataMessage)
parse_stream_end_message = partial(
    parse_message, message_type=StreamEndMessage
)


def create_data_message(chat_id: UUID, payload: JSON) -> DataMessage:
    try:
        return DataMessage(chat_id=chat_id, payload=payload)
    except ValidationError as e:
        raise ProtocolError("Can't create data message.") from e


def serialize_data_message(message: DataMessage) -> str:
    try:
        return message.json()
    except JSONDecodeError as e:
        raise ProtocolError("Can't serialize data message.") from e


async def send_start_message(
    websocket: websockets.WebSocketServerProtocol, chat_id: UUID
) -> None:
    await websocket.send(StartMessage(chat_id=chat_id).json())


async def receive_stop_message(
    websocket: websockets.WebSocketServerProtocol, chat_id: UUID
) -> StartMessage:
    message = parse_stop_message(await websocket.recv())
    check_chat_id(message, chat_id)
    return message


def check_chat_id(message: BaseMessage, expected_id: UUID) -> None:
    if message.chat_id != expected_id:
        raise CONVERSATION_ERROR


async def receive_data_message(
    websocket: websockets.WebSocketServerProtocol, chat_id: UUID
) -> DataMessage:
    message = parse_data_message(await websocket.recv())
    check_chat_id(message, chat_id)
    return message


async def receive_data_message_stream(
    websocket: websockets.WebSocketServerProtocol, chat_id: UUID
) -> AsyncIterable[DataMessage]:
    async for data in websocket:
        try:
            message = parse_data_message(data)
        except ProtocolError as e:
            try:
                message = parse_stream_end_message(data)
            except ProtocolError:
                raise e

            check_chat_id(message, chat_id)
            return

        check_chat_id(message, chat_id)
        yield message


async def send_data_message(
    websocket: websockets.WebSocketServerProtocol,
    chat_id: UUID,
    payload: JSON,
) -> None:
    message = create_data_message(chat_id, payload)
    data = serialize_data_message(message)
    await websocket.send(data)


async def send_data_message_stream(
    websocket: websockets.WebSocketServerProtocol,
    chat_id: UUID,
    payloads: Union[Iterable[JSON], AsyncIterable[JSON]],
) -> None:
    async for payload in asyncify(payloads):
        message = create_data_message(chat_id, payload)
        data = serialize_data_message(message)
        await websocket.send(data)

    stream_end_message = StreamEndMessage(chat_id=chat_id)
    await websocket.send(stream_end_message.json())


@asynccontextmanager
async def chat(websocket: websockets.WebSocketServerProtocol) -> UUID:
    chat_id = uuid4()

    await send_start_message(websocket, chat_id)

    yield chat_id

    await receive_stop_message(websocket, chat_id)
