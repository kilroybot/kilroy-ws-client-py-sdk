from typing import AsyncIterable, Iterable, Union

import websockets

from kilroy_ws_client_py_sdk.errors import ProtocolError
from kilroy_ws_client_py_sdk.messages import StreamEndMessage
from kilroy_ws_client_py_sdk.protocol import (
    create_request_message,
    parse_data_message,
    parse_reply_message,
    parse_stream_end_message,
    serialize_request_message,
)
from kilroy_ws_client_py_sdk.types import JSON
from kilroy_ws_client_py_sdk.utils import asyncify


async def get(*args, **kwargs) -> JSON:
    async with websockets.connect(*args, **kwargs) as websocket:
        incoming_data = await websocket.recv()

    return parse_data_message(incoming_data).payload


async def get_stream(*args, **kwargs) -> AsyncIterable[JSON]:
    async with websockets.connect(*args, **kwargs) as websocket:
        async for incoming_data in websocket:
            try:
                yield parse_data_message(incoming_data).payload
            except ProtocolError:
                parse_stream_end_message(incoming_data)
                return


async def subscribe(*args, **kwargs) -> AsyncIterable[JSON]:
    async with websockets.connect(*args, **kwargs) as websocket:
        async for incoming_data in websocket:
            yield parse_data_message(incoming_data).payload


async def request(*args, payload: JSON, **kwargs) -> JSON:
    request_message = create_request_message(payload)
    outgoing_data = serialize_request_message(request_message)

    async with websockets.connect(*args, **kwargs) as websocket:
        await websocket.send(outgoing_data)
        incoming_data = await websocket.recv()

    return parse_reply_message(incoming_data, request_message.id).payload


async def request_stream_out(
    *args, payload: JSON, **kwargs
) -> AsyncIterable[JSON]:
    request_message = create_request_message(payload)
    outgoing_data = serialize_request_message(request_message)

    async with websockets.connect(*args, **kwargs) as websocket:
        await websocket.send(outgoing_data)
        async for incoming_data in websocket:
            try:
                yield parse_reply_message(
                    incoming_data, request_message.id
                ).payload
            except ProtocolError:
                parse_stream_end_message(incoming_data)
                return


async def request_stream_in(
    *args, payloads: Union[Iterable[JSON], AsyncIterable[JSON]], **kwargs
) -> JSON:
    async with websockets.connect(*args, **kwargs) as websocket:
        async for payload in asyncify(payloads):
            request_message = create_request_message(payload)
            outgoing_data = serialize_request_message(request_message)
            await websocket.send(outgoing_data)

        await websocket.send(StreamEndMessage().json())

        incoming_data = await websocket.recv()
        return parse_reply_message(incoming_data, request_message.id).payload


async def request_stream_in_out(
    *args, payloads: Union[Iterable[JSON], AsyncIterable[JSON]], **kwargs
) -> AsyncIterable[JSON]:
    async with websockets.connect(*args, **kwargs) as websocket:
        async for payload in asyncify(payloads):
            request_message = create_request_message(payload)
            outgoing_data = serialize_request_message(request_message)
            await websocket.send(outgoing_data)

        await websocket.send(StreamEndMessage().json())

        async for incoming_data in websocket:
            try:
                yield parse_reply_message(
                    incoming_data, request_message.id
                ).payload
            except ProtocolError:
                parse_stream_end_message(incoming_data)
                return
