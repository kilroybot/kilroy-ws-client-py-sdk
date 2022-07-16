from json import JSONDecodeError
from uuid import UUID

from pydantic import ValidationError

from kilroy_ws_client_py_sdk.errors import (
    AppError,
    ProtocolError,
)
from kilroy_ws_client_py_sdk.messages import (
    AppErrorMessage,
    DataMessage,
    JSON,
    ProtocolErrorMessage,
    ReplyMessage,
    RequestMessage,
    StreamEndMessage,
)


def create_request_message(payload: JSON) -> RequestMessage:
    try:
        return RequestMessage(payload=payload)
    except ValidationError as e:
        raise ProtocolError("Can't create request message.") from e


def serialize_request_message(request: RequestMessage) -> str:
    try:
        return request.json()
    except JSONDecodeError as e:
        raise ProtocolError("Can't serialize request message.") from e


def get_error(data: str) -> Exception:
    try:
        error_message = AppErrorMessage.parse_raw(data)
        return AppError(error_message.code, error_message.reason)
    except ValidationError:
        try:
            error_message = ProtocolErrorMessage.parse_raw(data)
            return ProtocolError(error_message.reason)
        except ValidationError:
            return ProtocolError("Invalid message received.")


def parse_data_message(data: str) -> DataMessage:
    try:
        return DataMessage.parse_raw(data)
    except ValidationError as e:
        raise get_error(data) from e


def parse_reply_message(data: str, request_id: UUID) -> ReplyMessage:
    try:
        reply_message = ReplyMessage.parse_raw(data)
    except ValidationError as e:
        raise get_error(data) from e

    if reply_message.request != request_id:
        raise ProtocolError("Got a reply for different request.")
    return reply_message


def parse_stream_end_message(data: str) -> StreamEndMessage:
    try:
        return StreamEndMessage.parse_raw(data)
    except ValidationError as e:
        raise get_error(data) from e
