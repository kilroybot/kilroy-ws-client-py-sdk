from typing import Literal
from uuid import UUID, uuid4

from pydantic import BaseModel, Field

from kilroy_ws_client_py_sdk.types import JSON


class DataMessage(BaseModel):
    type: Literal["data"] = "data"
    payload: JSON


class AppErrorMessage(BaseModel):
    type: Literal["app-error"] = "app-error"
    code: int
    reason: str


class ProtocolErrorMessage(BaseModel):
    type: Literal["protocol-error"] = "protocol-error"
    reason: str


class RequestMessage(BaseModel):
    type: Literal["request"] = "request"
    id: UUID = Field(default_factory=uuid4)
    payload: JSON


class ReplyMessage(BaseModel):
    type: Literal["reply"] = "reply"
    request: UUID
    payload: JSON


class StreamEndMessage(BaseModel):
    type: Literal["stream-end"] = "stream-end"
