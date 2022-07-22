from abc import ABC
from typing import Literal, Optional
from uuid import UUID, uuid4

from humps import camelize
from pydantic import BaseModel, Field

from kilroy_ws_client_py_sdk.types import JSON


class BaseMessage(BaseModel, ABC):
    id: UUID = Field(default_factory=uuid4)
    chat_id: UUID

    def json(self, *args, by_alias: bool = True, **kwargs) -> str:
        return super().json(*args, by_alias=by_alias, **kwargs)

    class Config:
        allow_population_by_field_name = True
        alias_generator = camelize


class StartMessage(BaseMessage):
    type: Literal["start"] = "start"


class StopMessage(BaseMessage):
    type: Literal["stop"] = "stop"


class DataMessage(BaseMessage):
    type: Literal["data"] = "data"
    payload: JSON


class AppErrorMessage(BaseMessage):
    type: Literal["app-error"] = "app-error"
    code: int
    reason: str


class ProtocolErrorMessage(BaseMessage):
    type: Literal["protocol-error"] = "protocol-error"
    chat_id: Optional[UUID]
    reason: str


class StreamEndMessage(BaseMessage):
    type: Literal["stream-end"] = "stream-end"
