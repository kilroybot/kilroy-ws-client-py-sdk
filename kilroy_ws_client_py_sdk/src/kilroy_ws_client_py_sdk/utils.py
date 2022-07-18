from typing import AsyncIterable, Iterable, TypeVar, Union

from kilroy_ws_client_py_sdk.types import JSON

T = TypeVar("T")


async def asyncify(
    iterable: Union[Iterable[JSON], AsyncIterable[JSON]]
) -> AsyncIterable[T]:
    try:
        iter(iterable)
    except TypeError:
        async for item in iterable:
            yield item
    else:
        for item in iterable:
            yield item
