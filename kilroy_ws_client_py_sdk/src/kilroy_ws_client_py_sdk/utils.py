from typing import AsyncIterable, Iterable, TypeVar, Union

T = TypeVar("T")


async def asyncify(
    iterable: Union[Iterable[T], AsyncIterable[T]]
) -> AsyncIterable[T]:
    try:
        iter(iterable)
    except TypeError:
        async for item in iterable:
            yield item
    else:
        for item in iterable:
            yield item
