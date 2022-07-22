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


def lead(path: str) -> str:
    return path if path.startswith("/") else "/" + path


def untrail(url: str) -> str:
    return url if not url.endswith("/") else url[:-1]
