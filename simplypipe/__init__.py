from collections.abc import Iterable
from typing import Any

from simplypipe.core.pipe import Pipe
from simplypipe.stats.model import RunStats


def pipe(iterable: Iterable[Any]) -> Pipe:
    """Create a synchronous lazy pipeline from an iterable."""
    return Pipe(iterable)


__all__ = ["pipe", "Pipe", "RunStats"]
