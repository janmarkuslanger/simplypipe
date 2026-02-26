from __future__ import annotations

import time
from collections import OrderedDict
from collections.abc import Callable, Iterable
from typing import Any

from simplypipe.stats.model import RunStats

_Op = Callable[[Any, RunStats], Any]


class Pipe:
    """Synchronous lazy pipeline. No work is done until run() is called."""

    def __init__(self, source: Iterable, _ops: list[_Op] | None = None) -> None:
        self._source = source
        self._ops: list[_Op] = _ops if _ops is not None else []

    def _add_op(self, op: _Op) -> "Pipe":
        return Pipe(self._source, self._ops + [op])

    def map(self, fn: Callable) -> "Pipe":
        def op(iterable: Iterable, _stats: RunStats):
            for item in iterable:
                yield fn(item)
        return self._add_op(op)

    def flat_map(self, fn: Callable) -> "Pipe":
        def op(iterable: Iterable, _stats: RunStats):
            for item in iterable:
                yield from fn(item)
        return self._add_op(op)

    def filter(self, fn: Callable) -> "Pipe":
        def op(iterable: Iterable, stats: RunStats):
            for item in iterable:
                if fn(item):
                    yield item
                else:
                    stats.dropped += 1
        return self._add_op(op)

    def tap(self, fn: Callable) -> "Pipe":
        def op(iterable: Iterable, _stats: RunStats):
            for item in iterable:
                fn(item)
                yield item
        return self._add_op(op)

    def batch(self, size: int) -> "Pipe":
        def op(iterable: Iterable, stats: RunStats):
            buffer: list = []
            for item in iterable:
                buffer.append(item)
                if len(buffer) == size:
                    stats.batches += 1
                    yield buffer
                    buffer = []
            if buffer:
                stats.batches += 1
                yield buffer
        return self._add_op(op)

    def rate_limit(self, rate: float, per: float = 1.0) -> "Pipe":
        """Emit at most `rate` items per `per` seconds."""
        min_interval = per / rate

        def op(iterable: Iterable, _stats: RunStats):
            last_time: float | None = None
            for item in iterable:
                if last_time is not None:
                    elapsed = time.perf_counter() - last_time
                    if elapsed < min_interval:
                        time.sleep(min_interval - elapsed)
                last_time = time.perf_counter()
                yield item
        return self._add_op(op)

    def dedupe(self, key: Callable | None = None, max_size: int | None = None) -> "Pipe":
        """Drop duplicate items. key extracts the comparison value.
        max_size bounds memory by evicting the oldest seen key (LRU)."""

        def op(iterable: Iterable, stats: RunStats):
            seen: OrderedDict = OrderedDict()
            for item in iterable:
                key_value = key(item) if key is not None else item
                if key_value not in seen:
                    seen[key_value] = None
                    if max_size is not None and len(seen) > max_size:
                        seen.popitem(last=False)
                    yield item
                else:
                    stats.dropped += 1
        return self._add_op(op)

    def retry_map(
        self,
        fn: Callable,
        retries: int = 3,
        backoff: float = 1.0,
        exceptions: tuple = (Exception,),
    ) -> "Pipe":
        """Apply fn with up to `retries` retries on failure.
        Backoff is exponential: backoff * 2 ** attempt seconds."""

        def op(iterable: Iterable, stats: RunStats):
            for item in iterable:
                last_exc: BaseException | None = None
                for attempt in range(retries + 1):
                    try:
                        yield fn(item)
                        break
                    except exceptions as e:
                        last_exc = e
                        stats.errors += 1
                        if attempt < retries and backoff > 0:
                            time.sleep(backoff * (2**attempt))
                else:
                    raise last_exc  # type: ignore[misc]
        return self._add_op(op)

    def take(self, n: int) -> "Pipe":
        """Emit at most `n` items then stop."""

        def op(iterable: Iterable, _stats: RunStats):
            count = 0
            for item in iterable:
                if count >= n:
                    break
                yield item
                count += 1

        return self._add_op(op)

    def catch(
        self,
        fn: Callable,
        on_error: Callable,
        exceptions: tuple = (Exception,),
    ) -> "Pipe":
        """Apply fn to each item. On exception, call on_error(item, exc) and drop the item."""

        def op(iterable: Iterable, stats: RunStats):
            for item in iterable:
                try:
                    yield fn(item)
                except exceptions as e:
                    on_error(item, e)
                    stats.errors += 1

        return self._add_op(op)

    def run(self, sink: Callable | None = None) -> RunStats:
        """Execute the pipeline and return RunStats."""
        stats = RunStats()
        start = time.perf_counter()

        def _source():
            for item in self._source:
                stats.processed += 1
                yield item

        pipeline: Any = _source()
        for op in self._ops:
            pipeline = op(pipeline, stats)

        for item in pipeline:
            stats.emitted += 1
            if sink is not None:
                sink(item)

        stats.duration = time.perf_counter() - start
        return stats
