# simplypipe

Chainable data pipelines for Python iterables. Zero dependencies.

```python
stats = (
    pipe(records)
    .filter(is_valid)
    .map(normalize)
    .catch(enrich_from_api, on_error=dead_letters.append)
    .batch(100)
    .run(sink=write_to_db)
)
```

`.run()` returns a `RunStats` object with processed/emitted counts, error counts, and wall-clock duration.

```
pip install simplypipe
```

## Quick start

```python
from simplypipe import pipe

stats = (
    pipe(range(1000))
    .filter(lambda x: x % 2 == 0)
    .map(lambda x: x * 3)
    .batch(100)
    .run(sink=print)
)

print(stats.processed, stats.emitted, stats.duration)
```

## Operators

### `map(fn)`

Applies `fn` to each item and passes the result downstream. The original item is replaced by the return value of `fn`.

```python
pipe(["hello", "world"])
    .map(str.upper)
    .run(sink=print)
# HELLO
# WORLD
```

---

### `flat_map(fn)`

Applies `fn` to each item and flattens the result. Use this when `fn` returns an iterable and you want each element of that iterable to continue as a separate item.

```python
pipe(["hello world", "foo bar"])
    .flat_map(str.split)
    .run(sink=print)
# hello
# world
# foo
# bar
```

---

### `filter(fn)`

Keeps only items for which `fn` returns a truthy value. Dropped items are counted in `RunStats.dropped`.

```python
pipe(range(10))
    .filter(lambda x: x % 2 == 0)
    .run(sink=print)
# 0
# 2
# 4
# 6
# 8
```

---

### `tap(fn)`

Calls `fn` for its side-effect on each item, then passes the item through unchanged. Useful for logging or debugging mid-pipeline.

```python
pipe(range(3))
    .tap(lambda x: print(f"processing {x}"))
    .map(lambda x: x * 10)
    .run(sink=print)
# processing 0
# 0
# processing 1
# 10
# processing 2
# 20
```

---

### `batch(size)`

Collects items into lists of up to `size` elements. The last batch may be smaller if the source is exhausted. Each batch counts in `RunStats.batches`.

```python
pipe(range(7))
    .batch(3)
    .run(sink=print)
# [0, 1, 2]
# [3, 4, 5]
# [6]
```

---

### `rate_limit(rate, per=1.0)`

Throttles throughput to at most `rate` items per `per` seconds by sleeping between items as needed.

```python
# Process at most 5 items per second
pipe(range(20))
    .rate_limit(5, per=1.0)
    .run(sink=print)
```

---

### `dedupe(key=None, max_size=None)`

Drops duplicate items, keeping only the first occurrence. Use `key` to extract the comparison value from each item. Use `max_size` to bound memory — when the seen-set exceeds `max_size`, the oldest entry is evicted (LRU). Dropped duplicates are counted in `RunStats.dropped`.

```python
pipe([1, 2, 2, 3, 1, 4])
    .dedupe()
    .run(sink=print)
# 1
# 2
# 3
# 4

# With a key function
pipe([{"id": 1, "v": "a"}, {"id": 1, "v": "b"}, {"id": 2, "v": "c"}])
    .dedupe(key=lambda x: x["id"])
    .run(sink=print)
# {"id": 1, "v": "a"}
# {"id": 2, "v": "c"}
```

---

### `retry_map(fn, retries=3, backoff=1.0, exceptions=(Exception,))`

Like `map`, but retries `fn` up to `retries` times if it raises one of the specified `exceptions`. Backoff between attempts is exponential: `backoff * 2 ** attempt` seconds. If all retries are exhausted, the last exception is re-raised. Each failed attempt increments `RunStats.errors`.

```python
import random

def flaky(x):
    if random.random() < 0.5:
        raise ValueError("transient error")
    return x * 2

pipe(range(5))
    .retry_map(flaky, retries=3, backoff=0.1)
    .run(sink=print)
```

---

### `take(n)`

Emits at most `n` items, then stops. Useful for previewing a pipeline, limiting output, or processing only a slice of a large source.

```python
pipe(range(1_000_000))
    .map(expensive_transform)
    .take(10)
    .run(sink=print)
```

---

### `catch(fn, on_error, exceptions=(Exception,))`

Like `map`, but handles errors per item instead of crashing the pipeline. If `fn` raises one of the specified `exceptions`, `on_error(item, exc)` is called and the item is dropped. Processing continues with the next item. Each caught error increments `RunStats.errors`.

```python
dead_letters = []

pipe(records)
    .catch(
        enrich_from_api,
        on_error=lambda item, exc: dead_letters.append(item),
        exceptions=(IOError, TimeoutError),
    )
    .run(sink=write_to_db)
```

---

## RunStats

`.run()` returns a `RunStats` dataclass:

```python
@dataclass
class RunStats:
    processed: int    # items from source
    emitted: int      # items delivered to sink
    dropped: int      # items removed by filter or dedupe
    batches: int      # batches produced by batch()
    errors: int       # exceptions caught by retry_map or catch
    duration: float   # wall-clock time in seconds
```

## Development

```bash
git clone https://github.com/janmarkuslanger/simplypipe.git
cd simplypipe
pip install -e ".[dev]"
```

Run tests:

```bash
pytest
```

Lint and format:

```bash
ruff check .
ruff format .
```

Type-check:

```bash
mypy simplypipe
```

## License

MIT
