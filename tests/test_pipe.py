import pytest

from simplypipe import pipe


def test_map_transforms_items():
    result = []
    pipe([1, 2, 3]).map(lambda x: x * 2).run(sink=result.append)
    assert result == [2, 4, 6]


def test_flat_map_expands_items():
    result = []
    pipe([1, 2, 3]).flat_map(lambda x: [x, x]).run(sink=result.append)
    assert result == [1, 1, 2, 2, 3, 3]


def test_flat_map_empty_expansion():
    result = []
    pipe([1, 2, 3]).flat_map(lambda x: [] if x == 2 else [x]).run(sink=result.append)
    assert result == [1, 3]


def test_filter_keeps_matching_items():
    result = []
    pipe([1, 2, 3, 4]).filter(lambda x: x % 2 == 0).run(sink=result.append)
    assert result == [2, 4]


def test_filter_drops_all():
    result = []
    pipe([1, 3, 5]).filter(lambda x: x % 2 == 0).run(sink=result.append)
    assert result == []


def test_tap_calls_fn_and_passes_through():
    side = []
    result = []
    pipe([1, 2, 3]).tap(side.append).run(sink=result.append)
    assert side == [1, 2, 3]
    assert result == [1, 2, 3]


def test_batch_groups_items():
    result = []
    pipe(range(7)).batch(3).run(sink=result.append)
    assert result == [[0, 1, 2], [3, 4, 5], [6]]


def test_batch_exact_multiple():
    result = []
    pipe(range(6)).batch(3).run(sink=result.append)
    assert result == [[0, 1, 2], [3, 4, 5]]


def test_batch_size_one():
    result = []
    pipe([10, 20]).batch(1).run(sink=result.append)
    assert result == [[10], [20]]


def test_batch_larger_than_source():
    result = []
    pipe([1, 2]).batch(10).run(sink=result.append)
    assert result == [[1, 2]]


def test_dedupe_removes_duplicates():
    result = []
    pipe([1, 2, 1, 3, 2]).dedupe().run(sink=result.append)
    assert result == [1, 2, 3]


def test_dedupe_key_based():
    result = []
    pipe(["a", "A", "b"]).dedupe(key=str.lower).run(sink=result.append)
    assert result == ["a", "b"]


def test_dedupe_preserves_first_seen():
    result = []
    pipe([3, 1, 2, 1, 3]).dedupe().run(sink=result.append)
    assert result == [3, 1, 2]


def test_dedupe_bounded_evicts_oldest():
    # max_size=2: [1, 2] fill the set, 3 evicts 1, so 1 reappears later
    result = []
    pipe([1, 2, 3, 1]).dedupe(max_size=2).run(sink=result.append)
    assert result == [1, 2, 3, 1]


def test_rate_limit_passes_all_items():
    result = []
    pipe([1, 2, 3]).rate_limit(1_000_000).run(sink=result.append)
    assert result == [1, 2, 3]


def test_retry_map_success_on_first_try():
    result = []
    pipe([1, 2, 3]).retry_map(lambda x: x * 10, retries=2, backoff=0).run(
        sink=result.append
    )
    assert result == [10, 20, 30]


def test_retry_map_succeeds_after_retries():
    calls = []

    def flaky(x):
        calls.append(x)
        if len(calls) < 3:
            raise ValueError("not yet")
        return x * 10

    result = []
    pipe([1]).retry_map(flaky, retries=3, backoff=0).run(sink=result.append)
    assert result == [10]
    assert len(calls) == 3


def test_retry_map_raises_after_exhausted():
    def always_fail(x):
        raise ValueError("always")

    with pytest.raises(ValueError, match="always"):
        pipe([1]).retry_map(always_fail, retries=2, backoff=0).run()


def test_retry_map_counts_errors():
    calls = [0]

    def flaky(x):
        calls[0] += 1
        if calls[0] < 3:
            raise RuntimeError("retry me")
        return x

    stats = pipe([1]).retry_map(flaky, retries=3, backoff=0).run()
    assert stats.errors == 2


def test_retry_map_only_catches_specified_exceptions():
    def raises_type_error(x):
        raise TypeError("wrong type")

    with pytest.raises(TypeError):
        pipe([1]).retry_map(
            raises_type_error, retries=2, backoff=0, exceptions=(ValueError,)
        ).run()


def test_take_limits_items():
    result = []
    pipe(range(100)).take(3).run(sink=result.append)
    assert result == [0, 1, 2]


def test_take_more_than_source():
    result = []
    pipe([1, 2]).take(10).run(sink=result.append)
    assert result == [1, 2]


def test_take_zero():
    result = []
    pipe([1, 2, 3]).take(0).run(sink=result.append)
    assert result == []


def test_catch_passes_successful_items():
    result = []
    errors = []
    pipe([1, 2, 3]).catch(
        lambda x: x * 2, on_error=lambda item, _e: errors.append(item)
    ).run(sink=result.append)
    assert result == [2, 4, 6]
    assert errors == []


def test_catch_routes_failed_items_to_handler():
    result = []
    errors = []

    def fail_on_2(x):
        if x == 2:
            raise ValueError("bad")
        return x * 10

    pipe([1, 2, 3]).catch(fail_on_2, on_error=lambda item, _e: errors.append(item)).run(
        sink=result.append
    )
    assert result == [10, 30]
    assert errors == [2]


def test_catch_counts_errors_in_stats():
    def always_fail(x):
        raise ValueError("nope")

    stats = pipe([1, 2, 3]).catch(always_fail, on_error=lambda _item, _e: None).run()
    assert stats.errors == 3


def test_catch_only_catches_specified_exceptions():
    def raises_type_error(x):
        raise TypeError("wrong")

    with pytest.raises(TypeError):
        pipe([1]).catch(
            raises_type_error,
            on_error=lambda _item, _e: None,
            exceptions=(ValueError,),
        ).run()


def test_chaining_filter_map_batch():
    result = []
    (
        pipe(range(10))
        .filter(lambda x: x % 2 == 0)
        .map(lambda x: x * 3)
        .batch(2)
        .run(sink=result.append)
    )
    # even: 0,2,4,6,8 → *3 → 0,6,12,18,24 → batched by 2
    assert result == [[0, 6], [12, 18], [24]]


def test_chaining_tap_does_not_alter_output():
    log = []
    result = []
    pipe([1, 2, 3]).tap(log.append).map(lambda x: x + 10).run(sink=result.append)
    assert log == [1, 2, 3]
    assert result == [11, 12, 13]


def test_lazy_no_work_before_run():
    side = []
    p = pipe([1, 2, 3]).tap(side.append)
    assert side == [], "tap must not fire before run()"
    p.run()
    assert side == [1, 2, 3]


def test_multiple_runs_on_list_source():
    source = [1, 2, 3]
    p = pipe(source).map(lambda x: x * 2)

    r1: list = []
    p.run(sink=r1.append)

    r2: list = []
    p.run(sink=r2.append)

    assert r1 == r2 == [2, 4, 6]


def test_stats_processed():
    stats = pipe(range(10)).run()
    assert stats.processed == 10


def test_stats_emitted_no_ops():
    stats = pipe(range(10)).run()
    assert stats.emitted == 10


def test_stats_emitted_with_filter():
    stats = pipe(range(10)).filter(lambda x: x < 5).run()
    assert stats.processed == 10
    assert stats.emitted == 5


def test_stats_dropped_by_filter():
    stats = pipe(range(10)).filter(lambda x: x < 5).run()
    assert stats.dropped == 5


def test_stats_dropped_by_dedupe():
    stats = pipe([1, 1, 2, 2, 3]).dedupe().run()
    assert stats.dropped == 2


def test_stats_batches():
    stats = pipe(range(7)).batch(3).run()
    assert stats.batches == 3


def test_stats_errors_from_retry():
    attempt = [0]

    def flaky(x):
        attempt[0] += 1
        if attempt[0] < 3:
            raise ValueError()
        return x

    stats = pipe([1]).retry_map(flaky, retries=3, backoff=0).run()
    assert stats.errors == 2


def test_stats_duration_is_non_negative():
    stats = pipe(range(100)).map(lambda x: x).run()
    assert stats.duration >= 0.0


def test_run_without_sink_returns_stats():
    stats = pipe([1, 2, 3]).map(lambda x: x * 2).run()
    assert stats.emitted == 3
    assert stats.processed == 3


def test_empty_source():
    result = []
    stats = pipe([]).map(lambda x: x).run(sink=result.append)
    assert result == []
    assert stats.processed == 0
    assert stats.emitted == 0
