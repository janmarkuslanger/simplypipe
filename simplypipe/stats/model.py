from dataclasses import dataclass


@dataclass
class RunStats:
    processed: int = 0  # items consumed from source
    emitted: int = 0  # items delivered to sink
    dropped: int = 0  # items removed by filter or dedupe
    batches: int = 0  # batches produced by the batch operator
    errors: int = 0  # exceptions caught by retry operators
    duration: float = 0.0  # wall-clock time in seconds
