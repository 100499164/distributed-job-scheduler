from collections.abc import Callable, Mapping
from math import isqrt

from scheduler.protocol.models import MonteCarloPayload, Payload, PrimePayload, RangeSumPayload


class Cancelled(Exception):
    pass


def prime_count(start: int, end: int, cancelled: Callable[[], bool] = lambda: False) -> int:
    """Count primes in a range, checking periodically for cancellation."""
    if not 2 <= start < end <= 1_000_000_000:
        raise ValueError("Invalid PRIME_COUNT interval")
    count = int(start <= 2 < end)
    for number in range(max(3, start | 1), end, 2):
        if cancelled():
            raise Cancelled()
        bound, divisor = isqrt(number), 3
        while divisor <= bound and number % divisor:
            if divisor % 255 == 0 and cancelled():
                raise Cancelled()
            divisor += 2
        if divisor > bound:
            count += 1
    if cancelled():
        raise Cancelled()
    return count


def execute_prime(payload: Payload, cancelled: Callable[[], bool]) -> dict[str, int]:
    assert isinstance(payload, PrimePayload)
    return {"primeCount": prime_count(payload.from_inclusive, payload.to_exclusive, cancelled)}


def execute_sum(payload: Payload, cancelled: Callable[[], bool]) -> dict[str, int]:
    assert isinstance(payload, RangeSumPayload)
    if cancelled():
        raise Cancelled()
    count = payload.to_exclusive - payload.from_inclusive
    return {"rangeSum": count * (payload.from_inclusive + payload.to_exclusive - 1) // 2}


def execute_monte_carlo(payload: Payload, cancelled: Callable[[], bool]) -> dict[str, int]:
    assert isinstance(payload, MonteCarloPayload)
    from random import Random

    random = Random(payload.seed)
    inside = 0
    for index in range(payload.samples):
        if index % 1024 == 0 and cancelled():
            raise Cancelled()
        x, y = random.random(), random.random()
        inside += x * x + y * y <= 1.0
    if cancelled():
        raise Cancelled()
    return {"samples": payload.samples, "insideCircle": inside}


EXECUTORS = {
    "PRIME_COUNT": execute_prime,
    "RANGE_SUM": execute_sum,
    "MONTE_CARLO_PI": execute_monte_carlo,
}


def execute(payload: Mapping[str, object], cancelled: Callable[[], bool] = lambda: False) -> dict[str, int]:
    """Validate a payload, execute its workload and validate the result."""
    from scheduler.workloads.catalog import UnsupportedOperation, workload

    operation = payload.get("operation", "PRIME_COUNT")
    if not isinstance(operation, str):
        raise UnsupportedOperation("Operation must be a string")
    definition = workload(operation)
    executor = EXECUTORS.get(operation)
    if executor is None:
        raise UnsupportedOperation("Worker does not support this operation")
    validated = definition.payload_type.model_validate(payload)
    result = definition.result_type.model_validate(executor(validated, cancelled))
    definition.validate_result(payload, result)
    return result.model_dump(by_alias=True)
