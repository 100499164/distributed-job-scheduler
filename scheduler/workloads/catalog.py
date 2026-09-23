"""Partitioning, validation and result aggregation for supported workloads."""

import hashlib
from collections.abc import Callable, Iterable, Iterator, Mapping
from dataclasses import dataclass

from scheduler.protocol.models import (
    MonteCarloPayload,
    MonteCarloResult,
    Payload,
    PrimeCountResult,
    PrimePayload,
    RangeSumPayload,
    RangeSumResult,
    TaskResult,
)


def range_partitions(payload: Payload, count: int) -> Iterator[tuple[int, dict[str, str | int]]]:
    assert isinstance(payload, (PrimePayload, RangeSumPayload))
    start = payload.from_inclusive
    for index, size in enumerate(partition_sizes(payload.work_units, count)):
        end = start + size
        yield index, {"operation": payload.operation, "fromInclusive": start, "toExclusive": end}
        start = end


def partition_sizes(units: int, count: int) -> Iterator[int]:
    if not 1 <= count <= units:
        raise ValueError("Partitions must be nonempty")
    q, r = divmod(units, count)
    for index in range(count):
        yield q + (index < r)


def partition_seed(seed: int, index: int) -> int:
    digest = hashlib.sha256(f"monte-carlo-pi:v1:{seed}:{index}".encode("ascii")).digest()
    return int.from_bytes(digest[:8], "big") >> 11


def monte_carlo_partitions(payload: Payload, count: int) -> Iterator[tuple[int, dict[str, str | int]]]:
    assert isinstance(payload, MonteCarloPayload)
    for index, samples in enumerate(partition_sizes(payload.samples, count)):
        yield (
            index,
            {"operation": payload.operation, "samples": samples, "seed": partition_seed(payload.seed, index)},
        )


def validate_prime(payload: Payload, result: TaskResult) -> None:
    assert isinstance(payload, PrimePayload) and isinstance(result, PrimeCountResult)
    if result.prime_count > payload.work_units:
        raise ValueError("primeCount exceeds interval length")


def validate_sum(payload: Payload, result: TaskResult) -> None:
    assert isinstance(payload, RangeSumPayload) and isinstance(result, RangeSumResult)
    if (
        not payload.work_units * payload.from_inclusive
        <= result.range_sum
        <= payload.work_units * (payload.to_exclusive - 1)
    ):
        raise ValueError("rangeSum outside partition bounds")


def validate_monte_carlo(payload: Payload, result: TaskResult) -> None:
    assert isinstance(payload, MonteCarloPayload) and isinstance(result, MonteCarloResult)
    if result.samples != payload.samples:
        raise ValueError("samples must match the assigned partition")


def reduce_prime(results: Iterable[Mapping[str, int]]) -> dict[str, int | float]:
    return {"totalPrimeCount": sum(r["primeCount"] for r in results)}


def reduce_sum(results: Iterable[Mapping[str, int]]) -> dict[str, int | float]:
    return {"totalSum": sum(r["rangeSum"] for r in results)}


def reduce_monte_carlo(results: Iterable[Mapping[str, int]]) -> dict[str, int | float]:
    samples, inside = 0, 0
    for result in results:
        samples += result["samples"]
        inside += result["insideCircle"]
    return {"samples": samples, "insideCircle": inside, "piEstimate": 4 * inside / samples}


@dataclass(frozen=True)
class Workload:
    payload_type: type[PrimePayload] | type[RangeSumPayload] | type[MonteCarloPayload]
    result_type: type[PrimeCountResult] | type[RangeSumResult] | type[MonteCarloResult]
    partition: Callable[[Payload, int], Iterable[tuple[int, dict[str, str | int]]]]
    validate: Callable[[Payload, TaskResult], None]
    reduce: Callable[[Iterable[Mapping[str, int]]], dict[str, int | float]]

    def validate_result(self, payload: Mapping[str, object], result: TaskResult) -> None:
        if type(result) is not self.result_type:
            raise ValueError("Result schema does not match task operation")
        self.validate(self.payload_type.model_validate(payload), result)


WORKLOADS = {
    "PRIME_COUNT": Workload(PrimePayload, PrimeCountResult, range_partitions, validate_prime, reduce_prime),
    "RANGE_SUM": Workload(RangeSumPayload, RangeSumResult, range_partitions, validate_sum, reduce_sum),
    "MONTE_CARLO_PI": Workload(
        MonteCarloPayload, MonteCarloResult, monte_carlo_partitions, validate_monte_carlo, reduce_monte_carlo
    ),
}


class UnsupportedOperation(ValueError):
    pass


def workload(operation: str) -> Workload:
    try:
        return WORKLOADS[operation]
    except (KeyError, TypeError):
        raise UnsupportedOperation("Unsupported workload operation") from None


def partitions(payload: Payload, count: int) -> Iterable[tuple[int, dict[str, str | int]]]:
    return workload(payload.operation).partition(payload, count)
