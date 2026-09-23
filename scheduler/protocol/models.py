"""Request and response models shared by the scheduler and workers."""

from typing import Annotated, Literal, Self, get_args
from uuid import UUID

from pydantic import AfterValidator, BaseModel, ConfigDict, Field, StrictInt, field_validator, model_validator

Operation = Literal["PRIME_COUNT", "RANGE_SUM", "MONTE_CARLO_PI"]
KNOWN_OPERATIONS = tuple(sorted(get_args(Operation)))


def canonical_operations(operations: list[Operation]) -> list[Operation]:
    if len(set(operations)) != len(operations):
        raise ValueError("Duplicate supported operation")
    return sorted(operations)


SupportedOperations = Annotated[
    list[Operation], Field(strict=True, min_length=1, max_length=3), AfterValidator(canonical_operations)
]


def camel(value: str) -> str:
    first, *rest = value.split("_")
    return first + "".join(word.title() for word in rest)


class WireModel(BaseModel):
    model_config = ConfigDict(extra="forbid", populate_by_name=True, alias_generator=camel)


class IntervalPayload(WireModel):
    from_inclusive: StrictInt
    to_exclusive: StrictInt

    @model_validator(mode="after")
    def interval(self) -> Self:
        if self.from_inclusive >= self.to_exclusive:
            raise ValueError("Interval must be nonempty")
        return self

    @property
    def work_units(self) -> int:
        return self.to_exclusive - self.from_inclusive


class PrimePayload(IntervalPayload):
    operation: Literal["PRIME_COUNT"] = "PRIME_COUNT"
    from_inclusive: Annotated[StrictInt, Field(ge=2, le=999_999_999)]
    to_exclusive: Annotated[StrictInt, Field(ge=3, le=1_000_000_000)]


class RangeSumPayload(IntervalPayload):
    operation: Literal["RANGE_SUM"] = "RANGE_SUM"
    from_inclusive: Annotated[StrictInt, Field(ge=-100_000_000, le=99_999_999)]
    to_exclusive: Annotated[StrictInt, Field(ge=-99_999_999, le=100_000_000)]


class MonteCarloPayload(WireModel):
    operation: Literal["MONTE_CARLO_PI"] = "MONTE_CARLO_PI"
    samples: Annotated[StrictInt, Field(ge=1, le=100_000_000)]
    seed: Annotated[StrictInt, Field(ge=0, le=2**53 - 1)]

    @property
    def work_units(self) -> int:
        return self.samples


Payload = Annotated[PrimePayload | RangeSumPayload | MonteCarloPayload, Field(discriminator="operation")]


class CreateJob(WireModel):
    name: Annotated[str, Field(min_length=1, max_length=120)]
    task_count: Annotated[StrictInt, Field(ge=1, le=10000)]
    payload: Payload
    max_retries: Annotated[StrictInt, Field(ge=0, le=10)] | None = None

    @field_validator("payload", mode="before")
    @classmethod
    def legacy_operation(cls, value: object) -> object:
        if isinstance(value, dict) and "operation" not in value:
            return {**value, "operation": "PRIME_COUNT"}
        return value

    @field_validator("name")
    @classmethod
    def nonblank(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("Name cannot be blank")
        return value

    @model_validator(mode="after")
    def nonempty_partitions(self) -> Self:
        if self.task_count > self.payload.work_units:
            raise ValueError("Each partition must contain at least one work unit")
        return self


class Register(WireModel):
    worker_id: UUID
    hostname: Annotated[str, Field(min_length=1, max_length=255)]
    capacity: Annotated[StrictInt, Field(ge=1, le=64)]
    version: Literal["1"]
    supported_operations: SupportedOperations = Field(default_factory=lambda: list(KNOWN_OPERATIONS))


class Claim(WireModel):
    worker_id: UUID
    claim_request_id: UUID


class Owner(WireModel):
    worker_id: UUID


class Heartbeat(WireModel):
    active_attempt_ids: Annotated[list[UUID], Field(max_length=64)]

    @field_validator("active_attempt_ids")
    @classmethod
    def unique(cls, value: list[UUID]) -> list[UUID]:
        if len(set(value)) != len(value):
            raise ValueError("Duplicate attempt IDs")
        return value


class PrimeCountResult(WireModel):
    prime_count: Annotated[StrictInt, Field(ge=0, le=1_000_000_000)]


class RangeSumResult(WireModel):
    range_sum: Annotated[StrictInt, Field(ge=-(2**53 - 1), le=2**53 - 1)]


class MonteCarloResult(WireModel):
    samples: Annotated[StrictInt, Field(ge=1, le=100_000_000)]
    inside_circle: Annotated[StrictInt, Field(ge=0, le=100_000_000)]

    @model_validator(mode="after")
    def bounded_hits(self) -> Self:
        if self.inside_circle > self.samples:
            raise ValueError("insideCircle exceeds samples")
        return self


TaskResult = PrimeCountResult | RangeSumResult | MonteCarloResult


class ExecutionError(WireModel):
    code: Literal["TRANSIENT_ERROR", "INVALID_PAYLOAD", "UNSUPPORTED_OPERATION", "EXECUTION_ERROR"]
    message: str

    @field_validator("message")
    @classmethod
    def byte_limit(cls, value: str) -> str:
        if len(value.encode("utf-8")) > 2048:
            raise ValueError("Error message exceeds 2 KiB")
        return value


class Completion(Owner):
    outcome: Literal["SUCCEEDED", "FAILED"]
    result: TaskResult | None = None
    error: ExecutionError | None = None

    @model_validator(mode="after")
    def outcome_content(self) -> Self:
        if self.outcome == "SUCCEEDED":
            if self.result is None or self.error is not None:
                raise ValueError("Success requires only result")
        elif self.error is None or self.result is not None:
            raise ValueError("Failure requires only error")
        return self
