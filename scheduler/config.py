import os
from dataclasses import dataclass, fields


@dataclass(frozen=True)
class Settings:
    poll_interval_ms: int = 500
    heartbeat_interval_ms: int = 5000
    worker_timeout_ms: int = 30000
    assignment_timeout_ms: int = 15000
    execution_lease_ms: int = 30000
    max_execution_ms: int = 300000
    recovery_interval_ms: int = 1000
    default_max_retries: int = 3
    body_limit: int = 65536
    max_tasks: int = 10000
    max_capacity: int = 64

    def __post_init__(self) -> None:
        for field in fields(self):
            value = getattr(self, field.name)
            if field.name == "default_max_retries":
                if not 0 <= value <= 10:
                    raise ValueError("DEFAULT_MAX_RETRIES must be in [0,10]")
            elif value <= 0:
                raise ValueError(f"{field.name} must be positive")
        if self.heartbeat_interval_ms >= min(self.worker_timeout_ms, self.execution_lease_ms):
            raise ValueError("Heartbeat interval must be below worker timeout and execution lease")
        if self.max_tasks > 10000 or self.max_capacity > 64:
            raise ValueError("Configured limits exceed the supported maximum")

    @classmethod
    def from_env(cls) -> "Settings":
        return cls(
            **{f.name: int(os.environ[f.name.upper()]) for f in fields(cls) if f.name.upper() in os.environ}
        )

    def wire(self) -> dict[str, int]:
        from scheduler.protocol.models import camel

        return {camel(f.name): getattr(self, f.name) for f in fields(self)}
