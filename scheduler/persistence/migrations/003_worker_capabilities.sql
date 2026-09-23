-- Existing workers and legacy registrations support all workloads of this release.
ALTER TABLE workers ADD COLUMN supported_operations TEXT[] NOT NULL
    DEFAULT ARRAY['MONTE_CARLO_PI','PRIME_COUNT','RANGE_SUM']::text[];
ALTER TABLE workers ADD CONSTRAINT workers_supported_operations CHECK (
    array_ndims(supported_operations) = 1
    AND cardinality(supported_operations) BETWEEN 1 AND 3
    AND array_position(supported_operations, NULL) IS NULL
    AND supported_operations <@ ARRAY['MONTE_CARLO_PI','PRIME_COUNT','RANGE_SUM']::text[]
    AND cardinality(supported_operations) =
        ('MONTE_CARLO_PI' = ANY(supported_operations))::int +
        ('PRIME_COUNT' = ANY(supported_operations))::int +
        ('RANGE_SUM' = ANY(supported_operations))::int
);

-- Fair selection probes eligibility, then takes the oldest unlocked task within a job.
CREATE INDEX tasks_eligible_per_job ON tasks(job_id, available_at, created_at, id)
    WHERE status IN ('QUEUED', 'RETRY_WAIT');
