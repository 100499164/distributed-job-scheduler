CREATE TABLE jobs (
    id UUID PRIMARY KEY,
    name VARCHAR(120) NOT NULL CHECK (length(name) BETWEEN 1 AND 120),
    operation VARCHAR(32) NOT NULL CHECK (operation = 'PRIME_COUNT'),
    status VARCHAR(16) NOT NULL CHECK (status IN ('QUEUED', 'RUNNING', 'COMPLETED', 'FAILED')),
    payload JSONB NOT NULL CHECK (jsonb_typeof(payload) = 'object'),
    task_count INTEGER NOT NULL CHECK (task_count BETWEEN 1 AND 10000),
    completed_tasks INTEGER NOT NULL DEFAULT 0 CHECK (completed_tasks >= 0),
    failed_tasks INTEGER NOT NULL DEFAULT 0 CHECK (failed_tasks >= 0),
    idempotency_key TEXT NOT NULL UNIQUE CHECK (length(idempotency_key) > 0),
    request_hash CHAR(64) NOT NULL CHECK (request_hash ~ '^[0-9a-f]{64}$'),
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    started_at TIMESTAMPTZ,
    finished_at TIMESTAMPTZ,
    CONSTRAINT jobs_counter_bound CHECK (completed_tasks + failed_tasks <= task_count),
    CONSTRAINT jobs_terminal_time CHECK ((status IN ('COMPLETED', 'FAILED')) = (finished_at IS NOT NULL)),
    CONSTRAINT jobs_start_time CHECK ((status = 'QUEUED') = (started_at IS NULL)),
    CONSTRAINT jobs_time_order CHECK (
        (started_at IS NULL OR started_at >= created_at) AND
        (finished_at IS NULL OR finished_at >= started_at)),
    CONSTRAINT jobs_terminal_counts CHECK (
        (status IN ('COMPLETED', 'FAILED')) = (completed_tasks + failed_tasks = task_count)),
    CONSTRAINT jobs_success_counts CHECK (status <> 'COMPLETED' OR failed_tasks = 0),
    CONSTRAINT jobs_failure_counts CHECK (status <> 'FAILED' OR failed_tasks > 0)
);

CREATE TABLE tasks (
    id UUID PRIMARY KEY,
    job_id UUID NOT NULL REFERENCES jobs(id),
    partition_index INTEGER NOT NULL CHECK (partition_index >= 0),
    status VARCHAR(16) NOT NULL CHECK (status IN ('QUEUED', 'ASSIGNED', 'RUNNING', 'RETRY_WAIT', 'COMPLETED', 'FAILED')),
    payload JSONB NOT NULL CHECK (jsonb_typeof(payload) = 'object'),
    attempt_count INTEGER NOT NULL DEFAULT 0,
    max_retries INTEGER NOT NULL CHECK (max_retries BETWEEN 0 AND 10),
    available_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    last_error_code VARCHAR(64),
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    first_started_at TIMESTAMPTZ,
    finished_at TIMESTAMPTZ,
    UNIQUE (job_id, partition_index),
    CONSTRAINT tasks_attempt_budget CHECK (attempt_count BETWEEN 0 AND max_retries + 1),
    CONSTRAINT tasks_reserved_before_execution CHECK ((status = 'QUEUED') = (attempt_count = 0)),
    CONSTRAINT tasks_terminal_time CHECK ((status IN ('COMPLETED', 'FAILED')) = (finished_at IS NOT NULL)),
    CONSTRAINT tasks_started_for_success CHECK (status NOT IN ('RUNNING', 'COMPLETED') OR first_started_at IS NOT NULL),
    CONSTRAINT tasks_time_order CHECK (
        (first_started_at IS NULL OR first_started_at >= created_at) AND
        (finished_at IS NULL OR finished_at >= created_at) AND
        (finished_at IS NULL OR first_started_at IS NULL OR finished_at >= first_started_at))
);

CREATE TABLE workers (
    id UUID PRIMARY KEY,
    hostname TEXT NOT NULL,
    version TEXT NOT NULL,
    status VARCHAR(16) NOT NULL CHECK (status IN ('ONLINE', 'OFFLINE')),
    capacity INTEGER NOT NULL CHECK (capacity BETWEEN 1 AND 64),
    registered_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    last_heartbeat_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    offline_at TIMESTAMPTZ,
    CONSTRAINT workers_terminal_time CHECK ((status = 'OFFLINE') = (offline_at IS NOT NULL)),
    CONSTRAINT workers_time_order CHECK (
        last_heartbeat_at >= registered_at AND
        (offline_at IS NULL OR offline_at >= last_heartbeat_at))
);

CREATE TABLE task_attempts (
    id UUID PRIMARY KEY,
    task_id UUID NOT NULL REFERENCES tasks(id),
    worker_id UUID NOT NULL REFERENCES workers(id),
    attempt_number INTEGER NOT NULL CHECK (attempt_number > 0),
    claim_request_id UUID NOT NULL,
    status VARCHAR(16) NOT NULL CHECK (status IN ('ASSIGNED', 'RUNNING', 'SUCCEEDED', 'FAILED', 'EXPIRED')),
    assigned_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    started_at TIMESTAMPTZ,
    finished_at TIMESTAMPTZ,
    lease_expires_at TIMESTAMPTZ NOT NULL,
    execution_deadline_at TIMESTAMPTZ,
    error_code VARCHAR(64),
    error_message TEXT CHECK (octet_length(error_message) <= 2048),
    result JSONB CHECK (jsonb_typeof(result) = 'object' AND octet_length(result::text) <= 4096),
    completion_hash CHAR(64) CHECK (completion_hash ~ '^[0-9a-f]{64}$'),
    UNIQUE (task_id, attempt_number),
    UNIQUE (worker_id, claim_request_id),
    CONSTRAINT attempts_terminal_time CHECK ((status IN ('SUCCEEDED', 'FAILED', 'EXPIRED')) = (finished_at IS NOT NULL)),
    CONSTRAINT attempts_completion_hash CHECK ((status IN ('SUCCEEDED', 'FAILED')) = (completion_hash IS NOT NULL)),
    CONSTRAINT attempts_success_result CHECK ((status = 'SUCCEEDED') = (result IS NOT NULL)),
    CONSTRAINT attempts_failure_cause CHECK ((status IN ('FAILED', 'EXPIRED')) = (error_code IS NOT NULL)),
    CONSTRAINT attempts_error_message CHECK (error_message IS NULL OR error_code IS NOT NULL),
    CONSTRAINT attempts_start_and_deadline CHECK ((started_at IS NULL) = (execution_deadline_at IS NULL)),
    CONSTRAINT attempts_started CHECK (status NOT IN ('RUNNING', 'SUCCEEDED', 'FAILED') OR started_at IS NOT NULL),
    CONSTRAINT attempts_assigned_not_started CHECK (status <> 'ASSIGNED' OR started_at IS NULL),
    CONSTRAINT attempts_time_order CHECK (
        lease_expires_at > assigned_at AND
        (started_at IS NULL OR started_at >= assigned_at) AND
        (execution_deadline_at IS NULL OR execution_deadline_at > started_at) AND
        (execution_deadline_at IS NULL OR lease_expires_at <= execution_deadline_at) AND
        (finished_at IS NULL OR finished_at >= assigned_at) AND
        (finished_at IS NULL OR started_at IS NULL OR finished_at >= started_at))
);

CREATE UNIQUE INDEX attempts_one_active_per_task ON task_attempts(task_id)
    WHERE status IN ('ASSIGNED', 'RUNNING');
CREATE UNIQUE INDEX attempts_one_success_per_task ON task_attempts(task_id)
    WHERE status = 'SUCCEEDED';
CREATE INDEX tasks_eligible ON tasks(available_at, created_at, id)
    WHERE status IN ('QUEUED', 'RETRY_WAIT');
-- UNIQUE(job_id, partition_index) and UNIQUE(task_id, attempt_number) already provide their lookup indexes.
CREATE INDEX attempts_active_worker ON task_attempts(worker_id)
    WHERE status IN ('ASSIGNED', 'RUNNING');
CREATE INDEX attempts_active_lease ON task_attempts(lease_expires_at)
    WHERE status IN ('ASSIGNED', 'RUNNING');
CREATE INDEX workers_online_heartbeat ON workers(last_heartbeat_at) WHERE status = 'ONLINE';
CREATE INDEX jobs_created ON jobs(created_at, id);
CREATE INDEX jobs_status_created ON jobs(status, created_at, id);
