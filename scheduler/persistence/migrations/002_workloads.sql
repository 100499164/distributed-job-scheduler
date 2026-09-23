-- Evolve existing installations without changing 001 or its recorded checksum.
ALTER TABLE jobs DROP CONSTRAINT jobs_operation_check;
ALTER TABLE jobs ADD CONSTRAINT jobs_operation_check
    CHECK (operation IN ('PRIME_COUNT', 'RANGE_SUM', 'MONTE_CARLO_PI'));
