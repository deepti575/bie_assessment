-- stg_workers.sql
-- Staging model: rename columns from worker_base
-- Grain: 1 row per (worker_id, employment_cycle)
--
-- Data observations:
--   - ix0 = employment cycle index (0 = first hire, 1 = first rehire, etc.)
--     NOT the same as ix0 in task_event_raw (which means event sequence)
--   - All workers are type = 'gaia'
--   - Workers can have up to 7 cycles (ix0 = 0 through 6)
--   - fire_reason values: FAILED_EXAM (most common), FIRED, QUIT, UNKNOWN, NULL
--   - hire_timestamp and fire_timestamp are full datetimes
--   - fire_timestamp is NULL when worker is still active (aligns with fire_reason being NULL)
--   - 32.9% of workers have been rehired at least once

SELECT
    id                          AS worker_id,
    type                        AS worker_type,
    ix0                         AS employment_cycle,
    hire_timestamp,
    fire_timestamp,
    exam_batch_id,
    fire_reason,

    -- Derived: was this cycle terminated?
    CASE
        WHEN fire_reason IS NOT NULL THEN TRUE
        ELSE FALSE
    END                         AS is_terminated

FROM worker_base
