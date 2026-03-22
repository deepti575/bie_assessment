-- dim_workers.sql
-- Dimension table: one row per worker (current/latest state)
-- Grain: 1 row per worker_id
--
-- Design rationale:
--   Workers have multiple employment cycles (hire → fire → rehire).
--   This dimension collapses all cycles into one row per worker,
--   carrying the LATEST cycle as "current state" plus aggregate
--   lifecycle stats (total cycles, failed exams, etc.).
--
--   The raw cycle-level detail lives in stg_workers for anyone
--   who needs the full history.

WITH worker_cycles AS (
    SELECT
        worker_id,
        worker_type,
        employment_cycle,
        hire_timestamp,
        fire_timestamp,
        exam_batch_id,
        fire_reason,
        is_terminated
    FROM {{ ref('stg_workers') }}
),

-- Latest employment cycle per worker (highest ix0)
latest_cycle AS (
    SELECT
        worker_id,
        MAX(employment_cycle) AS latest_cycle_ix
    FROM worker_cycles
    GROUP BY worker_id
),

-- Aggregate stats across all cycles
cycle_stats AS (
    SELECT
        worker_id,
        MIN(hire_timestamp)                                             AS first_hire_timestamp,
        COUNT(*)                                                        AS total_employment_cycles,
        COUNT(CASE WHEN fire_reason = 'FAILED_EXAM' THEN 1 END)        AS failed_exam_count,
        COUNT(CASE WHEN is_terminated = TRUE THEN 1 END)               AS termination_count
    FROM worker_cycles
    GROUP BY worker_id
)

SELECT
    wc.worker_id,
    wc.worker_type,

    -- Current (latest) cycle details
    wc.employment_cycle         AS current_cycle,
    wc.hire_timestamp           AS current_hire_timestamp,
    wc.fire_timestamp           AS current_fire_timestamp,
    wc.exam_batch_id            AS current_exam_batch_id,
    wc.fire_reason              AS current_fire_reason,

    -- Lifecycle aggregates
    cs.first_hire_timestamp,
    cs.total_employment_cycles,
    cs.failed_exam_count,
    cs.termination_count,

    -- Worker status (based on latest cycle)
    CASE
        WHEN wc.fire_reason IS NULL THEN 'active'
        ELSE 'inactive'
    END                                     AS worker_status,

    -- Termination category (based on latest cycle)
    CASE
        WHEN wc.fire_reason IS NULL        THEN 'active'
        WHEN wc.fire_reason = 'FAILED_EXAM' THEN 'failed_exam'
        WHEN wc.fire_reason = 'FIRED'       THEN 'fired'
        WHEN wc.fire_reason = 'QUIT'        THEN 'quit'
        WHEN wc.fire_reason = 'UNKNOWN'     THEN 'unknown'
        ELSE 'other'
    END                                     AS termination_category,

    -- Rehire flag
    CASE
        WHEN cs.total_employment_cycles > 1 THEN TRUE
        ELSE FALSE
    END                                     AS is_rehired_worker

FROM worker_cycles wc
INNER JOIN latest_cycle lc
    ON  wc.worker_id = lc.worker_id
    AND wc.employment_cycle = lc.latest_cycle_ix
LEFT JOIN cycle_stats cs
    ON wc.worker_id = cs.worker_id
