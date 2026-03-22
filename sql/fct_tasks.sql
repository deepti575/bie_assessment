-- fct_tasks.sql
-- Fact table: one row per task
-- Grain: 1 row per task_id
--
-- Design rationale:
--   This is the primary analytical table. It joins the task record
--   with pivoted event timestamps so analysts can query task volume,
--   throughput, and worker productivity without touching the event stream.
--
-- Key data considerations:
--   - insert_time is a full datetime; task_date (DATE cast) enables dim_dates joins
--   - Batch tasks (task_submission_count > 1) have fractional estimated_task_time
--     (~0.67 min) and different completion semantics
--   - Some tasks have multiple SUBMITTED events (resubmissions). We use the FINAL
--     submission timestamp.
--
-- DATA QUALITY NOTE:
--   Event IDs in task_event_raw are truncated (only 2 distinct values due to
--   Excel precision loss). The LEFT JOINs to event CTEs will NOT produce correct
--   matches until the source provides full-precision event IDs. Event-derived
--   columns (acquired_at, submitted_at, time_to_submit_minutes, event counts,
--   and status flags) should be treated as unreliable until this is resolved.

WITH tasks AS (
    SELECT * FROM {{ ref('stg_tasks') }}
),

-- First ACQUIRED event per task (always ix0 = 0)
acquired AS (
    SELECT
        task_id,
        MIN(event_timestamp) AS acquired_at
    FROM {{ ref('stg_task_events') }}
    WHERE event_type = 'ACQUIRED'
    GROUP BY task_id
),

-- Final SUBMITTED event per task (latest timestamp among SUBMITTED events)
submitted AS (
    SELECT
        task_id,
        MAX(event_timestamp) AS submitted_at
    FROM {{ ref('stg_task_events') }}
    WHERE event_type = 'SUBMITTED'
    GROUP BY task_id
),

-- Event-level aggregates per task
event_counts AS (
    SELECT
        task_id,
        COUNT(*)                                                            AS total_event_count,
        COUNT(CASE WHEN event_type = 'SUBMITTED' THEN 1 END)               AS submit_event_count,
        COUNT(CASE WHEN event_type = 'PROJECT_REGISTRATION' THEN 1 END)    AS project_reg_count,
        COUNT(CASE WHEN event_type = 'RESULTS_RELEASED' THEN 1 END)        AS results_released_count,
        MAX(project_id)                                                     AS project_id
    FROM {{ ref('stg_task_events') }}
    GROUP BY task_id
)

SELECT
    -- Task identifiers
    t.task_id,
    t.worker_id,
    t.item_id,

    -- Task metadata
    t.insert_time,
    t.task_date,
    t.task_submission_count,
    t.task_acquire_count,
    t.estimated_task_time,
    t.reviewed_by_rater_in_feedback_portal,
    t.is_batch_task,

    -- Event-derived timestamps
    a.acquired_at,
    s.submitted_at,

    -- Time-to-submit (minutes between ACQUIRED and final SUBMITTED)
    DATEDIFF('minute', a.acquired_at, s.submitted_at)   AS time_to_submit_minutes,

    -- Event counts
    COALESCE(ec.total_event_count, 0)                   AS total_event_count,
    COALESCE(ec.submit_event_count, 0)                  AS submit_event_count,
    COALESCE(ec.project_reg_count, 0)                   AS project_reg_count,
    COALESCE(ec.results_released_count, 0)              AS results_released_count,
    ec.project_id,

    -- Status flags
    CASE WHEN s.submitted_at IS NOT NULL
         THEN TRUE ELSE FALSE
    END                                                 AS is_submitted,

    CASE WHEN COALESCE(ec.submit_event_count, 0) > 1
         THEN TRUE ELSE FALSE
    END                                                 AS is_resubmitted,

    CASE WHEN COALESCE(ec.project_reg_count, 0) > 0
         THEN TRUE ELSE FALSE
    END                                                 AS has_project_registration,

    CASE WHEN COALESCE(ec.results_released_count, 0) > 0
         THEN TRUE ELSE FALSE
    END                                                 AS has_results_released

FROM tasks t
LEFT JOIN acquired a       ON t.task_id = a.task_id
LEFT JOIN submitted s      ON t.task_id = s.task_id
LEFT JOIN event_counts ec  ON t.task_id = ec.task_id
