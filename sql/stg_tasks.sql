-- stg_tasks.sql
-- Staging model: rename and cast columns from task_base
-- Grain: 1 row per task_id (already unique in source — 1,000 unique IDs)
--
-- Data observations:
--   - insert_time is a full datetime (e.g., 2025-12-01 20:16:27)
--     Date range: 2025-12-01 through 2026-01-01
--   - task_submission_count / task_acquire_count are mostly 1;
--     batch tasks have count=3 with estimated_task_time ≈ 0.67 min (~3.9% of tasks)
--   - reviewed_by_rater_in_feedback_portal is almost entirely null (1 non-null in 1,000 rows)

SELECT
    id                                          AS task_id,
    user_id                                     AS worker_id,
    item_id,
    insert_time,
    CAST(insert_time AS DATE)                   AS task_date,    -- for joining to dim_dates
    task_submission_count,
    task_acquire_count,
    estimated_task_time,
    reviewed_by_rater_in_feedback_portal,

    -- Flag batch tasks (bundled tasks with count > 1)
    CASE
        WHEN task_submission_count > 1 THEN TRUE
        ELSE FALSE
    END                                         AS is_batch_task

FROM task_base
