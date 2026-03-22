-- stg_task_events.sql
-- Staging model: rename columns from task_event_raw
-- Grain: 1 row per (task_id, event_sequence, event_type)
--
-- DATA QUALITY ISSUE:
--   The `id` column in task_event_raw is truncated due to a precision loss
--   in the Excel export (scientific notation rounding). All 2,134 events map
--   to only 2 distinct IDs (11732000000 and 11716000000) instead of the full
--   1,000 unique task IDs found in task_base. This breaks the join to task_base.
--
--   Until the source system provides full-precision IDs, event-level joins to
--   fct_tasks will produce incorrect fan-outs. Flag this for remediation with
--   the upstream data team.
--
-- Other observations:
--   - ix0 = event sequence index within a task (0-based); ix0=0 is always ACQUIRED
--   - Event types: ACQUIRED, SUBMITTED, PROJECT_REGISTRATION, RESULTS_RELEASED
--   - Timestamps are time-only strings (HH:MM:SS), no date component
--   - description contains project IDs for PROJECT_REGISTRATION events

SELECT
    id                          AS task_id,
    ix0                         AS event_sequence,
    type                        AS event_type,
    timestamp                   AS event_timestamp,
    description,
    additional_description,

    -- Extract project_id from description for PROJECT_REGISTRATION events
    -- description format: "Project 189022838"
    CASE
        WHEN type = 'PROJECT_REGISTRATION'
        THEN REPLACE(description, 'Project ', '')
        ELSE NULL
    END                         AS project_id

FROM task_event_raw
