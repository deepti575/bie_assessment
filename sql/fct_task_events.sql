-- fct_task_events.sql
-- Fact table: cleaned event stream with lag calculations
-- Grain: 1 row per (task_id, event_sequence, event_type)
--
-- Use cases:
--   - Funnel analysis (what % of tasks reach each stage?)
--   - Time-between-events analysis
--   - PROJECT_REGISTRATION pattern analysis
--   - Identifying stuck/abandoned tasks
--
-- DATA QUALITY NOTE:
--   Event IDs are truncated in the source (only 2 distinct values). This table
--   will not produce meaningful per-task analysis until full-precision IDs are
--   provided. The lag calculations will span across unrelated tasks grouped
--   under the same truncated ID.

WITH events AS (
    SELECT * FROM {{ ref('stg_task_events') }}
),

with_lag AS (
    SELECT
        task_id,
        event_sequence,
        event_type,
        event_timestamp,
        description,
        additional_description,
        project_id,

        -- Previous event within same task (ordered by sequence then timestamp)
        LAG(event_type) OVER (
            PARTITION BY task_id
            ORDER BY event_sequence, event_timestamp
        )                       AS prev_event_type,

        LAG(event_timestamp) OVER (
            PARTITION BY task_id
            ORDER BY event_sequence, event_timestamp
        )                       AS prev_event_timestamp

    FROM events
)

SELECT
    task_id,
    event_sequence,
    event_type,
    event_timestamp,
    description,
    additional_description,
    project_id,
    prev_event_type,
    prev_event_timestamp,

    -- Duration since previous event (minutes)
    -- NOTE: event_timestamp is time-only (HH:MM:SS), so DATEDIFF may not
    -- work correctly across midnight boundaries
    DATEDIFF('minute', prev_event_timestamp, event_timestamp) AS minutes_since_prev_event,

    -- Is this the first event for the task?
    CASE WHEN prev_event_type IS NULL
         THEN TRUE ELSE FALSE
    END                         AS is_first_event

FROM with_lag
