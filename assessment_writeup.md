# Business Intelligence Engineer — Technical Assessment

**Table of Contents**

1. [Modeling & Transformations](#part-1--modeling--transformations) — Data analysis, model, ERD, SQL
2. [Metrics Definition](#part-2--metrics-definition) — 5 operational metrics with SQL and caveats
3. [Write-up](#part-3--write-up) — Design decisions, data quality, scaling, collaboration
4. [Approach](#approach) — How the analysis was conducted

---

## Part 1 — Modeling & Transformations

### Data Analysis (pre-modeling)

I used a Python script ([`analyze_data.py`](analyze_data.py)) to programmatically profile all four sheets in the Excel workbook. The findings below come from that analysis.

#### `task_base` — 1,000 rows × 8 columns

| Column | Observations |
|--------|-------------|
| `id` | 1,000 unique integers in two ranges: `11716xxxxxx` (360 tasks) and `11731xxxxxx` (640 tasks). |
| `user_id` | 650 unique workers. FK to `worker_base.id`. Tasks per worker: 1–6. |
| `item_id` | 986 unique items. Multiple tasks can share the same `item_id`. |
| `insert_time` | Full datetime (e.g., `2025-12-01 20:16:27`). Spans 7 dates from 2025-12-01 to 2026-01-01. |
| `task_submission_count` | `1` for 961 tasks, `3` for 39 batch tasks (3.9%). |
| `estimated_task_time` | Non-batch: mean 5.5 min, range 1.67–17 min. Batch: `0.667` min. |
| `reviewed_by_rater` | 999 of 1,000 NULL. Effectively unused. |

> **Grain:** 1 row per `task_id` (unique, no dedup needed).

#### `task_event_raw` — 2,134 rows × 6 columns

| Column | Observations |
|--------|-------------|
| `id` | **Truncated to 2 values** (`11732000000`, `11716000000`) due to Excel precision loss. Breaks join to `task_base`. |
| `ix0` | Event sequence (0-based). `ix0=0` is always ACQUIRED. Not the same as `ix0` in `worker_base`. |
| `type` | `ACQUIRED` (1,000), `SUBMITTED` (1,011), `PROJECT_REGISTRATION` (120), `RESULTS_RELEASED` (3). |
| `timestamp` | Time-only strings (`HH:MM:SS`), no date. Unlike `task_base` which has full datetimes. |
| `description` | Project IDs for `PROJECT_REGISTRATION` events (e.g., `"Project 189022838"`). NULL otherwise. |

> **Grain:** 1 row per `(task_id, ix0, event_type)` — multiple events can share the same `ix0`.

**Task lifecycle patterns:**

```
Simple:    ACQUIRED(0) → SUBMITTED(1)
Complex:   ACQUIRED(0) → PROJECT_REGISTRATION(1) → SUBMITTED(2)
Resubmit:  ACQUIRED(0) → SUBMITTED(1) → SUBMITTED(2)
Reviewed:  ACQUIRED(0) → SUBMITTED(1) → RESULTS_RELEASED(2)
```

#### `worker_base` — 934 rows × 7 columns

| Column | Observations |
|--------|-------------|
| `id` | 650 unique workers. All match `task_base.user_id` (no orphans either way). |
| `type` | Always `'gaia'`. |
| `ix0` | Employment cycle (0 = first hire, 1+ = rehire). Max observed: 7 cycles. |
| `hire_timestamp` | Full datetime. Range: 2021-09-29 → 2025-12-30. |
| `fire_timestamp` | Full datetime when terminated; NULL when active. 625 NULL, 309 populated. Aligns perfectly with `fire_reason`. |
| `fire_reason` | `FAILED_EXAM` (297), `UNKNOWN` (9), `FIRED` (2), `QUIT` (1), NULL (625 active). |

> **Grain:** 1 row per `(worker_id, employment_cycle)`. 214 workers (32.9%) have been rehired at least once. 96.2% are currently active on their latest cycle.

#### `dim_dates` — 731 rows × 34 columns

Standard calendar dimension. Date range 2025-01-01 → 2027-01-01. Reference date (`age_days = 0`) is 2026-01-08. Includes ISO weeks, billing periods, YTD/QTD flags.

> `task_base.insert_time` and `worker_base` timestamps are full datetimes, so joining to `dim_dates` is straightforward via `CAST(insert_time AS DATE)`. Only `task_event_raw.timestamp` lacks a date.

---

### Entity Relationship Diagram

```
┌─────────────────────────────────┐
│          dim_workers            │
│─────────────────────────────────│
│ worker_id               (PK)   │
│ worker_type                     │
│ current_cycle                   │
│ current_hire/fire_timestamp     │
│ current_exam_batch_id           │
│ current_fire_reason             │
│ first_hire_timestamp            │
│ total_employment_cycles         │
│ failed_exam_count               │
│ termination_count               │
│ worker_status                   │
│ termination_category            │
│ is_rehired_worker               │
└───────────────┬─────────────────┘
                │
                │ worker_id (FK)
                │
┌───────────────▼──────────────────────────────────┐    ┌──────────────────┐
│                  fct_tasks                        │    │    dim_dates      │
│──────────────────────────────────────────────────│    │──────────────────│
│ task_id                (PK)                      │    │ date        (PK) │
│ worker_id              (FK → dim_workers)        │    │ year, quarter    │
│ item_id                                          │    │ month, week      │
│ insert_time, task_date (FK → dim_dates)          │    │ day_of_week      │
│ task_submission_count, task_acquire_count         │    │ billing_month    │
│ estimated_task_time                              │    │ is_ytd, is_qtd   │
│ is_batch_task                                    │    └──────────────────┘
│ acquired_at, submitted_at                        │
│ time_to_submit_minutes                           │
│ is_submitted, is_resubmitted                     │
│ has_project_registration, has_results_released   │
└───────────────┬──────────────────────────────────┘
                │
                │ task_id (FK)
                │
┌───────────────▼──────────────────────────────────┐
│             fct_task_events                       │
│──────────────────────────────────────────────────│
│ task_id + event_sequence + event_type  (PK)      │
│ event_timestamp                                  │
│ project_id                                       │
│ prev_event_type, prev_event_timestamp            │
│ minutes_since_prev_event, is_first_event         │
└──────────────────────────────────────────────────┘
```

---

### SQL Deliverables

All SQL is in the [`sql/`](sql/) directory:

| File | Layer | Grain | What it does |
|------|-------|-------|-------------|
| [`stg_tasks.sql`](sql/stg_tasks.sql) | Staging | 1 row per task_id | Renames columns. Casts `task_date` for dim_dates join. Adds `is_batch_task` flag. |
| [`stg_task_events.sql`](sql/stg_task_events.sql) | Staging | 1 row per (task_id, event_sequence, event_type) | Renames columns. Extracts `project_id`. **Event IDs are truncated in source.** |
| [`stg_workers.sql`](sql/stg_workers.sql) | Staging | 1 row per (worker_id, employment_cycle) | Renames columns. Adds `is_terminated` flag from `fire_reason`. |
| [`dim_workers.sql`](sql/dim_workers.sql) | **Dimension** | 1 row per worker_id | Collapses employment cycles; latest cycle = current state. |
| [`fct_tasks.sql`](sql/fct_tasks.sql) | **Fact** | 1 row per task_id | Joins task metadata with event-derived timestamps and metrics. |
| [`fct_task_events.sql`](sql/fct_task_events.sql) | Fact (optional) | 1 row per event | Cleaned event stream with lag calculations. |

---

## Part 2 — Metrics Definition

### Metric 1: Total Tasks Submitted (by day/week)

Count of distinct tasks with a SUBMITTED event, grouped by time period.

```sql
SELECT
    d.year_month, d.week,
    COUNT(DISTINCT f.task_id) AS total_tasks_submitted
FROM fct_tasks f
JOIN dim_dates d ON f.task_date = d.date
WHERE f.is_submitted = TRUE
GROUP BY d.year_month, d.week
```

**Caveats:** Resubmissions counted once per `task_id`. Batch tasks (`is_batch_task = TRUE`) should be filtered or reported separately.

---

### Metric 2: Average Time-to-Submit

Average minutes from ACQUIRED to final SUBMITTED event.

```sql
SELECT
    AVG(time_to_submit_minutes) AS avg_minutes,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY time_to_submit_minutes) AS median_minutes
FROM fct_tasks
WHERE is_submitted = TRUE
  AND time_to_submit_minutes > 0
  AND is_batch_task = FALSE
```

**Caveats:** Event IDs are truncated (Data Quality #1), so `time_to_submit_minutes` is unreliable until the source provides full-precision IDs. Distribution is skewed — always report median alongside mean. Resubmitted tasks inflate duration; recommend segmenting by `is_resubmitted`.

---

### Metric 3: Tasks per Active Worker

Average submitted tasks per worker who completed at least one task.

```sql
WITH worker_output AS (
    SELECT worker_id, COUNT(DISTINCT task_id) AS tasks_completed
    FROM fct_tasks
    WHERE is_submitted = TRUE
    GROUP BY worker_id
)
SELECT
    COUNT(worker_id)                                AS active_workers,
    SUM(tasks_completed)                            AS total_tasks,
    SUM(tasks_completed) * 1.0 / COUNT(worker_id)  AS tasks_per_worker
FROM worker_output
```

**Caveats:** "Active" = submitted ≥1 task, not "currently employed." Workers who acquired but never submitted are excluded — pair with Submission Rate.

---

### Metric 4: Submission Rate

Percentage of tasks submitted out of all tasks created.

```sql
SELECT
    COUNT(CASE WHEN is_submitted THEN 1 END) * 1.0 / COUNT(*) AS submission_rate
FROM fct_tasks
WHERE is_batch_task = FALSE
```

**Caveats:** Tasks near the end of the extract window may not yet be submitted, lowering the rate. Batch tasks have different mechanics and should be analyzed separately.

---

### Metric 5: Workforce Status Distribution

Breakdown by employment status and termination reason.

```sql
SELECT
    worker_status, termination_category, is_rehired_worker,
    COUNT(*) AS worker_count
FROM dim_workers
GROUP BY worker_status, termination_category, is_rehired_worker
ORDER BY worker_count DESC
```

**Pseudo-DAX (Power BI):**
```dax
Active Workers = CALCULATE(COUNTROWS(dim_workers), dim_workers[worker_status] = "active")
Churn Rate = DIVIDE(
    CALCULATE(COUNTROWS(dim_workers), dim_workers[worker_status] = "inactive"),
    COUNTROWS(dim_workers)
)
```

**Caveats:** `FAILED_EXAM` dominates terminations — this is a workforce quality signal, not typical attrition. 32.9% of workers have been rehired after exam failures.

---

## Part 3 — Write-up

### 1. Design Decisions

**`fct_tasks`** (1 row per task) is the primary analytical table. It pivots the event stream so analysts get `acquired_at`, `submitted_at`, and `time_to_submit_minutes` without writing event-level self-joins. `fct_task_events` is kept as an optional table for deeper lifecycle analysis.

**`dim_workers`** (1 row per worker) collapses multiple employment cycles into current state. Operational reporting asks "how many workers are active?" and "who is doing the work?" — both answered at the worker grain. Full cycle history remains in `stg_workers`.

**`ix0`** was renamed to `event_sequence` (events) and `employment_cycle` (workers) to prevent confusion.

| Decision | Tradeoff | Why |
|----------|----------|-----|
| No `fct_worker_daily` snapshot | Can't do historical headcount trending | Worker timestamps span years; snapshot table would be large. Add later if needed. |
| Event-derived columns flagged as unreliable | Event joins produce wrong results today | Event IDs truncated to 2 values — flagged rather than silently producing wrong data. |
| `CAST(insert_time AS DATE)` → `task_date` | Assumes insert_time = task creation date | Straightforward; enables dim_dates joins. |
| No dedup in staging | Future duplicates pass through | Add uniqueness tests (`dbt test`) to catch automatically. |

---

### 2. Data Quality & Validation

| # | Issue | Severity | Evidence |
|---|-------|----------|----------|
| 1 | **Event IDs truncated (Excel precision loss).** Only 2 distinct values instead of 1,000. Breaks event-to-task joins. | **High** | Cross-table join yields 0 matches. |
| 2 | **Event timestamps lack date component.** Time-only (`HH:MM:SS`) unlike other tables. | **Medium** | dtype is string; range `00:00:00` to `00:59:57`. |
| 3 | **`ix0` reused with different semantics** across tables. | **Medium** | Event sequence in events; employment cycle in workers. |
| 4 | **Batch tasks behave differently.** 39 tasks (3.9%) with `submission_count = 3`. | **Low** | Flagged with `is_batch_task`. |
| 5 | **`reviewed_by_rater_in_feedback_portal`** nearly 100% NULL. | **Low** | 1 of 1,000 rows populated. |

**Tests I would add (dbt):**

```yaml
models:
  - name: fct_tasks
    columns:
      - name: task_id
        tests: [unique, not_null]
      - name: worker_id
        tests: [not_null, { relationships: { to: ref('dim_workers'), field: worker_id } }]
  - name: dim_workers
    columns:
      - name: worker_id
        tests: [unique, not_null]
      - name: worker_status
        tests: [{ accepted_values: { values: ['active', 'inactive'] } }]
```

| Singular test | Assertion |
|---------------|-----------|
| `assert_hire_before_fire.sql` | `hire_timestamp < fire_timestamp` for terminated cycles |
| `assert_no_orphan_events.sql` | Every event `task_id` exists in `fct_tasks` |
| `assert_no_negative_durations.sql` | `time_to_submit_minutes >= 0` |

---

### 3. Scaling & Operations

**If volume grew 10x:**

| Optimization | Why |
|-------------|-----|
| Partition fact tables by `task_date` | Prune irrelevant data; enable incremental loads |
| Incremental materialization for facts | Process only new rows using `insert_time` watermark |
| Staging as views, not tables | Renaming/cleaning doesn't need storage |
| Cluster `fct_tasks` by `worker_id` | Common GROUP BY / filter pattern |
| `dim_workers` stays full-refresh | Bounded cardinality (~thousands); cheap to rebuild |

**Incremental config example:**

```sql
{{ config(materialized='incremental', unique_key='task_id', incremental_strategy='merge') }}

SELECT * FROM {{ ref('stg_tasks') }}
{% if is_incremental() %}
WHERE insert_time > (SELECT MAX(insert_time) FROM {{ this }})
{% endif %}
```

**dbt project layout:**

```
models/
├── staging/          # materialized='view'
│   ├── stg_tasks.sql, stg_task_events.sql, stg_workers.sql
│   └── _staging__sources.yml
├── marts/core/       # dim_workers='table', facts='incremental'
│   ├── dim_workers.sql, fct_tasks.sql, fct_task_events.sql
│   └── _core__models.yml
└── marts/reporting/
    └── rpt_daily_task_summary.sql
tests/
├── assert_hire_before_fire.sql
├── assert_no_orphan_events.sql
└── assert_no_negative_durations.sql
```

**Monitoring:**

| Check | What it catches |
|-------|----------------|
| Source freshness | Pipeline inputs stop arriving |
| Row count anomaly (±30% vs 7-day avg) | Drops or duplications |
| Null rate on event-derived columns | Broken event joins |
| New values in `event_type` | Unhandled event types |
| Schema drift | Silent pipeline breakage |

---

### 4. Collaboration Scenario

> *An ops stakeholder says your dashboard doesn't match their spreadsheet.*

**1. Get specific.** Which metric, what date range, their number vs ours.

**2. Check definitions.** Root cause ~80% of the time. Common mismatches:
- They count by `insert_time` (creation); dashboard counts by `submitted_at`
- They include batch tasks; dashboard filters them out
- They count resubmissions separately; dashboard deduplicates by `task_id`

**3. Trace lineage.** Walk through raw → staging → fact → dashboard measure. Find the divergence point.

**4. Check scope.** Different extract, timezone, date window, or freshness.

**5. Document and resolve.** Example: *"Difference of 55 tasks is because we exclude batch tasks. Including them matches your number. We've added this to the metric definition."*

**6. Prevent recurrence.** Add the agreed definition to dbt docs / Power BI measure descriptions. If it was a data bug, add a test.

**Principle:** Collaborative investigation, not a debate. One source of truth that both sides trust.

---

## Approach

The data analysis in this assessment was conducted programmatically using a Python script ([`analyze_data.py`](analyze_data.py)) rather than manual inspection. The script reads the `bie_assessment_data.xlsx` workbook directly and performs:

- **Schema profiling:** Column names, data types, null counts for each sheet
- **Uniqueness and grain validation:** Checks whether `id` is unique, validates composite keys like `(id, ix0, type)`
- **Distribution analysis:** Value counts for categorical columns (`fire_reason`, `event_type`, `task_submission_count`), descriptive stats for numerics (`estimated_task_time`)
- **Cross-table join validation:** Compares ID sets across tables to detect orphaned records and join issues — this is how the event ID truncation (Data Quality #1) was discovered
- **Data type verification:** Confirms which timestamps are full datetimes vs time-only strings

**Usage:**
```bash
python3 analyze_data.py                          # defaults to bie_assessment_data.xlsx
python3 analyze_data.py /path/to/file.xlsx       # explicit path
```

This approach ensures all observations in the writeup are reproducible and grounded in the actual data rather than visual inspection of samples.
