"""
BIE Assessment — Data Exploration Script

Reads the bie_assessment_data.xlsx workbook (4 sheets: task_base, task_event_raw,
worker_base, dim_dates) and prints summary statistics, data quality checks, and
key findings used to inform the data model.

Usage:
    python analyze_data.py                           # looks for bie_assessment_data.xlsx in current dir
    python analyze_data.py /path/to/file.xlsx        # explicit path
"""

import sys
import pandas as pd

# ── Configuration ──────────────────────────────────────────────────────────
DEFAULT_FILE = "bie_assessment_data.xlsx"
SHEET_NAMES = {
    "tasks": "tasks",
    "events": "task_events",
    "workers": "workers",
    "dates": "dates",
}

DIVIDER = "=" * 70


def load_data(filepath: str) -> dict[str, pd.DataFrame]:
    """Load all four sheets from the assessment workbook."""
    print(f"Reading: {filepath}\n")
    frames = {}
    for key, sheet in SHEET_NAMES.items():
        try:
            frames[key] = pd.read_excel(filepath, sheet_name=sheet)
            print(f"  ✓ {sheet:25s} → {len(frames[key]):>6,} rows × {len(frames[key].columns)} cols")
        except ValueError:
            print(f"  ✗ Sheet '{sheet}' not found — skipping")
    print()
    return frames


# ── Table 1: task_base ─────────────────────────────────────────────────────
def analyze_tasks(df: pd.DataFrame) -> None:
    print(DIVIDER)
    print("TABLE 1: task_base")
    print(DIVIDER)

    print(f"\nShape: {df.shape[0]:,} rows × {df.shape[1]} columns")
    print(f"Columns: {list(df.columns)}")

    # Uniqueness
    print("\n--- Uniqueness ---")
    print(f"Total rows:        {len(df):,}")
    print(f"Unique id:         {df['id'].nunique():,}")
    print(f"Unique user_id:    {df['user_id'].nunique():,}")
    print(f"Unique item_id:    {df['item_id'].nunique():,}")

    is_id_unique = df['id'].nunique() == len(df)
    print(f"id is unique key:  {is_id_unique}")

    # Workers per task / tasks per worker
    tasks_per_worker = df.groupby('user_id')['id'].nunique()
    print(f"\nTasks per worker:  min={tasks_per_worker.min()}, "
          f"median={tasks_per_worker.median():.0f}, max={tasks_per_worker.max()}")

    # insert_time
    print("\n--- insert_time ---")
    print(f"Sample values: {df['insert_time'].head(5).tolist()}")
    print(f"Null count:    {df['insert_time'].isna().sum()}")

    # task_submission_count
    print("\n--- task_submission_count ---")
    print(df['task_submission_count'].value_counts().sort_index().to_string())

    # Batch tasks
    batch = df[df['task_submission_count'] > 1]
    print(f"\nBatch tasks (submission_count > 1): {len(batch):,}  "
          f"({len(batch)/len(df)*100:.1f}%)")
    if len(batch) > 0:
        print(f"  est_task_time values: {batch['estimated_task_time'].unique()[:5]}")

    # estimated_task_time
    non_batch = df[df['task_submission_count'] == 1]
    print("\n--- estimated_task_time (non-batch only) ---")
    print(non_batch['estimated_task_time'].describe().to_string())

    # reviewed_by_rater
    col = 'reviewed_by_rater_in_feedback_portal'
    if col in df.columns:
        print(f"\n--- {col} ---")
        print(f"Non-null: {df[col].notna().sum()}")
        print(f"Null:     {df[col].isna().sum()}")

    # Nulls summary
    print("\n--- Null counts ---")
    print(df.isna().sum().to_string())
    print()


# ── Table 2: task_event_raw ────────────────────────────────────────────────
def analyze_events(df: pd.DataFrame) -> None:
    print(DIVIDER)
    print("TABLE 2: task_event_raw")
    print(DIVIDER)

    print(f"\nShape: {df.shape[0]:,} rows × {df.shape[1]} columns")
    print(f"Columns: {list(df.columns)}")

    # Event types
    print("\n--- Event type distribution ---")
    print(df['type'].value_counts().to_string())

    # ix0 analysis
    print("\n--- ix0 (event sequence) ---")
    print(df['ix0'].value_counts().sort_index().to_string())

    # Verify ix0=0 is always ACQUIRED
    ix0_zero = df[df['ix0'] == 0]
    acquired_at_zero = (ix0_zero['type'] == 'ACQUIRED').all()
    print(f"\nix0=0 is always ACQUIRED: {acquired_at_zero}")

    # ID truncation check (critical data quality issue)
    unique_ids = df['id'].nunique()
    print(f"\n--- ID TRUNCATION CHECK ---")
    print(f"Unique id values: {unique_ids}")
    print(f"Unique id list:   {sorted(df['id'].unique())}")
    if unique_ids < len(df) / 10:
        print("⚠ WARNING: IDs appear truncated (likely Excel precision loss).")
        print("  This breaks the join to task_base. All event-level analysis below")
        print("  is grouped by truncated ID, not by individual task.")

    # Events per truncated ID (not per true task)
    events_per_id = df.groupby('id').size()
    print(f"\nEvents per (truncated) id: {events_per_id.to_dict()}")

    # Timestamp format
    print(f"\n--- timestamp ---")
    print(f"dtype: {df['timestamp'].dtype}")
    print(f"sample: {df['timestamp'].head(5).tolist()}")

    # description field
    print(f"\n--- description ---")
    print(f"Non-null: {df['description'].notna().sum()}")
    proj_reg = df[df['type'] == 'PROJECT_REGISTRATION']
    if len(proj_reg) > 0:
        print(f"PROJECT_REGISTRATION descriptions (sample): {proj_reg['description'].head(3).tolist()}")

    # Nulls
    print("\n--- Null counts ---")
    print(df.isna().sum().to_string())
    print()


# ── Table 3: worker_base ──────────────────────────────────────────────────
def analyze_workers(df: pd.DataFrame) -> None:
    print(DIVIDER)
    print("TABLE 3: worker_base")
    print(DIVIDER)

    print(f"\nShape: {df.shape[0]:,} rows × {df.shape[1]} columns")
    print(f"Columns: {list(df.columns)}")

    # Worker type
    print(f"\n--- type ---")
    print(df['type'].value_counts().to_string())

    # ix0 = employment cycle
    print(f"\n--- ix0 (employment cycle) ---")
    print(df['ix0'].value_counts().sort_index().to_string())

    # Unique workers vs rows
    print(f"\n--- Uniqueness ---")
    print(f"Total rows:       {len(df):,}")
    print(f"Unique worker_id: {df['id'].nunique():,}")
    print(f"(id, ix0) unique: {df.groupby(['id', 'ix0']).ngroups == len(df)}")

    # Cycles per worker
    cycles = df.groupby('id')['ix0'].nunique()
    print(f"\nCycles per worker: min={cycles.min()}, "
          f"median={cycles.median():.0f}, max={cycles.max()}")
    print("\nCycle count distribution:")
    print(cycles.value_counts().sort_index().to_string())

    rehired = (cycles > 1).sum()
    print(f"\nRehired workers (>1 cycle): {rehired}  "
          f"({rehired/len(cycles)*100:.1f}%)")

    # fire_reason
    print("\n--- fire_reason ---")
    print(df['fire_reason'].value_counts(dropna=False).to_string())

    # fire_timestamp / fire_reason alignment
    print("\n--- fire_timestamp ↔ fire_reason alignment ---")
    print(f"fire_timestamp dtype: {df['fire_timestamp'].dtype}")
    both_null = ((df['fire_timestamp'].isna()) & (df['fire_reason'].isna())).sum()
    both_present = ((df['fire_timestamp'].notna()) & (df['fire_reason'].notna())).sum()
    mismatch = len(df) - both_null - both_present
    print(f"Both null (active):      {both_null}")
    print(f"Both present (fired):    {both_present}")
    print(f"Mismatched:              {mismatch}")
    if df['fire_timestamp'].notna().any():
        ft = df['fire_timestamp'].dropna()
        print(f"fire_timestamp range:    {ft.min()} → {ft.max()}")

    # Active workers (latest cycle, no fire)
    latest_cycle = df.loc[df.groupby('id')['ix0'].idxmax()]
    active = latest_cycle[latest_cycle['fire_reason'].isna()]
    print(f"\n--- Active workers (latest cycle, no fire_reason) ---")
    print(f"Count: {len(active)}  ({len(active)/latest_cycle.shape[0]*100:.1f}%)")

    # Nulls
    print("\n--- Null counts ---")
    print(df.isna().sum().to_string())
    print()


# ── Table 4: dim_dates ────────────────────────────────────────────────────
def analyze_dates(df: pd.DataFrame) -> None:
    print(DIVIDER)
    print("TABLE 4: dim_dates")
    print(DIVIDER)

    print(f"\nShape: {df.shape[0]:,} rows × {df.shape[1]} columns")
    print(f"Columns: {list(df.columns)}")

    if 'date' in df.columns:
        print(f"\nDate range: {df['date'].min()} → {df['date'].max()}")
        print(f"Unique dates: {df['date'].nunique():,}")

    if 'age_days' in df.columns:
        zero_age = df[df['age_days'] == 0]
        if len(zero_age) > 0:
            print(f"age_days = 0 on: {zero_age['date'].values[0]}")

    print("\n--- Null counts ---")
    print(df.isna().sum().to_string())
    print()


# ── Cross-table analysis ──────────────────────────────────────────────────
def cross_table_analysis(frames: dict[str, pd.DataFrame]) -> None:
    print(DIVIDER)
    print("CROSS-TABLE ANALYSIS")
    print(DIVIDER)

    tasks = frames.get("tasks")
    events = frames.get("events")
    workers = frames.get("workers")

    if tasks is not None and events is not None:
        # Tasks with events vs without
        task_ids_with_events = set(events['id'].unique())
        task_ids_in_base = set(tasks['id'].unique())

        tasks_no_events = task_ids_in_base - task_ids_with_events
        events_no_tasks = task_ids_with_events - task_ids_in_base

        print(f"\n--- task_base ↔ task_event_raw ---")
        print(f"Tasks in task_base:         {len(task_ids_in_base):,}")
        print(f"Tasks in task_event_raw:    {len(task_ids_with_events):,}")
        print(f"Tasks with no events:       {len(tasks_no_events):,}")
        print(f"Events for unknown tasks:   {len(events_no_tasks):,}")

        # Submission check
        submitted_tasks = events[events['type'] == 'SUBMITTED']['id'].nunique()
        print(f"Tasks with SUBMITTED event: {submitted_tasks:,}  "
              f"({submitted_tasks/len(task_ids_in_base)*100:.1f}%)")

    if tasks is not None and workers is not None:
        # Workers in tasks vs worker_base
        worker_ids_in_tasks = set(tasks['user_id'].unique())
        worker_ids_in_base = set(workers['id'].unique())

        print(f"\n--- task_base ↔ worker_base ---")
        print(f"Workers in task_base:       {len(worker_ids_in_tasks):,}")
        print(f"Workers in worker_base:     {len(worker_ids_in_base):,}")
        print(f"In tasks but not workers:   {len(worker_ids_in_tasks - worker_ids_in_base):,}")
        print(f"In workers but not tasks:   {len(worker_ids_in_base - worker_ids_in_tasks):,}")

    print()


# ── Main ──────────────────────────────────────────────────────────────────
def main():
    filepath = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_FILE

    frames = load_data(filepath)

    if not frames:
        print("No sheets loaded. Check the file path and sheet names.")
        sys.exit(1)

    if "tasks" in frames:
        analyze_tasks(frames["tasks"])

    if "events" in frames:
        analyze_events(frames["events"])

    if "workers" in frames:
        analyze_workers(frames["workers"])

    if "dates" in frames:
        analyze_dates(frames["dates"])

    if len(frames) >= 2:
        cross_table_analysis(frames)

    print(DIVIDER)
    print("Done. See assessment_writeup.md for model design and metric definitions.")
    print(DIVIDER)


if __name__ == "__main__":
    main()
