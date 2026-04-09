"""
LESSON 13: CATCHUP - HANDLING HISTORICAL AND MISSED RUNS

This DAG demonstrates:
  1. Understanding catchup=False: Only run for current/future dates
  2. Understanding catchup=True: Backfill all missed historical dates
  3. Comparing behavior and use cases for each approach
  4. Using logical_date to understand when/why tasks run

KEY CONCEPTS:
  - start_date: The earliest date Airflow considers for scheduling
  - schedule: How often the DAG runs (e.g., @daily = every day)
  - catchup: Whether to run historical backfill for past unrun dates
  - logical_date: The date/time this particular task run represents
    (not the time it actually runs, but the data period it processes)
  - Scenario: DAG created on 2026-04-09 with start_date=2026-01-01 and @daily schedule

WHAT IT DOES:
  1. DAG 1 (catchup=False):
     - Scheduler checks: "Should I run for 2026-01-01 through 2026-04-08?"
     - Answer: No, catchup=False skips backfill
     - Only runs for 2026-04-09 onwards (current/future dates)
     - Useful when you only care about recent data

  2. DAG 2 (catchup=True):
     - Scheduler checks: "Should I run for 2026-01-01 through 2026-04-08?"
     - Answer: Yes! Creates a run for every missed date
     - Creates ~99 task runs (one per day from Jan 1 to Apr 8)
     - Then runs normally going forward
     - Useful when you need complete historical coverage

REAL-WORLD SCENARIOS:
  - catchup=False: Monitoring dashboards, real-time alerts, streaming jobs
    No point backfilling old dashboards or alerts
  - catchup=True: ETL pipelines, data warehouses, analytics
    You need complete historical data coverage for accurate reports

THE logical_date PARAMETER:
  - This is NOT the current time
  - It represents the "data interval" this task is responsible for
  - For @daily schedule: logical_date is the START of that day
  - Example: Task with logical_date=2026-01-01 processes data for Jan 1
  - Passed automatically via Airflow's templating system

GOTCHAS:
  - Enabling catchup on a large backlog can create MANY parallel tasks
    (could overwhelm your database and scheduler!)
  - Use catchup=True carefully with scheduling to avoid resource issues
  - logical_date is often called data_interval_start in newer Airflow versions

KEY TAKEAWAY:
  catchup: False avoids backfill, runs only going forward.
  catchup=True backfills all historical dates, one run per schedule period.
  Choose based on whether you need historical coverage or not!
"""

from airflow.sdk import DAG, task
from datetime import datetime

with DAG(
    dag_id="13_catchup_false",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["learning"],
) as dag:
    """
    DAG 1: catchup=False
    
    Only runs for current and future dates.
    Skips all historical dates between start_date and today.
    """

    @task
    def process(logical_date=None):
        """
        Process data for a specific logical_date.
        
        Args:
            logical_date: The date this task run represents
                         (auto-passed by Airflow, e.g., 2026-04-09)
        """
        print(f"✅ Processing data for: {logical_date}")

    process()


with DAG(
    dag_id="13_catchup_true",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=True,
    tags=["learning"],
) as dag2:
    """
    DAG 2: catchup=True
    
    Backfills all historical dates from start_date to today,
    then continues running forward.
    Expects ~99 runs: one for each day from Jan 1 to Apr 8.
    """

    @task
    def process_with_catchup(logical_date=None):
        """
        Process data for a specific logical_date.
        
        This task will run multiple times during backfill:
        once for each date from start_date to today.
        
        Args:
            logical_date: The date this task run represents
                         (e.g., 2026-01-01, then 2026-01-02, etc.)
        """
        print(f"✅ Processing data for: {logical_date}")

    process_with_catchup()