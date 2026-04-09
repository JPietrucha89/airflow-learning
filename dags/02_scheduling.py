"""
LESSON 02: SCHEDULING - CRON EXPRESSIONS & ETL PATTERN

This DAG demonstrates:
  1. Scheduling with cron expressions
  2. Classic ETL (Extract → Transform → Load) pattern
  3. Task dependencies with data flow

KEY CONCEPTS:
  - schedule: Cron expression defining when DAG runs
    Examples:
      "*/5 * * * *"    = Every 5 minutes
      "0 8 * * *"      = Every day at 8 AM
      "0 0 * * 1"      = Every Monday at midnight
    Format: minute hour day month weekday
  - start_date: When the DAG first becomes active
  - catchup: If False, don't run missed schedules (good for learning)
  
  - ETL Pattern:
    Extract: Get data from source (database, API, file)
    Transform: Process and clean the data
    Load: Store results in destination

WHAT IT DOES:
  1. extract(): Simulates fetching 42 rows from a source
  2. transform(): Doubles the row count (simulates data transformation)
  3. load(): Simulates writing results to a database

HOW IT RUNS:
  - Automatically triggers every 5 minutes (*/5 * * * *)
  - Each run is a "DAG execution" with its own logs
  - Tasks run sequentially: extract → transform → load
  - Data flows: 42 rows → 84 rows → stored

KEY TAKEAWAY:
  This is a real-world workflow: Get data, modify it, save it.
  Many data pipelines follow this exact pattern!
"""

from airflow.sdk import DAG, task
from datetime import datetime

with DAG(
    dag_id="02_scheduling",
    start_date=datetime(2024, 1, 1),
    schedule="*/5 * * * *",   # every 5 minutes (cron expression)
    catchup=False,
    tags=["learning"],
) as dag:

    @task
    def extract():
        """Extract: Fetch raw data from a source"""
        print("Extracting data...")
        return {"rows": 42}  # Simulate fetching 42 rows

    @task
    def transform(data: dict):
        """Transform: Process and modify the data"""
        print(f"Transforming {data['rows']} rows...")
        return data["rows"] * 2  # Simulate: double the data

    @task
    def load(result: int):
        """Load: Store the processed data in destination"""
        print(f"Loading {result} records into the database...")

    # Define the ETL pipeline
    load(transform(extract())) # Again this works like XComs under the hood, but doesn't require manual push/pull of data between tasks. Airflow automatically handles it!