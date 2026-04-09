"""
LESSON 09: EXTERNAL SQL FILES - SEPARATING SQL FROM PYTHON

This DAG demonstrates:
  1. Loading SQL from external .sql files
  2. Building file paths relative to the DAG file
  3. Executing complex queries from Airflow

KEY CONCEPTS:
  - Benefits of external SQL files:
    - Separation of concerns: SQL team manages SQL, engineers manage DAG logic
    - Reusability: Same SQL can be used in multiple tools
    - Version control: SQL changes are tracked separately
    - Readability: Complex queries are readable in dedicated SQL files
  - Getting the DAG folder: os.path.dirname(os.path.abspath(__file__))
    This gets the directory containing the current Python file
  - Path joining: os.path.join(dag_folder, "09_query.sql")
    Safely combines paths (works on Windows and Unix)

WHAT IT DOES:
  1. run_sql_file(): Reads 09_query.sql and executes it
  2. show_results(): Queries the results and displays them nicely

THE SQL FILE (09_query.sql):
  1. Creates a department_summary table
  2. Inserts aggregated statistics (count, avg, max, min salary by department)
  3. Calculates salary statistics per department

REAL-WORLD PATTERN:
  Data teams write complex SQL queries
  Engineers integrate them into Airflow pipelines
  Both benefit from clean separation!

KEY TAKEAWAY:
  External SQL files make pipelines more maintainable and team-friendly.
  Data analysts can write SQL without touching Python!
"""

from airflow.sdk import DAG, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import os

with DAG(
    dag_id="09_external_sql",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["learning"],
) as dag:

    @task
    def run_sql_file():
        """Load and execute SQL from an external .sql file"""
        # Get the directory containing this DAG file
        dag_folder = os.path.dirname(os.path.abspath(__file__))
        # Build the path to the SQL file
        sql_file = os.path.join(dag_folder, "09_query.sql")

        # Read the SQL content
        with open(sql_file, "r") as f:
            sql = f.read()

        print(f"Executing SQL from: {sql_file}")
        print(f"SQL content:\n{sql}")

        # Execute against Postgres
        hook = PostgresHook(postgres_conn_id="postgres_learning")
        hook.run(sql)
        print("SQL file executed successfully!")

    @task
    def show_results():
        """Query and display the results"""
        hook = PostgresHook(postgres_conn_id="postgres_learning")
        records = hook.get_records("SELECT * FROM department_summary")
        print("===== DEPARTMENT SUMMARY =====")
        for row in records:
            print(
                f"  {row[0]:<20} "
                f"employees: {row[1]}  "
                f"avg: ${row[2]:<10} "
                f"max: ${row[3]}  "
                f"min: ${row[4]}"
            )
        print("==============================")

    # Run SQL first, then display results
    run_sql_file() >> show_results()