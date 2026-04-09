"""
LESSON 10: DBT INTEGRATION (BASH METHOD) - ORCHESTRATING DBT RUNS

This DAG demonstrates:
  1. BashOperator: Execute shell commands from Airflow
  2. Running dbt commands in sequence
  3. Organizing dbt runs: debug → test → run → test

KEY CONCEPTS:
  - BashOperator: Task that executes a bash command
    bash_command="cd /path && python script.py"
  - dbt commands used:
    - dbt debug: Verify dbt setup and database connection
    - dbt test: Run data quality tests on sources/models
    - dbt run: Execute model transformations
  - Order matters: Always debug first, then test, then run
  - Selectors: --select staging (run only "staging" tagged models)

DBT WORKFLOW:
  1. Debug: Is dbt installed? Can we connect to the database?
  2. Test sources: Are input data sources valid?
  3. Run staging: Build staging (raw data cleaning) models
  4. Run marts: Build mart (business logic) models
  5. Test models: Run quality checks on transformations

ADVANTAGES:
  - Simple and straightforward
  - Works with standard dbt CLI
  - Easy to debug (just bash commands)

DISADVANTAGES:
  - Limited visibility of dbt's internal task graph
  - Doesn't capture dbt's model dependencies
  - All dbt tasks appear as a single bash_command

WHEN TO USE:
  - Quick dbt integration with Airflow
  - Simple linear dbt pipelines
  - Teams already familiar with bash/CLI

KEY TAKEAWAY:
  BashOperator is flexible: any shell command works!
  But for dbt, consider using Cosmos (Lesson 11) for better integration.
"""

# airflow dag
from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator
from datetime import datetime

# Paths to dbt resources
DBT_PROJECT_DIR = "/opt/airflow/dbt_project"
DBT_PROFILES_DIR = "/opt/airflow/dbt_profiles"

with DAG(
    dag_id="10_dbt_bash",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["learning", "dbt"],
) as dag:

    # Task 1: Verify dbt setup and database connection
    dbt_debug = BashOperator(
        task_id="dbt_debug",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt debug --profiles-dir {DBT_PROFILES_DIR}",
    )

    # Task 2: Test the input data sources - just run all tests defined in schema.yml / sources.yml files
    dbt_test_sources = BashOperator(
        task_id="dbt_test_sources",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt test --select source:* --profiles-dir {DBT_PROFILES_DIR}",
    )

    # Task 3: Run staging models (raw data cleanup)
    dbt_run_staging = BashOperator(
        task_id="dbt_run_staging",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt run --select staging --profiles-dir {DBT_PROFILES_DIR}",
    )

    # Task 4: Run mart models (business logic)
    dbt_run_marts = BashOperator(
        task_id="dbt_run_marts",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt run --select marts --profiles-dir {DBT_PROFILES_DIR}",
    )

    # Task 5: Test all models (data quality checks)
    dbt_test_models = BashOperator(
        task_id="dbt_test_models",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt test --select staging marts --profiles-dir {DBT_PROFILES_DIR}",
    )

    # Define the linear workflow
    dbt_debug >> dbt_test_sources >> dbt_run_staging >> dbt_run_marts >> dbt_test_models