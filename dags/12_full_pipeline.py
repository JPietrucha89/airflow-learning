"""
LESSON 12: FULL PRODUCTION PIPELINE - END-TO-END ELT WORKFLOW

This DAG demonstrates:
  1. Complete data pipeline: Ingest → Transform → Report
  2. Combining Python tasks with dbt via DbtTaskGroup
  3. Daily scheduling (production-ready)
  4. Real-world patterns: Loading data, transforming, reporting

KEY CONCEPTS:
  - DbtTaskGroup: Embed dbt transformations into an existing DAG
    (Alternative to DbtDag when you need custom tasks too)
  - Three-layer architecture:
    1. Ingest: Get fresh data from sources
    2. Transform: dbt for complex SQL transformations
    3. Report: Present results to stakeholders
  - Scheduling: "0 8 * * *" = Every day at 8 AM
  - Data flow documentation: Show how raw data becomes insights

WHAT IT DOES:
  1. ingest_new_employees():
     - Truncates old employee data
     - Inserts fresh sample employees
     - Simulates daily data arrival
  
  2. dbt_transform (DbtTaskGroup):
     - Runs dbt models (staging, marts, etc.)
     - Creates transformed tables in database
     - Applies business logic and quality checks
  
  3. report_results():
     - Queries dbt output tables (marts)
     - Pretty-prints department statistics
     - Shows senior employees with raises
     - Beautiful terminal report

DATA LINEAGE:
  ┌────────────────────────────────────────────────┐
  │  RAW: employees table (fresh data ingested)    │
  └────────────┬─────────────────────────────────────┘
               │
               ▼ (ingest task)
  ┌────────────────────────────────────────────────┐
  │  STAGING: clean & standardize raw data         │
  │  ├─ Remove duplicates                          │
  │  ├─ Fix data types                             │
  │  └─ Add audit columns                          │
  └────────────┬─────────────────────────────────────┘
               │
               ▼ (dbt run staging)
  ┌────────────────────────────────────────────────┐
  │  MARTS: business-ready aggregations            │
  │  ├─ mart_department_stats                      │
  │  └─ mart_senior_employees                      │
  └────────────┬─────────────────────────────────────┘
               │
               ▼ (report task)
  ┌────────────────────────────────────────────────┐
  │  INSIGHTS: Human-readable reports              │
  │  ├─ Department breakdown                       │
  │  └─ Senior employee salaries                   │
  └────────────────────────────────────────────────┘

PRODUCTION READINESS:
  ✅ Scheduled daily (0 8 * * * = 8 AM)
  ✅ Data validation via dbt tests
  ✅ Error handling via default_args (inherited)
  ✅ Clear data lineage: Raw → Staged → Marts → Report
  ✅ Separation of concerns: Ingestion, Transform, Report

REAL-WORLD ANALOGY:
  - Ingest: Your data warehouse receives daily files
  - Transform: dbt cleaning and business logic
  - Report: Dashboard or executive summary

KEY TAKEAWAY:
  This is a production-grade ELT pipeline!
  It shows how Airflow orchestrates real data workflows:
  Get data → Process it → Present results
"""

# airflow dag
from airflow.sdk import DAG, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from pathlib import Path
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, ExecutionConfig, RenderConfig
from cosmos.profiles import PostgresUserPasswordProfileMapping
from datetime import datetime

DBT_PROJECT_DIR = "/opt/airflow/dbt_project"

# Configure dbt connection
profile_config = ProfileConfig(
    profile_name="dbt_project",
    target_name="dev",
    profile_mapping=PostgresUserPasswordProfileMapping(
        conn_id="postgres_learning",
        profile_args={"schema": "public"},
    ),
)

# Configure dbt execution
execution_config = ExecutionConfig(
    dbt_executable_path="/home/airflow/.local/bin/dbt",
    dbt_project_path=Path(DBT_PROJECT_DIR),
)

# Configure model exclusions
render_config=RenderConfig(
    exclude=["path:models/example/*"],
)

with DAG(
    dag_id="12_full_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule="0 8 * * *",   # every day at 8 AM
    catchup=False,
    tags=["learning", "dbt"],
) as dag:

    @task
    def ingest_new_employees():
        """LAYER 1: Ingestion
        
        Simulate fresh daily data arriving.
        In production, this would read from:
        - Cloud storage (S3, GCS, Azure Blob)
        - Data warehouse (Snowflake, BigQuery)
        - APIs (Salesforce, HubSpot)
        - CDC streams (Kafka, Pub/Sub)
        """
        hook = PostgresHook(postgres_conn_id="postgres_learning")
        
        # Simulate fresh daily data
        new_employees = [
            ("Eve", "Engineering", 102000),
            ("Frank", "Marketing", 78000),
            ("Grace", "HR", 69000),
            ("Henry", "Engineering", 91000),
        ]
        
        # Truncate old data, reload with fresh data
        hook.run("TRUNCATE TABLE employees")
        hook.insert_rows(
            table="employees",
            rows=new_employees,
            target_fields=["name", "department", "salary"]
        )
        print(f"Ingested {len(new_employees)} fresh employee records")
        return len(new_employees)

    @task
    def report_results(row_count: int):
        """LAYER 3: Reporting
        
        Query dbt output tables (marts) and present results.
        In production, this would:
        - Send alerts if anomalies detected
        - Update dashboards
        - Send email reports
        - Log metrics to monitoring system
        """
        hook = PostgresHook(postgres_conn_id="postgres_learning")
        
        print(f"\n{'='*50}")
        print("DAILY PIPELINE REPORT")
        print(f"{'='*50}")
        print(f"Ingested rows     : {row_count}")
        
        # Query dbt-created marts
        dept_stats = hook.get_records(
            "SELECT * FROM public_marts.mart_department_stats ORDER BY avg_salary DESC"
        )
        print(f"\nDEPARTMENT BREAKDOWN:")
        for row in dept_stats:
            print(
                f"  {row[0]:<20} "
                f"headcount: {row[1]}  "
                f"avg salary: ${row[2]:<10} "
                f"payroll: ${row[4]}"
            )

        seniors = hook.get_records(
            "SELECT * FROM public_marts.mart_senior_employees ORDER BY salary_usd DESC"
        )
        print(f"\nSENIOR EMPLOYEES ({len(seniors)} total):")
        for row in seniors:
            print(f"  {row[1]:<20} {row[2]:<20} ${row[3]} → ${row[4]} after raise")
        print(f"{'='*50}\n")

    # LAYER 2: Transformation via dbt
    # DbtTaskGroup generates tasks for each dbt model
    # Respects dependencies between models automatically
    dbt_transform = DbtTaskGroup(
        group_id="dbt_transform",
        project_config=ProjectConfig(
            manifest_path=Path(f"{DBT_PROJECT_DIR}/target/manifest.json"),
            project_name="dbt_project",
        ),
        profile_config=profile_config,
        render_config=render_config,
        execution_config=execution_config,
    )

    # Build the three-layer pipeline
    ingested = ingest_new_employees()
    ingested >> dbt_transform >> report_results(ingested)