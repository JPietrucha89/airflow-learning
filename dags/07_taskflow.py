"""
LESSON 07: TASKFLOW API - PROFESSIONAL DAG PATTERNS

This DAG demonstrates:
  1. Type hints with @task for better data flow documentation
  2. Complete ETL pipeline with transformations
  3. Composable functions: Write clean, testable business logic
  4. Return types guide Airflow's internal data handling

KEY CONCEPTS:
  - @task with type hints: Modern Python-first Airflow development
    @task
    def extract() -> list[dict]:  # Returns a list of dicts
        ...
  - Type hints help Airflow understand:
    - What data flows between tasks
    - Whether the data fits in memory
    - How to serialize/deserialize data
  - Clean separation: Business logic (functions) + orchestration (DAG)
    You can test functions independently without Airflow!

WHAT IT DOES:
  1. extract(): Query employees from database (returns list of dicts)
  2. transform(): Add calculated fields (salary_after_raise, senior status)
  3. summarise(): Aggregate data (count, seniors, average)
  4. report(): Pretty-print the final summary

This is a complete data pipeline:
  Raw data → Enrich → Aggregate → Report

KEY TAKEAWAY:
  The Taskflow API combines:
  - Readability: Functions look like normal Python
  - Testability: Each function is a pure Python function
  - Scalability: Can be distributed across workers
  - Type safety: Return types document the data contract
"""

from airflow.sdk import DAG, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime

with DAG(
    dag_id="07_taskflow",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["learning"],
) as dag:

    @task
    def extract() -> list[dict]:
        """Extract employee data from database"""
        hook = PostgresHook(postgres_conn_id="postgres_learning")
        records = hook.get_records("SELECT name, department, salary FROM employees")
        data = [
            {"name": r[0], "department": r[1], "salary": r[2]}
            for r in records
        ]
        print(f"Extracted {len(data)} records")
        return data  # Return type: list[dict]

    @task
    def transform(employees: list[dict]) -> list[dict]:
        """Transform: Add calculated fields to each employee"""
        for emp in employees:
            # Add salary after 10% raise
            emp["salary_after_raise"] = round(emp["salary"] * 1.1)
            # Mark as senior if salary > $80k
            emp["senior"] = emp["salary"] > 80000
        print(f"Transformed {len(employees)} records")
        return employees  # Return type: list[dict]

    @task
    def summarise(employees: list[dict]) -> dict:
        """Summarise: Aggregate data into key metrics"""
        total = len(employees)
        seniors = sum(1 for e in employees if e["senior"])
        avg_raise = round(
            sum(e["salary_after_raise"] for e in employees) / total, 2
        )
        summary = {
            "total_employees": total,
            "seniors": seniors,
            "avg_salary_after_raise": avg_raise,
        }
        print(f"Summary: {summary}")
        return summary  # Return type: dict

    @task
    def report(summary: dict):
        """Report: Display final results in a nice format"""
        print("===== REPORT =====")
        print(f"Total employees   : {summary['total_employees']}")
        print(f"Senior employees  : {summary['seniors']}")
        print(f"Avg salary (raise): ${summary['avg_salary_after_raise']}")
        print("==================")

    # Chain the tasks: extract → transform → summarise → report
    report(summarise(transform(extract())))