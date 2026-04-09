"""
LESSON 06: CONNECTIONS & HOOKS - DATABASE CONNECTIVITY

This DAG demonstrates:
  1. PostgresHook: Airflow's abstraction for talking to Postgres
  2. Connections: Secure credential storage in Airflow
  3. Common Hook methods: run(), insert_rows(), get_records(), get_first()

KEY CONCEPTS:
  - Connection: Stored credentials in Airflow UI (never hardcoded!)
    Admin → Connections → postgres_learning
    Stores: host, user, password, port, database
  - Hook: Python class that uses a Connection to interact with a system
    Examples: PostgresHook, S3Hook, HttpHook, MySqlHook
  - Why hooks? 
    - Centralized credential management
    - Credential rotation without changing DAGs
    - Consistent connection handling across pipelines

HOOK METHODS USED:
  - hook.run(sql)
    → Execute any SQL statement (CREATE, INSERT, UPDATE, DELETE, etc.)
  - hook.insert_rows(table, rows, target_fields)
    → Bulk insert many rows at once (faster than individual inserts)
  - hook.get_records(sql)
    → Execute SELECT and get all rows as tuples
  - hook.get_first(sql)
    → Execute SELECT and get just the first row

WHAT IT DOES:
  1. create_table(): Create employees table
  2. insert_data(): Insert 4 sample employees
  3. query_data(): Select all employees and print them
  4. get_avg_salary(): Calculate average salary

SETUP REQUIRED:
  Connection Name: postgres_learning
  Type: Postgres
  Host: postgres-learning
  User: airflow_user
  Password: airflow_pass
  Database: airflow_db
  Port: 5432

KEY TAKEAWAY:
  Hooks are the bridge between Airflow and external systems.
  Always use Connections + Hooks instead of hardcoding credentials!
"""

from airflow.sdk import DAG, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime

with DAG(
    dag_id="06_connections",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["learning"],
) as dag:

    @task
    def create_table():
        """Create the employees table"""
        hook = PostgresHook(postgres_conn_id="postgres_learning")
        hook.run("""
            CREATE TABLE IF NOT EXISTS employees (
                id SERIAL PRIMARY KEY,
                name VARCHAR(100),
                department VARCHAR(100),
                salary INTEGER
            )
        """)
        print("Table created!")

    @task
    def insert_data():
        """Insert sample employee data"""
        hook = PostgresHook(postgres_conn_id="postgres_learning")
        rows = [
            ("Alice", "Engineering", 95000),
            ("Bob", "Marketing", 72000),
            ("Charlie", "Engineering", 88000),
            ("Diana", "HR", 65000),
        ]
        hook.insert_rows(
            table="employees",
            rows=rows,
            target_fields=["name", "department", "salary"]
        )
        print(f"Inserted {len(rows)} rows!")

    @task
    def query_data():
        """Query all employees"""
        hook = PostgresHook(postgres_conn_id="postgres_learning")
        records = hook.get_records("SELECT * FROM employees")
        for row in records:
            print(f"ID: {row[0]}, Name: {row[1]}, Dept: {row[2]}, Salary: {row[3]}")
        return len(records)

    @task
    def get_avg_salary():
        """Calculate average salary"""
        hook = PostgresHook(postgres_conn_id="postgres_learning")
        result = hook.get_first("SELECT AVG(salary) FROM employees")
        avg = round(result[0], 2)
        print(f"Average salary: ${avg}")
        return avg

    # Build the pipeline
    count = query_data()
    avg = get_avg_salary()
    create_table() >> insert_data() >> [count, avg]