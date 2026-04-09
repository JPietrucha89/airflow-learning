"""
LESSON 03: XCOMS - CROSS-COMMUNICATION BETWEEN TASKS

This DAG demonstrates:
  1. How tasks share data via return values (Implicit XComs)
  2. Multiple independent tasks running in parallel
  3. Combining outputs from multiple tasks downstream

KEY CONCEPTS:
  - XCom: "Cross-communication". Airflow's way for tasks to share data.
  - Implicit XComs: When a @task returns a value, it automatically becomes
    an XCom that downstream tasks can receive as input.
  - Parallel execution: Tasks with no dependencies run at the same time!
    fetch_user() and fetch_product() run in parallel (faster than sequential)
  - Type hints: Use type hints (dict, str, etc) so Airflow understands data flow

WHAT IT DOES:
  1. fetch_user(): Gets user data (runs in parallel)
  2. fetch_product(): Gets product data (runs in parallel)
  3. generate_report(): Receives both outputs, creates a report
  4. send_report(): Sends the final report

EXECUTION FLOW:
  ┌──────────────┐         ┌────────────────┐
  │ fetch_user   │         │ fetch_product  │  (both run at the same time!)
  └──────┬───────┘         └────────┬───────┘
         │                          │
         └──────────────┬───────────┘
                        ▼
              ┌──────────────────────┐
              │  generate_report     │  (waits for both inputs)
              └──────────┬───────────┘
                         ▼
              ┌──────────────────────┐
              │  send_report         │
              └──────────────────────┘

KEY TAKEAWAY:
  Airflow automatically handles task dependencies based on data flow.
  If a task receives output from another task, they're automatically linked.
  Tasks with no data dependencies can run in parallel → faster pipelines!
"""

from airflow.sdk import DAG, task
from datetime import datetime

with DAG(
    dag_id="03_xcoms",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["learning"],
) as dag:

    @task
    def fetch_user():
        """Get user data from a source (database, API, etc.)"""
        user = {"id": 1, "name": "Alice", "score": 95}
        print(f"Fetched user: {user}")
        return user  # This becomes an XCom

    @task
    def fetch_product():
        """Get product data from a source (database, API, etc.)"""
        product = {"id": 42, "name": "Laptop", "price": 999}
        print(f"Fetched product: {product}")
        return product  # This becomes an XCom

    @task
    def generate_report(user: dict, product: dict):
        """Combine two XComs into a single report"""
        report = (
            f"User {user['name']} (score: {user['score']}) "
            f"is interested in {product['name']} "
            f"priced at ${product['price']}"
        )
        print(report)
        return report  # Another XCom for the next task

    @task
    def send_report(report: str):
        """Send the final report"""
        print(f"Sending report: {report}")

    # Execute the pipeline
    user = fetch_user()
    product = fetch_product()
    # Both user and product are XComs that can be passed to downstream tasks
    report = generate_report(user, product)
    send_report(report)