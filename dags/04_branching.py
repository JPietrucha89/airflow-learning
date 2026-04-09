"""
LESSON 04: BRANCHING - CONDITIONAL TASK EXECUTION

This DAG demonstrates:
  1. BranchPythonOperator: Conditionally execute different tasks
  2. EmptyOperator: Structural placeholders for organizing DAGs
  3. trigger_rule: Control when tasks execute (e.g., "one_success")
  4. Task joining: Merging branches back together

KEY CONCEPTS:
  - BranchPythonOperator: Returns a task_id (string) that determines
    which downstream task(s) to execute. All others are skipped!
  - EmptyOperator: A no-op task. Does nothing but serves as:
    - A start/end marker
    - A join point for branches
  - trigger_rule: Determines when a task runs:
    - "one_success" = Run if ANY upstream task succeeded (used for joins)
    - "all_success" = Run only if ALL upstream tasks succeeded (default)
    - "all_done" = Run regardless of upstream status

EXECUTION FLOW:
  ┌──────────┐
  │  start   │
  └────┬─────┘
       ▼
  ┌────────────────┐
  │    branch      │  (decides: high or low score?)
  └────┬─────────┬─┘
       ▼         ▼
  ┌──────────┐ ┌──────────┐
  │high_score│ │low_score │  (only one executes!)
  └────┬─────┘ └────┬─────┘
       │            │
       └────┬───────┘
            ▼
      ┌──────────┐
      │   end    │  (trigger_rule="one_success" allows both paths to reach here)
      └──────────┘

KEY TAKEAWAY:
  Branching lets pipelines make decisions at runtime. Perfect for:
  - Conditional processing (if score > 50, send premium email)
  - Error handling (if failed, go to cleanup task)
  - A/B testing different processing paths
"""

from airflow.sdk import DAG, task
from airflow.providers.standard.operators.python import BranchPythonOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from datetime import datetime
import random

with DAG(
    dag_id="04_branching",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["learning"],
) as dag:

    # Start marker (empty task, just for structure)
    start = EmptyOperator(task_id="start")

    # Function to decide which branch to take
    def decide_branch():
        """Randomly decide: high score or low score?
        Returns the task_id of the task to execute next"""
        score = random.randint(0, 100)
        print(f"Score is: {score}")
        if score >= 50:
            return "high_score"  # Execute the high_score task
        else:
            return "low_score"   # Execute the low_score task
    
    # BranchPythonOperator: Decides which path to take
    branch = BranchPythonOperator(
        task_id="branch",
        python_callable=decide_branch,
    )
    # NOTE: Unlike @task, this returns a task_id string, not data!
    # Airflow follows that path and skips the others.

    @task
    def high_score():
        """Execute if score >= 50"""
        print("Score is high — sending bonus email!")

    @task
    def low_score():
        """Execute if score < 50"""
        print("Score is low — sending encouragement email!")

    # End marker: Uses trigger_rule to accept both branches
    end = EmptyOperator(
        task_id="end",
        trigger_rule="one_success"  # Run if ANY upstream task succeeded
    )

    # Build the DAG structure:
    # start >> branch >> [high_score() or low_score()] >> end
    start >> branch >> [high_score(), low_score()] >> end
    # high_score() >> end
    # low_score() >> end