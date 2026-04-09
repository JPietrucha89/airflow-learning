"""
LESSON 08: ERROR HANDLING - RETRIES, CALLBACKS, & TRIGGER RULES

This DAG demonstrates:
  1. Retry logic: Automatically retry failed tasks
  2. Callbacks: Functions called on success/failure
  3. Trigger rules: Control when tasks execute
  4. Handling flaky tasks: Retry with exponential backoff

KEY CONCEPTS:
  - retries: Number of times to retry a failed task
    - Set in default_args (applies to all tasks)
    - Override per-task (individual task setting wins)
  - retry_delay: Wait time between retries (timedelta object)
    - timedelta(seconds=10) = Wait 10 seconds before retrying
    - timedelta(minutes=5) = Wait 5 minutes
  - Callbacks: Functions called when task completes
    - on_failure_callback: Called if task fails
    - on_success_callback: Called if task succeeds
    - Useful for: Alerts, logging, notifications
  - trigger_rule: Determines WHEN a task executes
    - "all_success" = Run only if ALL upstream succeeded (default)
    - "one_success" = Run if ANY upstream succeeded
    - "all_done" = Run regardless of upstream status
    - "one_failed" = Run if ANY upstream failed

WHAT IT DOES:
  1. stable_task(): Always succeeds
  2. flaky_task(): Fails 70% of the time (demonstrates retries)
  3. always_fails(): Deliberately fails (has retries=0)
  4. final_task(): Runs after both stable and flaky succeed
  5. end(): Uses trigger_rule="all_done" to catch failures

REAL-WORLD USES:
  - Retry transient failures (network timeouts, temp database issues)
  - Send alerts when tasks fail
  - Log successes for audit trails
  - Handle partial failures gracefully

KEY TAKEAWAY:
  Production pipelines need resilience!
  Retries + callbacks + trigger rules = robust pipelines
"""

from airflow.sdk import DAG, task
from airflow.providers.standard.operators.empty import EmptyOperator
from datetime import datetime, timedelta
import random

# Callbacks: Functions called on task completion
def on_failure_callback(context):
    """Called when a task fails"""
    task_id = context["task_instance"].task_id
    dag_id = context["task_instance"].dag_id
    logical_date = context["logical_date"]
    exception = context.get("exception")
    print(f"❌ Task failed!")
    print(f"   DAG       : {dag_id}")
    print(f"   Task      : {task_id}")
    print(f"   Execution : {logical_date}")
    print(f"   Exception : {exception}")
    # In production, you'd send a Slack/email alert here

def on_success_callback(context):
    """Called when a task succeeds"""
    task_id = context["task_instance"].task_id
    print(f"✅ Task {task_id} completed successfully!")

with DAG(
    dag_id="08_error_handling",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["learning"],
    # Default args apply to all tasks (unless overridden)
    default_args={
        "retries": 3,                           # Retry up to 3 times
        "retry_delay": timedelta(seconds=10),  # Wait 10 seconds between retries
        "on_failure_callback": on_failure_callback,
        "on_success_callback": on_success_callback,
    },
) as dag:

    @task
    def stable_task():
        """This task always succeeds"""
        print("This task always succeeds")
        return "ok"

    @task(
        retries=5,                       # Override default: retry up to 5 times
        retry_delay=timedelta(seconds=5),  # Shorter delay for this specific task
    )
    def flaky_task():
        """This task fails 70% of the time (demonstrates retry mechanism)"""
        if random.random() < 0.7:
            raise Exception("Random failure! (70% chance)")
        print("Flaky task succeeded!")
        return "flaky_ok"

    @task(
        retries=0,  # Don't retry this one
        on_failure_callback=on_failure_callback,
    )
    def always_fails():
        """This task always fails deliberately"""
        raise ValueError("This task always fails deliberately")

    @task
    def final_task(a: str, b: str):
        """Runs after both upstream tasks succeed"""
        print(f"Got: {a} and {b}")
        print("Pipeline complete!")

    # Start marker
    start = EmptyOperator(task_id="start")
    
    # End marker: trigger_rule="all_done" means run regardless of upstream status
    end = EmptyOperator(
        task_id="end",
        trigger_rule="all_done"  # Run when all upstream tasks are done (success, failed, or skipped)
    )

    # Build the pipeline
    stable = stable_task()
    flaky = flaky_task()
    
    # Both tasks run in parallel, then final_task waits for both
    start >> [stable, flaky] >> final_task(stable, flaky) >> end
    # Separate branch for always_fails (doesn't block the main pipeline)
    start >> always_fails() >> end