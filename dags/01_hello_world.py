"""
LESSON 01: HELLO WORLD - DAG BASICS

This is the simplest possible Airflow DAG. It demonstrates:
  1. How to define a DAG
  2. The @task decorator (modern Python-first approach)
  3. Task dependencies (task >> task)
  4. Data passing between tasks (XCom, implicit)

KEY CONCEPTS:
  - DAG: Directed Acyclic Graph. A set of tasks with defined dependencies.
  - dag_id: Unique identifier for this DAG (shown in Airflow UI)
  - schedule: None = manual trigger only. Use cron for automatic scheduling.
  - @task: Decorator that turns a Python function into an Airflow task
  - Return values: Automatically become "XComs" (cross-communication between tasks)
  
WHAT IT DOES:
  1. say_hello() task runs first, prints greeting, returns "hello"
  2. say_world() task runs second, receives "hello" as input, prints final message

HOW TO RUN:
  1. In Airflow UI (http://localhost:8080), find DAG "01_hello_world"
  2. Click the "play" button to trigger it manually (schedule=None)
  3. Watch tasks execute in the graph view
  4. Click on each task to view logs

KEY TAKEAWAY:
  The function call syntax say_world(say_hello()) is both:
  - A normal Python function call (for testing)
  - An Airflow dependency declaration (for orchestration)
"""

from airflow.sdk import DAG, task
from datetime import datetime

with DAG(
    dag_id="01_hello_world",
    start_date=datetime(2024, 1, 1),
    schedule=None,          # manual trigger only
    catchup=False,
    tags=["learning"],
) as dag:

    @task  # Turns this function into an Airflow task
    def say_hello():
        """First task: prints a greeting and returns a value"""
        print(f"\n{'='*50}")
        print("Hello from Airflow 3!")
        print(f"\n{'='*50}")
        return "hello"  # This return value becomes an XCom (task communication)

    @task  # Second task: receives data from the first task
    def say_world(greeting: str):
        """Second task: receives the greeting and prints a combined message"""
        print(f"\n{'='*50}")
        print(f"{greeting} — world!")
        print(f"\n{'='*50}")

    # Task dependency: say_world runs after say_hello
    # The returned "hello" is automatically passed to say_world()
    say_world(say_hello())