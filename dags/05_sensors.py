"""
LESSON 05: SENSORS - WAITING FOR EXTERNAL EVENTS

This DAG demonstrates:
  1. FileSensor: Wait for a file to appear before proceeding
  2. Sensor configuration: poke_interval, timeout, mode
  3. Sensor vs regular tasks: Sensors wait, tasks execute

KEY CONCEPTS:
  - Sensor: A task that checks for a condition repeatedly until it's met
    or times out. Perfect for waiting on external events.
  - FileSensor: Checks if a file exists at a specific path
  - poke_interval: How often to check (in seconds)
    - 10 = Check every 10 seconds
  - timeout: How long to wait before failing (in seconds)
    - 120 = Fail after 2 minutes if file never appears
  - mode: How to check
    - "poke" = Actively check (uses a worker)
    - "reschedule" = Check periodically (less resource-intensive)

WHAT IT DOES:
  1. wait_for_file: Sensor waits for /opt/airflow/dags/trigger.txt to appear
  2. process_file: Once file appears, this task processes it
  3. cleanup: Removes the trigger file after processing

REAL-WORLD USE CASES:
  - Wait for daily data file to be uploaded (then process it)
  - Wait for external system to finish (then continue)
  - Wait for database refresh (then query fresh data)
  - File-based orchestration between systems

HOW TO TEST:
  1. Trigger this DAG
  2. It will wait for trigger.txt to appear
  3. In another terminal: touch /path/to/trigger.txt
  4. Watch the DAG proceed!

KEY TAKEAWAY:
  Sensors make pipelines event-driven instead of just time-driven.
  Perfect for handling data that arrives unpredictably!
"""

from airflow.sdk import DAG, task
from airflow.providers.standard.sensors.filesystem import FileSensor
from datetime import datetime
import os

with DAG(
    dag_id="05_sensors",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["learning"],
) as dag:

    # FileSensor: Wait for a file to appear
    wait_for_file = FileSensor(
        task_id="wait_for_file",
        filepath="/opt/airflow/dags/trigger.txt",  # The file to wait for
        poke_interval=10,    # Check every 10 seconds
        timeout=120,         # Fail after 2 minutes if file never appears
        mode="poke",         # Active polling mode
    )
    # NOTE: FileSensor requires a Connection in Airflow UI:
    #   Admin → Connections
    #   Connection Id: fs_default
    #   Type: File (path)
    #   Path: /opt/airflow/dags

    @task
    def process_file():
        """Task that runs AFTER the file is detected"""
        print("File detected! Processing...")
        # In real pipelines, you'd read the file here
        # df = pd.read_csv("/opt/airflow/dags/trigger.txt")

    @task
    def cleanup():
        """Clean up: Remove the trigger file after processing"""
        path = "/opt/airflow/dags/trigger.txt"
        if os.path.exists(path):
            os.remove(path)
            print("Cleaned up trigger file")

    # Define the sensor-based workflow
    wait_for_file >> process_file() >> cleanup()