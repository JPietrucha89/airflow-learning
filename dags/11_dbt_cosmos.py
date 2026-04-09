"""
LESSON 11: DBT INTEGRATION (COSMOS METHOD) - NATIVE DBT INTEGRATION

This DAG demonstrates:
  1. Astronomer's Cosmos library for native dbt integration
  2. DbtDag: Automatically generates Airflow DAG from dbt project
  3. ProfileConfig: How to connect dbt to your database
  4. Better visibility: See dbt model dependencies in Airflow

KEY CONCEPTS (Cosmos):
  - DbtDag: Creates a complete Airflow DAG from your dbt project
    Automatically maps dbt models to Airflow tasks
    Respects dbt model dependencies (materializations, refs, etc.)
  - ProfileConfig: Tell Cosmos how to connect to the database
    postgres_learning Connection is used for dbt_project connection
  - ExecutionConfig: Where dbt files are located
  - ProjectConfig: Path to dbt manifest (dbt dependencies graph)
  - RenderConfig: Include/exclude certain models
    exclude=["path:models/example/*"] = Skip example folder models

ADVANTAGES OVER BASH:
  ✅ Automatic task generation from dbt models
  ✅ Respects dbt dependencies (more granular than bash)
  ✅ Better visualization in Airflow UI
  ✅ Native dbt integration (not just running CLI)
  ✅ Easier to debug individual model failures
  ✅ Built-in dbt test handling

DISADVANTAGES:
  ❌ Requires Cosmos library installation (astronomer-cosmos)
  ❌ Slightly more setup than bash
  ❌ Learning curve for configuration

WHAT IT DOES:
  1. Reads dbt project from /opt/airflow/dbt_project
  2. Creates a task for each dbt model
  3. Automatically links tasks based on dbt dependencies
  4. Connects to postgres_learning for execution
  5. Excludes models in the example folder

REAL-WORLD COMPARISON:
  Bash: dbt run (all models in one task)
  Cosmos: Automatic tasks for each model, see dependencies in UI

KEY TAKEAWAY:
  Cosmos is the "modern" way to integrate dbt with Airflow.
  It bridges dbt's DAG with Airflow's task graph perfectly!
"""

# airflow dag
from pathlib import Path
from cosmos import DbtDag, ProjectConfig, ProfileConfig, ExecutionConfig, RenderConfig
from cosmos.profiles import PostgresUserPasswordProfileMapping
from datetime import datetime

# Configure how Cosmos connects to the database
profile_config = ProfileConfig(
    profile_name="dbt_project",        # Name of dbt profile
    target_name="dev",                  # Target (dev/prod/staging)
    # Use the postgres_learning Connection from Airflow UI
    profile_mapping=PostgresUserPasswordProfileMapping(
        conn_id="postgres_learning",
        profile_args={"schema": "public"},
    ),
)

# Configure where dbt files are located
execution_config = ExecutionConfig(
    dbt_executable_path="/home/airflow/.local/bin/dbt",
    dbt_project_path=Path("/opt/airflow/dbt_project"),
)

# Configure the dbt project itself
project_config=ProjectConfig(
    # Cosmos reads the manifest to understand model dependencies
    manifest_path=Path("/opt/airflow/dbt_project/target/manifest.json"),
    project_name="dbt_project",
)

# Configure which models to include
render_config=RenderConfig(
    exclude=["path:models/example/*"],  # Skip example models
)

# Create the DAG
# This automatically generates tasks for each dbt model!
my_cosmos_dag = DbtDag(
    dag_id="11_dbt_cosmos",
    project_config=project_config,
    profile_config=profile_config,
    execution_config=execution_config,
    render_config=render_config,
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["learning", "dbt"],
    # The DAG is automatically built from dbt models and dependencies!
)