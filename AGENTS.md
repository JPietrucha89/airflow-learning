# Agent Guide: Airflow Learning

## 🚀 Quick Start
- **Infrastructure:** Docker Compose based.
- **Initialize DB:** `docker compose up airflow-init` (only once).
- **Start All:** `docker compose up -d`.
- **UI:** [http://localhost:8080](http://localhost:8080) (login: `airflow` / `airflow`).
- **Python Env:** Use `.venv` for local IDE support (already scaffolded).

## 🗄️ Database Context
There are **two** Postgres instances in `docker-compose.yaml`:
1. `postgres`: Airflow metadata (internal use).
2. `postgres-learning`: The target DB for ETL/dbt tasks.
   - **Host (inside docker):** `postgres-learning`
   - **Host (from Windows):** `localhost`
   - **Port:** 5432
   - **Credentials:** `airflow_user` / `airflow_pass`
   - **DB Name:** `airflow_db`
   - **Connection ID in Airflow:** `postgres_learning` (must be created in UI if missing).

## 🛠️ Development Workflow
- **DAGs:** Located in `dags/`. Named `01_*` to `12_*` for progressive learning.
- **dbt:**
  - Project: `dbt_project/`
  - Profiles: `dbt_profiles/`
  - Integration: Uses both `BashOperator` (manual) and `Cosmos` (automated).
- **Verification:**
  - Use `docker exec -it airflow-learning-postgres-learning-1 psql -U airflow_user -d airflow_db` to inspect data.

## ⚠️ Gotchas
- **Windows Pathing:** Use `/opt/airflow/...` when referencing paths *inside* DAGs/Tasks, but use local Windows paths when editing.
- **Dependencies:** `dbt-postgres` and `astronomer-cosmos` are auto-installed in containers via `_PIP_ADDITIONAL_REQUIREMENTS`.
- **DAG Discovery:** `AIRFLOW__CORE__DAG_DISCOVERY_SAFE_MODE` is `False` to ensure all files are scanned.
