# Airflow-Learning Repository - Complete Analysis

## 📋 Repository Overview

**Project Name:** airflow-learning  
**Purpose:** A comprehensive, hands-on learning repository for Apache Airflow  
**Type:** Educational Tutorial Project  
**Architecture:** Docker-based Airflow deployment with PostgreSQL and dbt integration  
**Target Audience:** Beginners to intermediate Airflow learners

---

## 🎯 What It Contains

### Directory Structure
```
airflow-learning/
├── dags/                 # 12 progressively complex DAG examples
├── dbt_project/          # dbt transformation project
├── dbt_profiles/         # dbt connection profiles
├── config/               # Airflow configuration
├── logs/                 # Execution logs
├── plugins/              # Custom Airflow plugins
├── docker-compose.yaml   # Full local environment
└── .env                  # Environment variables
```

### Docker Services
1. **Airflow Scheduler** - Schedules and monitors DAGs
2. **Airflow Webserver** - UI at http://localhost:8080
3. **PostgreSQL** - Data warehouse for learning
4. **Redis** - Message broker for task queuing
5. **Airflow Workers** - Execute tasks in parallel

---

## 📚 Airflow Concepts Explained (Progressive Order)

### 1️⃣ DAG Basics (01_hello_world.py)
- **What is a DAG:** Directed Acyclic Graph of tasks
- **@task decorator:** Modern Python-native way to create tasks
- **Task dependencies:** Using `>>` operator to chain tasks
- **Output passing:** Return values pass automatically to downstream tasks

```python
@task
def say_hello():
    return "hello"

@task
def say_world(greeting: str):
    print(f"{greeting} — world!")

say_world(say_hello())
```

### 2️⃣ Scheduling (02_scheduling.py)
- **schedule parameter:** Cron expressions like `"*/5 * * * *"` (every 5 mins)
- **start_date:** When the DAG first runs
- **catchup:** Whether to backfill missed runs (False for learning)
- **tags:** Organize DAGs in the UI
- **Basic ETL pattern:** Extract → Transform → Load

### 3️⃣ XComs - Cross Communication (03_xcoms.py)
- **Purpose:** How tasks share data between each other
- **Implicit XComs:** Tasks automatically capture return values
- **Passing complex data:** Dicts, lists, objects all work
- **Use case:** Passing extracted data to multiple downstream tasks simultaneously

### 4️⃣ Branching (04_branching.py)

**Operators Used:**
- **BranchPythonOperator:** Decide which path to execute at runtime
- **EmptyOperator:** No-op task for structuring pipelines

**Key Concepts:**
- Return a task_id (string) from the branch function
- Airflow executes only that task; others are skipped
- Use `trigger_rule="one_success"` to join branches back together

```python
def decide_branch():
    if condition:
        return "high_score"
    else:
        return "low_score"
```

### 5️⃣ Sensors (05_sensors.py)

**FileSensor:**
- Waits for a file to exist before proceeding
- `poke_interval=10` - Check every 10 seconds
- `timeout=120` - Fail after 2 minutes
- `mode="poke"` - Actively poll

**Other Sensors Available:**
- `TimeSensor` - Wait until specific time
- `ExternalTaskSensor` - Wait for another DAG to complete
- `S3KeySensor` - Wait for S3 file
- `SqlSensor` - Wait for SQL condition

### 6️⃣ Connections & Hooks (06_connections.py)

**Connection (in Airflow UI):**
- Stored credentials for external systems
- Admin → Connections
- Example: postgres_learning with host, user, password, port

**Hook (Python Class):**
- Uses Connection to interact with systems
- PostgresHook, S3Hook, HttpHook, etc.

**PostgresHook Methods:**
```python
hook = PostgresHook(postgres_conn_id="postgres_learning")

hook.run(sql)                    # Execute any SQL
hook.insert_rows(table, rows)    # Bulk insert
hook.get_records("SELECT ...")   # Fetch all rows
hook.get_first("SELECT ...")     # Fetch single row
```

**Key Benefit:** Credentials are never hardcoded in DAGs

### 7️⃣ Taskflow API (07_taskflow.py)

- **@task decorator** with type hints
- Return type annotations help Airflow understand data
- Write clean, testable business logic
- Modern Python-first approach

```python
@task
def extract() -> list[dict]:
    # Query and return data

@task
def transform(employees: list[dict]) -> list[dict]:
    # Process data

@task
def summarise(employees: list[dict]) -> dict:
    # Aggregate results
```

### 8️⃣ Error Handling (08_error_handling.py)

**Retry Mechanism:**
```python
default_args={
    "retries": 3,
    "retry_delay": timedelta(seconds=10),
}
```

**Callbacks:**
```python
def on_failure_callback(context):
    # Send alerts, log failures
    
def on_success_callback(context):
    # Log successes, notify team
```

**Trigger Rules:**
- `"one_success"` - Run if any upstream succeeded
- `"all_done"` - Run regardless of upstream status
- `"all_success"` - Only if all upstream succeeded

### 9️⃣ External SQL Files (09_external_sql.py)

- Read SQL from `.sql` files (separate from Python)
- Load file relative to DAG location
- Execute complex queries via hooks
- Benefits: Team reusability, version control, readability

### 🔟 DBT Integration - Bash Method (10_dbt_bash.py)

**Operator Used:** BashOperator

```python
BashOperator(
    task_id="dbt_debug",
    bash_command=f"cd {DBT_PROJECT_DIR} && dbt debug --profiles-dir {DBT_PROFILES_DIR}"
)
```

**Sequential Execution:**
1. dbt debug
2. dbt test (sources)
3. dbt run (staging models)
4. dbt run (marts)
5. dbt test (all models)

**Use Case:** Basic orchestration of dbt runs via Airflow

### 1️⃣1️⃣ DBT Integration - Cosmos Method (11_dbt_cosmos.py)

**DbtDag (Astronomer Cosmos):**
- Native dbt integration with Airflow
- Generates task dependency graph from dbt DAG
- Better visibility and error handling

**Configuration:**
```python
profile_config = ProfileConfig(
    profile_name="dbt_project",
    target_name="dev",
    profile_mapping=PostgresUserPasswordProfileMapping(
        conn_id="postgres_learning",
        profile_args={"schema": "public"},
    ),
)
```

### 1️⃣2️⃣ Full Production Pipeline (12_full_pipeline.py)

**Three Layers:**
1. **Ingestion:** Fresh daily employee data
2. **Transformation:** DbtTaskGroup for complex transformations
3. **Reporting:** Generate human-readable reports

**Schedule:** Daily at 8 AM (`schedule="0 8 * * *"`)

**Data Flow:**
```
Ingest → dbt Transform → Report Results
```

---

## ⚙️ Airflow Operators Used

| Operator | Files | Purpose |
|----------|-------|---------|
| `@task` | All | Execute Python functions as tasks |
| `BranchPythonOperator` | 04_branching | Conditional task routing |
| `EmptyOperator` | 04, 08 | Structural placeholders |
| `FileSensor` | 05_sensors | Wait for file to exist |
| `BashOperator` | 10_dbt_bash | Execute shell commands |
| `DbtDag` | 11_dbt_cosmos | Run entire dbt project as DAG |
| `DbtTaskGroup` | 12_full_pipeline | Embed dbt in DAG |

---

## 📦 Hooks Used

| Hook | Files | Purpose |
|------|-------|---------|
| `PostgresHook` | 06, 07, 09, 12 | Execute SQL, insert/fetch data |
| `FileSensor` | 05 | Detect file arrivals |

---

## 🗄️ Database Setup

**Database:** PostgreSQL (in Docker)

**Connection Details:**
```
Connection Id: postgres_learning
Type: Postgres
Host: postgres-learning
User: airflow_user
Password: airflow_pass
Database: airflow_db
Port: 5432
```

**Tables Created:**
- `employees` - Test data with name, department, salary
- `department_summary` - Aggregated statistics
- dbt-generated marts with transformed data

---

## 📊 Learning Progression

### Beginner Level
✓ 01 - Hello World: Basic @task decorator  
✓ 02 - Scheduling: Cron expressions and ETL  
✓ 03 - XComs: Passing data between tasks

### Intermediate Level
✓ 04 - Branching: Conditional task execution  
✓ 05 - Sensors: Wait for external conditions  
✓ 06 - Connections: Database connectivity  
✓ 07 - Taskflow: Professional DAG structure

### Advanced Level
✓ 08 - Error Handling: Retries, callbacks, trigger rules  
✓ 09 - External SQL: File-based SQL execution  
✓ 10 - dbt + Bash: Orchestrating dbt runs  
✓ 11 - dbt + Cosmos: Native dbt integration  
✓ 12 - Full Pipeline: Production-like ELT workflow

---

## 🎓 Key Patterns & Features

1. **Taskflow API** - Modern Python-first DAG development
2. **Error Handling** - Retries, callbacks, trigger rules
3. **Data Sharing** - XCom for inter-task communication
4. **Conditional Logic** - BranchPythonOperator for routing
5. **Waiting Patterns** - Sensors for event-driven workflows
6. **Database Operations** - PostgresHook for SQL execution
7. **DBT Transformation** - Two approaches (Bash + Cosmos)
8. **Scheduling** - Cron-based or manual triggers
9. **Docker Deployment** - Complete local dev environment
10. **Production Readiness** - Full ETL/ELT pipeline example

---

## 🚀 Ideal Use Cases

✓ Learning Airflow from scratch  
✓ Understanding DAG design patterns  
✓ Exploring operator types and hooks  
✓ Practicing scheduling and error handling  
✓ Integrating dbt with Airflow  
✓ Setting up a local Airflow development environment  
✓ Reference for small to medium data pipelines  
✓ Teaching Airflow concepts to team members

---

## 🔗 Quick Reference

**Start Airflow:**
```bash
docker compose up airflow-init    # First time only
docker compose up                  # Start services
```

**Access UI:**
- URL: http://localhost:8080
- Username: airflow
- Password: airflow

**Check Database:**
```bash
docker exec -it airflow-learning-postgres-learning-1 psql \
  -U airflow_user -d airflow_db -c "SELECT * FROM employees;"
```

**Required Packages:**
```bash
pip install apache-airflow
pip install dbt-postgres
pip install astronomer-cosmos
```

---

Generated: 2026-04-04  
Analyzed by: Copilot CLI
