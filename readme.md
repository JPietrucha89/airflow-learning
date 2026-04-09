# Airflow Learning Repository

## 1. Setup Local Environment

Create `.venv` and install apache-airflow:

```bash
python3 -m venv .venv
# On Windows:
.venv\Scripts\activate
# On macOS/Linux:
source .venv/bin/activate

pip install apache-airflow
```

## 2. Download Docker Configuration

Download the official Airflow Docker Compose file:

```bash
curl -o docker-compose.yaml https://airflow.apache.org/docs/apache-airflow/stable/docker-compose.yaml
```

## 3. Project Initialization

Create the required folders and env file:

```bash
mkdir dags logs plugins config
echo "AIRFLOW_UID=50000" > .env
```

Any `.py` file you drop into the `dags/` folder is automatically picked up by Airflow — no restart needed. You can edit files in Windows with VS Code as normal.

## 4. Start Airflow

```bash
# First-time DB setup (run only once)
docker compose up airflow-init 

# Start all services
docker compose up -d
```

Then open **http://localhost:8080** in your browser.  
**Login:** `airflow`  
**Password:** `airflow`

## 5. Airflow Connection for Postgres (Target DB)

Configure this in the Airflow UI (**Admin -> Connections**):

- **Connection Id:** `postgres_learning`
- **Connection Type:** `Postgres`
- **Host:** `postgres-learning`
- **Schema:** `airflow_db`
- **Login:** `airflow_user`
- **Password:** `airflow_pass`
- **Port:** `5432`

## 6. Verification & Persistent Volumes

To check if the docker volume is persistent:

1. Start container: `docker compose up -d`
2. Run a query inside the Postgres container:

```bash
docker exec -it airflow-learning-postgres-learning-1 psql -U airflow_user -d airflow_db -c "SELECT * FROM employees;"
```

**Breakdown of the command:**
```
docker exec -it airflow-learning-postgres-learning-1 psql -U airflow_user -d airflow_db -c "SELECT * FROM employees;"
  │      │    │                                        │    │              │             │
  │      │    │                                        │    │              │             └── SQL query to run
  │      │    │                                        │    │              └── database name to connect to
  │      │    │                                        │    └── username to connect with
  │      │    │                                        └── the psql CLI tool (Postgres client)
  │      │    └── name of the container to run the command in
  │      └── -i = interactive, -t = tty (terminal)
  └── run a command inside a running container
```

## 7. dbt Integration

Install dbt and Cosmos in your local environment:

```bash
# Windows
.venv\Scripts\activate
pip install dbt-postgres astronomer-cosmos
```
