# Create .venv and install apache-airflow

python3 -m venv .venv
source .venv/bin/activate or .venv\Scripts\activate
pip install apache-airflow

# Download the official Airflow Docker Compose file

curl -o docker-compose.yaml https://airflow.apache.org/docs/apache-airflow/stable/docker-compose.yaml

# Create the required folders and env file

mkdir dags logs plugins config
echo "AIRFLOW_UID=50000" > .env

Any .py file you drop into the dags\ folder is automatically picked up by Airflow — no restart needed. You edit files in Windows with VS Code as normal.

# Start Airflow

docker compose up airflow-init # first-time DB setup (run only once)
docker compose up # starts all services

Then open **http://localhost:8080** in your browser. Login: `airflow` / `airflow`.

# Connection in Airflow UI for Postgres db

Connection Id: postgres_learning
Connection Type: Postgres
Host: postgres-learning
Schema: airflow_db
Login: airflow_user
Password: airflow_pass
Port: 5432

# TO check if docker volume is persistent

1. Start container again - docker compose up -d
2.

```
docker exec -it airflow-learning-postgres-learning-1 psql -U airflow_user -d airflow_db -c "SELECT * FROM employees;"
  │      │    │                                        │    │              │             │
  │      │    │                                        │    │              │             └── SQL query to run
  │      │    │                                        │    │              └── database name to connect to
  │      │    │                                        │    └── username to connect with
  │      │    │                                        └── the psql CLI tool (Postgres client)
  │      │    └── name of the container to run the command in
  │      └── -i = interactive (keep stdin open)
  │           -t = tty (format output nicely in terminal)
  └── run a command inside a running container
```

So in plain English:
"Inside the running container airflow-learning-postgres-learning-1, run the psql tool, connect as airflow_user to the airflow_db database, and execute this SQL query"

The -it flags together are a common combo you'll see everywhere with docker exec — -i keeps the connection open and -t makes the output readable in your terminal. You'll need them whenever you want to run an interactive command inside a container.

# Add dbt to this tutorial project

source .venv/Scripts/activate
pip install dbt-postgres
pip install astronomer-cosmos
