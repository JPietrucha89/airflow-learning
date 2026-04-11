## 1. Let's create a dedicated employees_scd table:

```
docker exec -it airflow-learning-postgres-learning-1 psql -U airflow_user -d airflow_db -c "
CREATE TABLE OR REPLACE employees_scd (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100),
    department VARCHAR(100),
    department_code VARCHAR(10),
    salary INTEGER,
    updated_at TIMESTAMP DEFAULT NOW()
);

INSERT INTO employees_scd (name, department, department_code, salary) VALUES
    ('Eve', 'Engineering', 'ENG', 102000),
    ('Frank', 'Marketing', 'MKT', 78000),
    ('Grace', 'HR', 'HR', 69000),
    ('Henry', 'Engineering', 'ENG', 91000)
;
"
```

## 2. Run the initial snapshot:

```
cd C:\Kodzenie\Python\airflow-learning\dbt_project
dbt snapshot --select employees_snapshot
```

## 3. Then simulate Eve's promotion:

```
docker exec -it airflow-learning-postgres-learning-1 psql -U airflow_user -d airflow_db -c "
UPDATE employees_scd
SET
    department = 'Engineering Lead',
    department_code = 'ENG-L',
    salary = 120000,
    updated_at = NOW()
WHERE name = 'Eve'
;
"
```

## 4.Run snapshot again (after simulated changes) and query the result:

```powershell
dbt snapshot --select employees_snapshot
```

```
docker exec -it airflow-learning-postgres-learning-1 psql -U airflow_user -d airflow_db -c "
SELECT *
FROM snapshots.employees_snapshot
ORDER BY id, updated_at
;
"
```

## 5. Simulate a hard delete. Remove Grace from the source table:

```
docker exec -it airflow-learning-postgres-learning-1 psql -U airflow_user -d airflow_db -c "
DELETE FROM employees_scd WHERE name = 'Grace'
;
"
```

## 6. Then run the snapshot again:

```
dbt snapshot --select employees_snapshot
```

## 7. Query Grace's record:

```
docker exec -it airflow-learning-postgres-learning-1 psql -U airflow_user -d airflow_db -c "
SELECT *
FROM snapshots.employees_snapshot
WHERE name = 'Grace'
;
"
```

Because in `employees_snapshot.sql` we set `invalidate_hard_deletes=True`, Grace's record should now have a `dbt_valid_to` timestamp instead of NULL — meaning dbt detected she was deleted from the source and marked her snapshot record as expired.
