-- Create a summary table
CREATE TABLE IF NOT EXISTS department_summary (
    department VARCHAR(100),
    total_employees INTEGER,
    avg_salary NUMERIC(10, 2),
    max_salary INTEGER,
    min_salary INTEGER
);

-- Clear previous results
TRUNCATE TABLE department_summary;

-- Insert fresh summary
INSERT INTO department_summary
SELECT
    department,
    COUNT(*)            AS total_employees,
    AVG(salary)         AS avg_salary,
    MAX(salary)         AS max_salary,
    MIN(salary)         AS min_salary
FROM employees
GROUP BY department;