with employees as (
    select * from {{ ref('stg_employees') }}
),

stats as (
    select
        department,
        count(*)                        as total_employees,
        round(avg(salary_usd), 2)       as avg_salary,
        max(salary_usd)                 as max_salary,
        min(salary_usd)                 as min_salary,
        sum(salary_usd)                 as total_payroll
    from employees
    group by department
)

select * from stats