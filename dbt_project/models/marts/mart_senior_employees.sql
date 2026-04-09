with employees as (
    select * from {{ ref('stg_employees') }}
),

seniors as (
    select
        employee_id,
        employee_name,
        department,
        salary_usd,
        round(salary_usd * 1.1, 2)     as salary_after_raise
    from employees
    where salary_usd > 80000
)

select * from seniors