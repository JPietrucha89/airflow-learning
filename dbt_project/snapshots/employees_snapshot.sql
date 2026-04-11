{% snapshot employees_snapshot %}

{{
    config(
        target_schema='snapshots',
        unique_key='id',
        strategy='timestamp',
        updated_at='updated_at',
        invalidate_hard_deletes=True,
    )
}}

select
    id,
    name,
    department,
    department_code,
    salary,
    updated_at
from {{ source('public', 'employees_scd') }}

{% endsnapshot %}

-- Here's what each snapshot config option means:
-- Option                           -> What it does
-- unique_key='id'                  -> How dbt identifies the same record across runs
-- strategy='timestamp'             -> Detect changes by comparing updated_at timestamp
-- updated_at='updated_at'          -> Which column to use as the timestamp
-- invalidate_hard_deletes=True     -> If a row disappears from source, mark it as expired
-- target_schema='snapshots'        -> Store snapshot table in a separate schema