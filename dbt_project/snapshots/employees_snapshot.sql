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