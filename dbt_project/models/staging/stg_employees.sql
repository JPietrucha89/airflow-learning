with source as (
    select * from {{ source('public', 'employees') }}
),

renamed as (
    select
        id          as employee_id,
        name        as employee_name,
        department  as department,
        salary      as salary_usd
    from source
)

select * from renamed