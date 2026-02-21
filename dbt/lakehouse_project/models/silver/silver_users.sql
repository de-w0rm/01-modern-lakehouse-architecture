{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key='user_id'
) }}

with src as (
    select
        cast(user_id as bigint) as user_id,
        cast(account_id as bigint) as account_id,
        lower(trim(email)) as email,
        cast(created_at as timestamp) as created_at,
        cast(updated_at as timestamp) as updated_at
    from {{ ref('stg_bronze_users') }}
),

dedup as (
    select *
    from (
        select
            *,
            row_number() over (
                partition by user_id
                order by updated_at desc, created_at desc
            ) as rn
        from src
    )
    where rn = 1
)

select
    user_id,
    account_id,
    email,
    created_at,
    updated_at
from dedup