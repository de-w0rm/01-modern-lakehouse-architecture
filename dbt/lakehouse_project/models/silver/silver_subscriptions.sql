{{ config(materialized='incremental',
          incremental_strategy='delete+insert',
          unique_key='subscription_id') }}

select
    cast(subscription_id as varchar) as subscription_id,
    cast(account_id as bigint) as account_id,
    plan,
    cast(monthly_price as double) as monthly_price,
    cast(start_date as date) as start_date,
    cast(end_date as date) as end_date
from {{ ref('stg_bronze_subscriptions') }}