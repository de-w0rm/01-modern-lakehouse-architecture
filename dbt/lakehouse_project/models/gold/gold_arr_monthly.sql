{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key='account_month_key'
) }}

-- Grain: 1 row per (account_id, month)

with base as (

    select
        month,
        account_id,
        account_month_key,
        cast(mrr as double) as mrr
    from {{ ref('gold_mrr_monthly') }}

    {% if is_incremental() %}
      where month >= (
        select max(month) from {{ this }}
      ) - interval '3 months'
    {% endif %}

)

select
    month,
    account_id,
    account_month_key,
    mrr,
    mrr * 12 as arr
from base