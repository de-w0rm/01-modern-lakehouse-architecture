{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key='month'
) }}

-- Grain: 1 row per month

with base as (

    select
        month,
        cast(arr as double) as arr
    from {{ ref('gold_arr_monthly') }}

    {% if is_incremental() %}
      where month >= (
        select max(month) from {{ this }}
      ) - interval '3 months'
    {% endif %}

)

select
    month,
    sum(arr) as arr_total
from base
group by 1