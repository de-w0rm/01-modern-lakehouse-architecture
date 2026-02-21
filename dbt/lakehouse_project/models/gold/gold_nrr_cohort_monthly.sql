{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key='cohort_period_key'
) }}

-- Grain: 1 row per (cohort_month, month)

with mrr as (

    select
        month,
        account_id,
        cast(mrr as double) as mrr
    from {{ ref('gold_mrr_monthly') }}

),

cohorts as (

    select
        account_id,
        min(month) as cohort_month
    from mrr
    where mrr > 0
    group by 1

),

cohort_base as (

    select
        c.cohort_month,
        m.account_id,
        m.month,
        m.mrr
    from cohorts c
    join mrr m
      on m.account_id = c.account_id
     and m.month >= c.cohort_month

    {% if is_incremental() %}
      where m.month >= (
        select max(month) from {{ this }}
      ) - interval '6 months'
    {% endif %}

),

cohort_start as (

    select
        cohort_month,
        sum(mrr) as cohort_start_mrr
    from cohort_base
    where month = cohort_month
    group by 1

),

cohort_monthly as (

    select
        cb.cohort_month,
        cb.month,
        sum(cb.mrr) as cohort_current_mrr
    from cohort_base cb
    group by 1,2

),

final as (

    select
        cm.cohort_month,
        cm.month,
        cs.cohort_start_mrr,
        cm.cohort_current_mrr,

        case
            when cs.cohort_start_mrr > 0
                then cm.cohort_current_mrr / cs.cohort_start_mrr
            else null
        end as cohort_nrr,

        cast(cm.cohort_month as varchar) || '|' || cast(cm.month as varchar) as cohort_period_key

    from cohort_monthly cm
    join cohort_start cs
      on cs.cohort_month = cm.cohort_month

)

select * from final