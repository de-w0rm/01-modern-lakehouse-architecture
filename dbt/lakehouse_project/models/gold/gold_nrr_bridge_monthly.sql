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
        cast(mrr as double) as mrr,
        account_month_key
    from {{ ref('gold_mrr_monthly') }}

    {% if is_incremental() %}
      -- include enough history for lag() correctness
      where month >= (
        select max(month) from {{ this }}
      ) - interval '3 months'
    {% endif %}

),

with_history as (

    select
        month,
        account_id,
        mrr,
        account_month_key,

        lag(mrr) over (
            partition by account_id
            order by month
        ) as prev_mrr,

        -- had any MRR before the previous month? (used to split new_logo vs reactivation)
        max(case when mrr > 0 then 1 else 0 end) over (
            partition by account_id
            order by month
            rows between unbounded preceding and 1 preceding
        ) as had_mrr_before

    from base

),

bridge as (

    select
        month,
        account_id,
        account_month_key,

        coalesce(prev_mrr, 0) as starting_mrr,
        mrr as ending_mrr,

        case
            when coalesce(prev_mrr, 0) = 0 and mrr > 0 and coalesce(had_mrr_before, 0) = 0
                then mrr
            else 0
        end as new_logo_mrr,

        case
            when coalesce(prev_mrr, 0) = 0 and mrr > 0 and coalesce(had_mrr_before, 0) = 1
                then mrr
            else 0
        end as reactivation_mrr,

        case
            when coalesce(prev_mrr, 0) > 0 and mrr > coalesce(prev_mrr, 0)
                then (mrr - coalesce(prev_mrr, 0))
            else 0
        end as expansion_mrr,

        case
            when coalesce(prev_mrr, 0) > 0 and mrr > 0 and mrr < coalesce(prev_mrr, 0)
                then (coalesce(prev_mrr, 0) - mrr)
            else 0
        end as contraction_mrr,

        case
            when coalesce(prev_mrr, 0) > 0 and mrr = 0
                then coalesce(prev_mrr, 0)
            else 0
        end as churn_mrr

    from with_history

),

final as (

    select
        month,
        account_id,
        account_month_key,

        starting_mrr,
        ending_mrr,

        new_logo_mrr,
        reactivation_mrr,
        expansion_mrr,
        contraction_mrr,
        churn_mrr,

        -- NRR cohort ending excludes new logo + reactivation by construction:
        -- starting + expansion - contraction - churn
        (starting_mrr + expansion_mrr - contraction_mrr - churn_mrr) as ending_mrr_for_nrr,

        case
            when starting_mrr > 0
                then (starting_mrr + expansion_mrr - contraction_mrr - churn_mrr) / starting_mrr
            else null
        end as nrr_ratio,

        case
            when starting_mrr > 0
                then (starting_mrr - contraction_mrr - churn_mrr) / starting_mrr
            else null
        end as grr_ratio

    from bridge

)

select * from final