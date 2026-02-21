{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key='month'
) }}

-- Grain: 1 row per month

with bridge as (

    select *
    from {{ ref('gold_nrr_bridge_monthly') }}

    {% if is_incremental() %}
      where month >= (
        select max(month) from {{ this }}
      ) - interval '3 months'
    {% endif %}

),

monthly as (

    select
        month,

        sum(starting_mrr) as starting_mrr_total,
        sum(ending_mrr) as ending_mrr_total,

        sum(new_logo_mrr) as new_logo_mrr_total,
        sum(reactivation_mrr) as reactivation_mrr_total,
        sum(expansion_mrr) as expansion_mrr_total,
        sum(contraction_mrr) as contraction_mrr_total,
        sum(churn_mrr) as churn_mrr_total,

        sum(ending_mrr_for_nrr) as ending_mrr_for_nrr_total,

        case
            when sum(starting_mrr) > 0
                then sum(ending_mrr_for_nrr) / sum(starting_mrr)
            else null
        end as nrr_ratio,

        case
            when sum(starting_mrr) > 0
                then (sum(starting_mrr) - sum(contraction_mrr) - sum(churn_mrr)) / sum(starting_mrr)
            else null
        end as grr_ratio

    from bridge
    group by 1

)

select * from monthly