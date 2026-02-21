{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key='month'
) }}

with mrr as (
    select
        month,
        account_id,
        mrr
    from {{ ref('gold_mrr_monthly') }}
),

months as (
    select distinct month
    from mrr
),

account_month as (
    select
        m.month,
        a.account_id,
        coalesce(cur.mrr, 0) as cur_mrr,
        coalesce(prev.mrr, 0) as prev_mrr
    from months m
    join (select distinct account_id from mrr) a on 1=1
    left join mrr cur
        on cur.month = m.month and cur.account_id = a.account_id
    left join mrr prev
        on prev.month = (m.month - interval '1 month')::date and prev.account_id = a.account_id
),

churned as (
    select
        month,
        account_id
    from account_month
    where prev_mrr > 0 and cur_mrr = 0
)

select
    month,
    count(distinct account_id) as churned_accounts
from churned
group by 1
order by 1