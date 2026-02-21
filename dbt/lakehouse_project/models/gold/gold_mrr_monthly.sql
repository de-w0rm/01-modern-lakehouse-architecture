{{ config(materialized='table') }}

with active_subs as (
    select
        account_id,
        monthly_price,
        start_date,
        coalesce(end_date, current_date) as end_date
    from {{ ref('silver_subscriptions') }}
),

calendar as (
    select distinct date_trunc('month', invoice_date) as month
    from {{ ref('silver_invoices') }}
),

mrr as (
    select
        c.month,
        s.account_id,
        s.monthly_price as mrr
    from calendar c
    join active_subs s
      on c.month between date_trunc('month', s.start_date)
                     and date_trunc('month', s.end_date)
)

select
    month,
    sum(mrr) as total_mrr,
    count(distinct account_id) as active_accounts
from mrr
group by 1
order by 1