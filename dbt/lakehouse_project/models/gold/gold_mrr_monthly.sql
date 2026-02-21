{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key='account_month_key'
) }}

with subs as (
    select
        account_id,
        monthly_price,
        start_date,
        coalesce(end_date, current_date) as end_date
    from {{ ref('silver_subscriptions') }}
),

months as (
    select distinct
        cast(date_trunc('month', invoice_date) as date) as month
    from {{ ref('silver_invoices') }}
),

active_subs_by_month as (
    select
        m.month,
        s.account_id,
        sum(s.monthly_price) as mrr
    from months m
    join subs s
      on m.month between cast(date_trunc('month', s.start_date) as date)
                     and cast(date_trunc('month', s.end_date) as date)
    group by 1, 2
)

select
    month,
    account_id,
    mrr,
    cast(account_id as varchar) || '-' || cast(month as varchar) as account_month_key
from active_subs_by_month