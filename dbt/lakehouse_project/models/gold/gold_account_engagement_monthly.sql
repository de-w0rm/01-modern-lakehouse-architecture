{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key='account_month_key'
) }}

with base as (
    select
        u.account_id,
        e.user_id,
        e.event_type,
        e.event_ts
    from {{ ref('silver_events') }} e
    join {{ ref('silver_users') }} u
      on e.user_id = u.user_id

    {% if is_incremental() %}
      -- Reprocess last 2 months for safety
      where e.event_ts >= (select coalesce(max(month), date '1970-01-01') from {{ this }})::timestamp - interval '62 days'
    {% endif %}
),

agg as (
    select
        account_id,
        cast(date_trunc('month', event_ts) as date) as month,
        count(*) as events,
        count(distinct user_id) as monthly_active_users,
        sum(case when event_type = 'purchase' then 1 else 0 end) as purchase_events
    from base
    group by 1, 2
)

select
    cast(account_id as bigint) as account_id,
    month,
    events,
    monthly_active_users,
    purchase_events,
    cast(account_id as varchar) || '-' || cast(month as varchar) as account_month_key
from agg