{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key='account_day_key'
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
      -- Reprocess a small rolling window to handle late-arriving events
      where e.event_ts >= (select coalesce(max(date), date '1970-01-01') from {{ this }})::timestamp - interval '7 days'
    {% endif %}
),

agg as (
    select
        account_id,
        cast(date_trunc('day', event_ts) as date) as date,
        count(*) as events,
        count(distinct user_id) as active_users,
        sum(case when event_type = 'purchase' then 1 else 0 end) as purchase_events
    from base
    group by 1, 2
)

select
    cast(account_id as bigint) as account_id,
    date,
    events,
    active_users,
    purchase_events,
    -- surrogate key to support uniqueness tests + incremental delete+insert
    cast(account_id as varchar) || '-' || cast(date as varchar) as account_day_key
from agg