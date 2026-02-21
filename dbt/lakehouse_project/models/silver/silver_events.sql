{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key='event_id'
) }}

select
    cast(event_id as bigint) as event_id,
    cast(user_id as bigint) as user_id,
    cast(event_type as varchar) as event_type,
    cast(event_ts as timestamp) as event_ts
from {{ ref('stg_bronze_events') }}

{% if is_incremental() %}
where event_ts > (select max(event_ts) from {{ this }})
{% endif %}