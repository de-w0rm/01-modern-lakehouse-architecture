{{ config(materialized='incremental',
          incremental_strategy='delete+insert',
          unique_key='invoice_id') }}

select
    cast(invoice_id as varchar) as invoice_id,
    cast(subscription_id as varchar) as subscription_id,
    cast(account_id as bigint) as account_id,
    cast(invoice_date as date) as invoice_date,
    cast(amount as double) as amount,
    cast(status as varchar) as status
from {{ ref('stg_bronze_invoices') }}