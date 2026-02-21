# Partitioning and Incremental Strategy

## Storage Layout
lakehouse/
- bronze/<dataset>/ingestion_date=YYYY-MM-DD/*.parquet
- silver/<dataset>/snapshot_date=YYYY-MM-DD/*.parquet
- gold/<mart>/<date or month partitions>/*.parquet

## Bronze Partitioning
- All datasets partition by ingestion_date for auditability and replay.
- events may later add event_date if volume requires additional pruning.

## Silver Partitioning
- Snapshot-like entities partition by snapshot_date.
- events partition by event_date for time-series access patterns.

## Gold Partitioning
- fct_subscription_daily partition by date
- fct_product_usage_daily partition by date
- fct_revenue_monthly partition by month
- dimensions typically unpartitioned.

## Incremental Strategy
### Append-only
- events incremental by event_ts (append)
- unique key: event_id

### Merge (mutable entities)
- users/accounts/subscriptions/invoices merge by natural id using updated_at (latest record wins in Silver)
- unique keys:
  - user_id, account_id, subscription_id, invoice_id

## Gold Unique Keys
- fct_subscription_daily: (subscription_id, date)
- fct_product_usage_daily: (account_id, date)
- fct_revenue_monthly: (account_id, month)