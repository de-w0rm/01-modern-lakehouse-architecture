# SaaS Domain Model

## Entities
- accounts (workspace / customer container)
- users
- subscriptions
- invoices
- events

## Grains
### Bronze (raw)
- users/accounts/subscriptions/invoices: snapshot per ingestion run
- events: append-only

### Silver (conformed)
- one row per entity id (deduped, typed, standardized)

### Gold (marts)
- dim_account, dim_user, dim_plan
- fct_subscription_daily (subscription_id, date)
- fct_revenue_monthly (account_id, month)
- fct_product_usage_daily (account_id, date)

## Keys and Relationships
- users.account_id -> accounts.account_id
- subscriptions.account_id -> accounts.account_id
- invoices.subscription_id -> subscriptions.subscription_id
- events.user_id -> users.user_id
- events.account_id -> accounts.account_id

## Incremental Strategy
- events: incremental by event_ts (append-only)
- invoices: incremental by issued_at; handle updates via merge
- users/accounts/subscriptions: merge by id using updated_at (latest wins)