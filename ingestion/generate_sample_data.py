import os
import uuid
import random
import pandas as pd
import numpy as np
from datetime import datetime, timedelta, date

BASE_PATH = "lakehouse/bronze"

def ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)

def write_parquet(df: pd.DataFrame, dataset_name: str) -> None:
    ingestion_date = datetime.utcnow().strftime("%Y-%m-%d")
    path = os.path.join(BASE_PATH, dataset_name, f"ingestion_date={ingestion_date}")
    ensure_dir(path)
    df.to_parquet(os.path.join(path, "data.parquet"), index=False)

def generate_users(n: int = 100) -> pd.DataFrame:
    now = datetime.utcnow()
    return pd.DataFrame({
        "user_id": np.arange(1, n + 1),
        "account_id": np.random.randint(1, 21, n),  # 1..20 inclusive
        "email": [f"user{i}@example.com" for i in range(1, n + 1)],
        "created_at": [now - timedelta(days=np.random.randint(0, 365)) for _ in range(n)],
        "updated_at": [now for _ in range(n)],
    })

def generate_events(n: int = 1000, user_max: int = 100) -> pd.DataFrame:
    now = datetime.utcnow()
    return pd.DataFrame({
        "event_id": np.arange(1, n + 1),
        "user_id": np.random.randint(1, user_max + 1, n),
        "event_type": np.random.choice(["login", "click", "purchase"], n),
        "event_ts": [now - timedelta(days=np.random.randint(0, 30)) for _ in range(n)],
    })

def generate_subscriptions(accounts) -> pd.DataFrame:
    plans = [
        {"plan": "basic", "price": 29.0},
        {"plan": "pro", "price": 79.0},
        {"plan": "enterprise", "price": 199.0},
    ]

    subs = []
    today = datetime.utcnow().date()

    for account_id in accounts:
        plan = random.choice(plans)
        start_date = today - timedelta(days=random.randint(30, 365))

        # 20% churn probability
        if random.random() < 0.2:
            end_date = start_date + timedelta(days=random.randint(30, 180))
        else:
            end_date = None

        subs.append({
            "subscription_id": str(uuid.uuid4()),
            "account_id": int(account_id),
            "plan": plan["plan"],
            "monthly_price": float(plan["price"]),
            "start_date": start_date,
            "end_date": end_date,
        })

    return pd.DataFrame(subs)

def add_months_approx(d: date, months: int = 1) -> date:
    # Good enough for synthetic data; avoids external deps
    return (datetime.combine(d, datetime.min.time()) + timedelta(days=30 * months)).date()

def generate_invoices(subscriptions: pd.DataFrame) -> pd.DataFrame:
    invoices = []
    today = datetime.utcnow().date()

    for _, row in subscriptions.iterrows():
        start = row["start_date"]
        end = row["end_date"] if pd.notna(row["end_date"]) else today

        current = start
        while current <= end:
            invoices.append({
                "invoice_id": str(uuid.uuid4()),
                "account_id": int(row["account_id"]),
                "subscription_id": row["subscription_id"],
                "invoice_date": current,
                "amount": float(row["monthly_price"]),
                "status": "paid",
            })
            current = add_months_approx(current, 1)

    return pd.DataFrame(invoices)

if __name__ == "__main__":
    users = generate_users()
    events = generate_events(user_max=len(users))

    accounts = users["account_id"].unique()
    subscriptions = generate_subscriptions(accounts)
    invoices = generate_invoices(subscriptions)

    write_parquet(users, "users")
    write_parquet(events, "events")
    write_parquet(subscriptions, "subscriptions")
    write_parquet(invoices, "invoices")

    print("Sample Bronze data generated successfully:")
    print(f"- users: {len(users)}")
    print(f"- events: {len(events)}")
    print(f"- subscriptions: {len(subscriptions)}")
    print(f"- invoices: {len(invoices)}")