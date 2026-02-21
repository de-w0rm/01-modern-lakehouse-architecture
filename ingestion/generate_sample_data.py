import os
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

BASE_PATH = "lakehouse/bronze"

def ensure_dir(path):
    os.makedirs(path, exist_ok=True)

def generate_users(n=100):
    now = datetime.utcnow()
    return pd.DataFrame({
        "user_id": np.arange(1, n + 1),
        "account_id": np.random.randint(1, 20, n),
        "email": [f"user{i}@example.com" for i in range(1, n + 1)],
        "created_at": [now - timedelta(days=np.random.randint(0, 365)) for _ in range(n)],
        "updated_at": now
    })

def generate_events(n=1000):
    now = datetime.utcnow()
    return pd.DataFrame({
        "event_id": np.arange(1, n + 1),
        "user_id": np.random.randint(1, 100, n),
        "event_type": np.random.choice(["login", "click", "purchase"], n),
        "event_ts": [now - timedelta(days=np.random.randint(0, 30)) for _ in range(n)]
    })

def write_parquet(df, dataset_name):
    ingestion_date = datetime.utcnow().strftime("%Y-%m-%d")
    path = os.path.join(BASE_PATH, dataset_name, f"ingestion_date={ingestion_date}")
    ensure_dir(path)
    df.to_parquet(os.path.join(path, "data.parquet"), index=False)

if __name__ == "__main__":
    users = generate_users()
    events = generate_events()

    write_parquet(users, "users")
    write_parquet(events, "events")

    print("Sample Bronze data generated successfully.")