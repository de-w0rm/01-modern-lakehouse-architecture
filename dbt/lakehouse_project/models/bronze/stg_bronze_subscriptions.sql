select *
from read_parquet('/app/lakehouse/bronze/subscriptions/*/*.parquet')