select *
from read_parquet('/app/lakehouse/bronze/events/*/*.parquet')