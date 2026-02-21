select *
from read_parquet('/app/lakehouse/bronze/users/*/*.parquet')