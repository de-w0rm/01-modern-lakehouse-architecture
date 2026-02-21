select *
from read_parquet('/app/lakehouse/bronze/invoices/*/*.parquet')