import os
import duckdb

DEFAULT_DB_PATH = "/app/lakehouse/lakehouse.duckdb"

def main():
    db_path = os.getenv("DUCKDB_PATH", DEFAULT_DB_PATH)

    con = duckdb.connect(db_path)

    rows = con.execute("""
        select table_schema, table_type, table_name
        from information_schema.tables
        where table_schema not in ('information_schema', 'pg_catalog')
        order by table_schema, table_type, table_name
    """).fetchall()

    print(f"DuckDB file: {db_path}")
    print("Objects:")
    for schema, table_type, table_name in rows:
        print(f"{schema}\t{table_type}\t{table_name}")

if __name__ == "__main__":
    main()