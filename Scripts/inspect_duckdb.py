import os
import duckdb

DEFAULT_DB_PATH = "/app/lakehouse/lakehouse.duckdb"

def safe_ident(name: str) -> str:
    # Quote identifiers to handle weird names safely
    return '"' + name.replace('"', '""') + '"'

def main():
    db_path = os.getenv("DUCKDB_PATH", DEFAULT_DB_PATH)
    con = duckdb.connect(db_path)

    objects = con.execute("""
        select table_schema, table_name, table_type
        from information_schema.tables
        where table_schema not in ('information_schema', 'pg_catalog')
        order by table_schema, table_type, table_name
    """).fetchall()

    print(f"DuckDB file: {db_path}")
    print("Objects (with row counts):")

    for schema, table_name, table_type in objects:
        full_name = f"{safe_ident(schema)}.{safe_ident(table_name)}"
        try:
            row_count = con.execute(f"select count(*) from {full_name}").fetchone()[0]
        except Exception as e:
            row_count = f"ERROR: {e.__class__.__name__}"

        print(f"{schema}\t{table_type}\t{table_name}\trows={row_count}")

if __name__ == "__main__":
    main()