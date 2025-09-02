# scripts/bootstrap_duckdb.py
import duckdb
import os

DB = os.getenv("DUCKDB_PATH", "./data/duckdb/commerce.duckdb")

def bootstrap():
    """DuckDBスキーマ初期化"""
    con = duckdb.connect(DB, read_only=False)
    
    # ブートストラップSQLを実行
    with open("scripts/bootstrap.sql", "r", encoding="utf-8") as f:
        sql = f.read()
    
    con.execute(sql)
    con.close()
    print(f"✅ DuckDB初期化完了: {DB}")

if __name__ == "__main__":
    bootstrap()
