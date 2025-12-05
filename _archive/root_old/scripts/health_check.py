# scripts/health_check.py
import duckdb
import os

DB = os.getenv("DUCKDB_PATH", "./data/duckdb/commerce_test.duckdb")

def health_check():
    """DuckDBヘルスチェック"""
    con = duckdb.connect(DB, read_only=True)
    
    print("🔍 DuckDBヘルスチェック開始...")
    
    # 1. テーブル存在確認
    tables = con.execute("SHOW TABLES").fetchall()
    print(f"✅ テーブル数: {len(tables)}")
    for table in tables:
        print(f"  - {table[0]}")
    
    # 2. ビュー存在確認（SHOW VIEWSの代わりに直接確認）
    view_names = ["mart_revenue_daily", "mart_traffic_daily", "mart_ads_daily", "mart_daily", "mart_daily_yoy"]
    print(f"✅ ビュー確認:")
    for view_name in view_names:
        try:
            con.execute(f"SELECT 1 FROM {view_name} LIMIT 1")
            print(f"  - {view_name}: ✅")
        except Exception as e:
            print(f"  - {view_name}: ❌ ({e})")
    
    # 3. mart_daily_yoy確認
    try:
        yoy_count = con.execute("SELECT COUNT(*) FROM mart_daily_yoy").fetchone()[0]
        print(f"✅ mart_daily_yoy: {yoy_count}行")
    except Exception as e:
        print(f"❌ mart_daily_yoy: {e}")
    
    # 4. 必須KPI確認
    try:
        null_count = con.execute("""
            SELECT COUNT(*) FROM mart_daily 
            WHERE total_revenue IS NULL AND cost IS NULL AND sessions IS NULL
        """).fetchone()[0]
        print(f"✅ NULLデータ: {null_count}行")
    except Exception as e:
        print(f"❌ KPI確認: {e}")
    
    con.close()
    print("✅ ヘルスチェック完了")

if __name__ == "__main__":
    health_check()
