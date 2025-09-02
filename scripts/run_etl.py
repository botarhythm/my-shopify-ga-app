# scripts/run_etl.py
import os, datetime as dt
import duckdb, pandas as pd
from pathlib import Path

# ===== Connectors（あなたの src/connectors 実装へ合わせて import を調整）=====
from src.connectors.ga4 import fetch_ga4_daily_all
from src.connectors.google_ads import fetch_ads_campaign_daily
from src.connectors.shopify import fetch_orders_incremental
from src.connectors.square import fetch_payments

DB = os.getenv("DUCKDB_PATH", "./data/duckdb/commerce.duckdb")
DEFAULT_BACKFILL_DAYS = int(os.getenv("DEFAULT_BACKFILL_DAYS", "400"))

def connect_rw():
    con = duckdb.connect(DB, read_only=False)
    con.execute("PRAGMA threads=4; PRAGMA enable_object_cache=true;")
    return con

def last_date(con, table, col):
    try:
        d = con.execute(f"SELECT max({col}) FROM {table}").fetchone()[0]
    except duckdb.CatalogException:
        d = None
    return d

def upsert_df(con, df: pd.DataFrame, table: str, pk: list[str]):
    if df is None or df.empty:
        return
    # テーブル作成（なければ）
    con.execute(f"CREATE TABLE IF NOT EXISTS {table} AS SELECT * FROM df LIMIT 0")
    # 一時テーブルにロード → 擬似MERGE（DELETE+INSERT）
    con.register("df", df)
    cond = " AND ".join([f"t.{k}=s.{k}" for k in pk])
    con.execute("BEGIN")
    con.execute(f"CREATE TEMP TABLE tmp_{table} AS SELECT * FROM df")
    con.execute(f"DELETE FROM {table} t USING tmp_{table} s WHERE {cond}")
    con.execute(f"INSERT INTO {table} SELECT * FROM tmp_{table}")
    con.execute("COMMIT")
    con.unregister("df")

def run_incremental(start: str|None=None, end: str|None=None):
    Path(DB).parent.mkdir(parents=True, exist_ok=True)
    con = connect_rw()

    today = dt.date.today()
    end   = end or today.isoformat()

    # 既存データから開始点を決める（無ければ400日前）
    start_guess = (today - dt.timedelta(days=DEFAULT_BACKFILL_DAYS)).isoformat()
    start = start or (last_date(con, "core_ga4", "date") or start_guess)

    # === FETCH ===
    ga4 = fetch_ga4_daily_all(start, end)
    ads = fetch_ads_campaign_daily(start, end)
    shop = fetch_orders_incremental(start)     # Shopifyは created_at_min= start
    sq   = fetch_payments(start, end)

    # === UPSERT to STG相当（core_*に直に格納でもOK）===
    upsert_df(con, ga4,  "core_ga4", ["date","source","channel","page_path"])
    upsert_df(con, ads,  "core_ads_campaign", ["date","campaign_id"])
    upsert_df(con, shop, "core_shopify", ["order_id","lineitem_id"])
    upsert_df(con, sq,   "core_square", ["payment_id"])

    # === 変換ビューは bootstrap.sql が作ってくれているので不要 ===
    con.close()

if __name__ == "__main__":
    # 例：python scripts/run_etl.py で当日まで更新
    run_incremental()
