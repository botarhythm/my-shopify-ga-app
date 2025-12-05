"""
データ取り込みモジュール
DuckDBへの増分・再実行安全なデータ取り込み
"""
import os
import duckdb
import pandas as pd
import datetime as dt
from typing import Optional
import sys
import argparse

# コネクタをインポート
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from connectors.ga4 import fetch_ga4_daily_all, fetch_ga4_yoy
from connectors.google_ads import fetch_ads_campaign_daily, fetch_ads_adgroup_daily, fetch_ads_keyword_daily
from connectors.shopify import fetch_orders_incremental, fetch_products_incremental
from connectors.square import fetch_payments


def _get_db_path() -> str:
    """DuckDBパスを取得"""
    db_path = os.getenv("DUCKDB_PATH", "./data/duckdb/commerce.duckdb")
    # ディレクトリが存在しない場合は作成
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    return db_path


def _get_connection():
    """DuckDB接続を取得"""
    return duckdb.connect(_get_db_path())


def _last_date(con, table: str, col: str) -> str:
    """
    テーブルの最終日付を取得
    
    Args:
        con: DuckDB接続
        table: テーブル名
        col: 日付列名
    
    Returns:
        str: 最終日付（YYYY-MM-DD）
    """
    try:
        result = con.execute(f"SELECT max({col}) FROM {table}").fetchone()
        if result and result[0]:
            return result[0]
    except:
        pass
    
    # デフォルトは400日前
    default_days = int(os.getenv("DEFAULT_BACKFILL_DAYS", "400"))
    return (dt.date.today() - dt.timedelta(days=default_days)).isoformat()


def upsert(df: pd.DataFrame, table: str, pk: list[str], con):
    """
    データフレームをUpsert（挿入・更新）
    
    Args:
        df: データフレーム
        table: テーブル名
        pk: 主キー列のリスト
        con: DuckDB接続
    """
    if df.empty:
        return
    
    # テーブルが存在しない場合は作成
    con.execute(f"CREATE TABLE IF NOT EXISTS {table} AS SELECT * FROM df LIMIT 0")
    
    # トランザクション開始
    con.execute("BEGIN")
    
    try:
        # 一時テーブルを作成
        tmp_table = f"tmp_{table}"
        con.execute(f"CREATE TEMP TABLE {tmp_table} AS SELECT * FROM df")
        
        # 主キー条件を作成
        pk_conditions = " AND ".join([f"t.{k}=s.{k}" for k in pk])
        
        # 既存データを削除（主キーが一致するもの）
        con.execute(f"DELETE FROM {table} t USING {tmp_table} s WHERE {pk_conditions}")
        
        # 新データを挿入
        con.execute(f"INSERT INTO {table} SELECT * FROM {tmp_table}")
        
        # トランザクションコミット
        con.execute("COMMIT")
        
        print(f"✓ {table}: {len(df)} 行を更新")
        
    except Exception as e:
        con.execute("ROLLBACK")
        print(f"✗ {table}: エラー - {e}")
        raise


def run_incremental(start: Optional[str] = None, end: Optional[str] = None):
    """
    増分データ取り込みを実行
    
    Args:
        start: 開始日（指定しない場合は最終日から）
        end: 終了日（指定しない場合は今日）
    """
    today = dt.date.today()
    start = start or _last_date(_get_connection(), "stg_ga4", "date")
    end = end or today.isoformat()
    
    print(f"🔄 増分取り込み開始: {start} 〜 {end}")
    
    con = _get_connection()
    
    try:
        # GA4 データ
        print("📊 GA4 データ取得中...")
        ga4_df = fetch_ga4_daily_all(start, end)
        if not ga4_df.empty:
            upsert(ga4_df, "stg_ga4", ["date", "source", "sessionDefaultChannelGroup", "pagePath"], con)
        
        # Google Ads キャンペーンデータ
        print("📈 Google Ads キャンペーンデータ取得中...")
        ads_campaign_df = fetch_ads_campaign_daily(start, end)
        if not ads_campaign_df.empty:
            upsert(ads_campaign_df, "stg_ads_campaign", ["date", "campaign_id"], con)
        
        # Google Ads 広告グループデータ
        print("📈 Google Ads 広告グループデータ取得中...")
        ads_adgroup_df = fetch_ads_adgroup_daily(start, end)
        if not ads_adgroup_df.empty:
            upsert(ads_adgroup_df, "stg_ads_adgroup", ["date", "campaign_id", "ad_group_id"], con)
        
        # Google Ads キーワードデータ
        print("📈 Google Ads キーワードデータ取得中...")
        ads_keyword_df = fetch_ads_keyword_daily(start, end)
        if not ads_keyword_df.empty:
            upsert(ads_keyword_df, "stg_ads_keyword", ["date", "campaign_id", "ad_group_id", "keyword"], con)
        
        # Shopify 注文データ
        print("🛒 Shopify 注文データ取得中...")
        shopify_orders_df = fetch_orders_incremental(start)
        if not shopify_orders_df.empty:
            upsert(shopify_orders_df, "stg_shopify_orders", ["order_id", "lineitem_id"], con)
        
        # Shopify 商品データ
        print("🛒 Shopify 商品データ取得中...")
        shopify_products_df = fetch_products_incremental(start)
        if not shopify_products_df.empty:
            upsert(shopify_products_df, "stg_shopify_products", ["product_id", "variant_id"], con)
        
        # Square 支払いデータ
        print("💳 Square 支払いデータ取得中...")
        square_payments_df = fetch_payments(start, end)
        if not square_payments_df.empty:
            upsert(square_payments_df, "stg_square_payments", ["payment_id"], con)
        
        print("✅ 増分取り込み完了")
        
    except Exception as e:
        print(f"❌ 増分取り込みエラー: {e}")
        raise
    finally:
        con.close()


def run_backfill(start: str, end: str):
    """
    バックフィル（全期間）データ取り込みを実行
    
    Args:
        start: 開始日
        end: 終了日
    """
    print(f"🔄 バックフィル開始: {start} 〜 {end}")
    
    # 日付範囲を分割して処理（API制限対策）
    start_date = dt.datetime.strptime(start, "%Y-%m-%d")
    end_date = dt.datetime.strptime(end, "%Y-%m-%d")
    
    current_date = start_date
    while current_date <= end_date:
        # 30日ずつ処理
        batch_end = min(current_date + dt.timedelta(days=30), end_date)
        batch_start = current_date.strftime("%Y-%m-%d")
        batch_end_str = batch_end.strftime("%Y-%m-%d")
        
        print(f"📅 バッチ処理: {batch_start} 〜 {batch_end_str}")
        run_incremental(batch_start, batch_end_str)
        
        current_date = batch_end + dt.timedelta(days=1)
    
    print("✅ バックフィル完了")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="データ取り込み実行")
    parser.add_argument("--start", help="開始日 (YYYY-MM-DD)")
    parser.add_argument("--end", help="終了日 (YYYY-MM-DD)")
    parser.add_argument("--backfill", action="store_true", help="バックフィルモード")
    
    args = parser.parse_args()
    
    if args.backfill:
        if not args.start or not args.end:
            print("バックフィルには --start と --end が必要です")
            sys.exit(1)
        run_backfill(args.start, args.end)
    else:
        run_incremental(args.start, args.end)
