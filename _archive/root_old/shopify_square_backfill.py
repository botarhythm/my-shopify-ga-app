#!/usr/bin/env python3
"""
ShopifyとSquareのデータのみでバックフィル実行
"""

import os
from dotenv import load_dotenv
from src.connectors.shopify import fetch_shopify_all_incremental
from src.connectors.square import fetch_square_all
import duckdb
import pandas as pd

def run_shopify_square_backfill(start_date: str, end_date: str):
    """ShopifyとSquareのデータのみでバックフィル実行"""
    
    # 環境変数を読み込み
    load_dotenv()
    
    # DuckDB接続
    db_path = os.getenv('DUCKDB_PATH', './data/duckdb/commerce.duckdb')
    conn = duckdb.connect(db_path)
    
    print(f"🔄 Shopify & Square バックフィル開始: {start_date} 〜 {end_date}")
    
    try:
        # Shopifyデータ取得
        print("📊 Shopify データ取得中...")
        shopify_data = fetch_shopify_all_incremental(start_date, end_date)
        
        # Squareデータ取得
        print("💳 Square データ取得中...")
        square_data = fetch_square_all(start_date, end_date)
        
        # データベースに保存
        print("💾 データベースに保存中...")
        
        # Shopifyデータを保存
        if not shopify_data['orders'].empty:
            conn.execute("DROP TABLE IF EXISTS staging_shopify_orders")
            conn.execute("CREATE TABLE staging_shopify_orders AS SELECT * FROM shopify_data['orders']")
            print(f"✅ Shopify注文: {len(shopify_data['orders'])}件保存")
        
        if not shopify_data['products'].empty:
            conn.execute("DROP TABLE IF EXISTS staging_shopify_products")
            conn.execute("CREATE TABLE staging_shopify_products AS SELECT * FROM shopify_data['products']")
            print(f"✅ Shopify商品: {len(shopify_data['products'])}件保存")
        
        # Squareデータを保存
        if not square_data['payments'].empty:
            conn.execute("DROP TABLE IF EXISTS staging_square_payments")
            conn.execute("CREATE TABLE staging_square_payments AS SELECT * FROM square_data['payments']")
            print(f"✅ Square支払い: {len(square_data['payments'])}件保存")
        
        if not square_data['refunds'].empty:
            conn.execute("DROP TABLE IF EXISTS staging_square_refunds")
            conn.execute("CREATE TABLE staging_square_refunds AS SELECT * FROM square_data['refunds']")
            print(f"✅ Square返金: {len(square_data['refunds'])}件保存")
        
        conn.close()
        print("🎉 バックフィル完了！")
        
    except Exception as e:
        print(f"❌ エラー: {e}")
        conn.close()

if __name__ == "__main__":
    import sys
    
    if len(sys.argv) != 3:
        print("使用方法: python shopify_square_backfill.py <start_date> <end_date>")
        print("例: python shopify_square_backfill.py 2025-08-01 2025-08-31")
        sys.exit(1)
    
    start_date = sys.argv[1]
    end_date = sys.argv[2]
    
    run_shopify_square_backfill(start_date, end_date)
