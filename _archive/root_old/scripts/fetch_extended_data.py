#!/usr/bin/env python3
"""
拡張データ取得スクリプト
ShopifyとSquareからより広範囲のデータを取得し、継続的なデータ活用を可能にする
"""
import os
import sys
import datetime as dt
import duckdb
import pandas as pd
from pathlib import Path
from dotenv import load_dotenv
from typing import Optional, Tuple

# .envファイルを読み込み
load_dotenv()

# プロジェクトルートをパスに追加
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.connectors.shopify import fetch_orders_incremental
from src.connectors.square import fetch_payments

class ExtendedDataFetcher:
    """拡張データ取得クラス"""
    
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.con = None
        
    def connect(self):
        """データベースに接続"""
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
        self.con = duckdb.connect(self.db_path, read_only=False)
        self.con.execute("PRAGMA threads=4; PRAGMA enable_object_cache=true;")
        return self.con
    
    def close(self):
        """データベース接続を閉じる"""
        if self.con:
            self.con.close()
    
    def get_data_range(self) -> Tuple[str, str]:
        """データ取得範囲を決定"""
        today = dt.date.today()
        
        # 2年分のデータを取得
        start_date = (today - dt.timedelta(days=730)).isoformat()
        end_date = today.isoformat()
        
        return start_date, end_date
    
    def fetch_shopify_data(self, start_date: str, end_date: str) -> pd.DataFrame:
        """Shopifyから広範囲のデータを取得"""
        print(f"Shopifyデータ取得開始: {start_date} ～ {end_date}")
        
        # Shopifyの日付形式に変換
        shopify_start = start_date + 'T00:00:00'
        
        try:
            data = fetch_orders_incremental(created_at_min=shopify_start)
            print(f"Shopifyデータ取得完了: {len(data)}行")
            
            if len(data) > 0:
                print(f"  - 最古データ: {data.iloc[-1]['date']}")
                print(f"  - 最新データ: {data.iloc[0]['date']}")
                print(f"  - 総売上: ¥{data['total_price'].sum():,.0f}")
            
            return data
            
        except Exception as e:
            print(f"Shopifyデータ取得エラー: {e}")
            return pd.DataFrame()
    
    def fetch_square_data(self, start_date: str, end_date: str) -> pd.DataFrame:
        """Squareから広範囲のデータを取得"""
        print(f"Squareデータ取得開始: {start_date} ～ {end_date}")
        
        try:
            data = fetch_payments(start_date, end_date)
            print(f"Squareデータ取得完了: {len(data)}行")
            
            if len(data) > 0:
                print(f"  - 最古データ: {data.iloc[-1]['date']}")
                print(f"  - 最新データ: {data.iloc[0]['date']}")
                print(f"  - 総売上: ¥{data['amount'].sum():,.0f}")
            
            return data
            
        except Exception as e:
            print(f"Squareデータ取得エラー: {e}")
            return pd.DataFrame()
    
    def upsert_data(self, df: pd.DataFrame, table: str, pk: list[str]):
        """データをデータベースに保存"""
        if df is None or df.empty:
            print(f"{table}: データなし、スキップ")
            return
        
        print(f"{table}: {len(df)}行をデータベースに保存中...")
        
        # テーブル作成（なければ）
        self.con.execute(f"CREATE TABLE IF NOT EXISTS {table} AS SELECT * FROM df LIMIT 0")
        
        # 一時テーブルにロード → 擬似MERGE（DELETE+INSERT）
        self.con.register("df", df)
        
        # 主キーで既存データを削除
        cond = " AND ".join([f"t.{k}=s.{k}" for k in pk])
        self.con.execute("BEGIN")
        self.con.execute(f"CREATE TEMP TABLE tmp_{table} AS SELECT * FROM df")
        self.con.execute(f"DELETE FROM {table} t USING tmp_{table} s WHERE {cond}")
        self.con.execute(f"INSERT INTO {table} SELECT * FROM tmp_{table}")
        self.con.execute("COMMIT")
        self.con.unregister("df")
        
        print(f"{table}: 保存完了")
    
    def initialize_schema(self):
        """データベーススキーマを初期化"""
        print("データベーススキーマ初期化中...")
        
        try:
            bootstrap_sql_path = "scripts/bootstrap.sql"
            if not os.path.exists(bootstrap_sql_path):
                raise FileNotFoundError(f"bootstrap.sql not found: {bootstrap_sql_path}")
            
            with open(bootstrap_sql_path, "r", encoding="utf-8") as f:
                sql = f.read()
            
            # SQLを分割して実行
            sql_statements = [stmt.strip() for stmt in sql.split(';') if stmt.strip()]
            for i, stmt in enumerate(sql_statements):
                try:
                    self.con.execute(stmt)
                    print(f"SQL実行 {i+1}/{len(sql_statements)}: {stmt[:50]}...")
                except Exception as e:
                    print(f"SQL実行エラー {i+1}/{len(sql_statements)}: {e}")
                    print(f"   SQL: {stmt[:100]}...")
            
            print("データベーススキーマ初期化完了")
            
        except Exception as e:
            print(f"データベーススキーマ初期化エラー: {e}")
            raise
    
    def run_extended_fetch(self):
        """拡張データ取得を実行"""
        print("=== 拡張データ取得開始 ===")
        print(f"データベース: {self.db_path}")
        
        # データベースに接続
        self.connect()
        
        # スキーマ初期化
        self.initialize_schema()
        
        # データ取得範囲を決定
        start_date, end_date = self.get_data_range()
        print(f"データ取得期間: {start_date} ～ {end_date}")
        
        # Shopifyデータ取得
        shopify_data = self.fetch_shopify_data(start_date, end_date)
        
        # Squareデータ取得
        square_data = self.fetch_square_data(start_date, end_date)
        
        # データベースに保存
        print("\n=== データベース保存 ===")
        self.upsert_data(shopify_data, "core_shopify", ["order_id", "lineitem_id"])
        self.upsert_data(square_data, "core_square", ["payment_id"])
        
        # データベース接続を閉じる
        self.close()
        
        print("\n=== 拡張データ取得完了 ===")
        
        # 結果サマリー
        if not shopify_data.empty:
            print(f"Shopify: {len(shopify_data)}行")
        if not square_data.empty:
            print(f"Square: {len(square_data)}行")

def main():
    """メイン関数"""
    db_path = os.getenv("DUCKDB_PATH", "./data/duckdb/commerce_fresh.duckdb")
    
    fetcher = ExtendedDataFetcher(db_path)
    
    try:
        fetcher.run_extended_fetch()
    except Exception as e:
        print(f"拡張データ取得エラー: {e}")
        import traceback
        traceback.print_exc()
        return 1
    
    return 0

if __name__ == "__main__":
    exit(main())
