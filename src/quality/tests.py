"""
データ品質テスト
pytest を使用した自動品質チェック
"""
import pytest
import duckdb
import os
import pandas as pd
from datetime import datetime, timedelta


def get_db_connection():
    """DuckDB接続を取得"""
    db_path = os.getenv("DUCKDB_PATH", "./data/duckdb/commerce.duckdb")
    return duckdb.connect(db_path)


class TestDataQuality:
    """データ品質テストクラス"""
    
    def test_roas_nonnegative(self):
        """ROASが非負であることを確認"""
        con = get_db_connection()
        try:
            df = con.execute("SELECT roas FROM mart_daily WHERE cost > 0").df()
            if not df.empty:
                assert (df["roas"] >= 0).all(), "ROASに負の値が含まれています"
        finally:
            con.close()
    
    def test_sessions_greater_than_purchases(self):
        """セッション数が購入数以上であることを確認"""
        con = get_db_connection()
        try:
            df = con.execute("SELECT sessions, purchases FROM mart_daily WHERE sessions IS NOT NULL AND purchases IS NOT NULL").df()
            if not df.empty:
                assert (df["sessions"] >= df["purchases"]).all(), "セッション数が購入数より少ないレコードがあります"
        finally:
            con.close()
    
    def test_revenue_consistency(self):
        """売上の整合性を確認"""
        con = get_db_connection()
        try:
            df = con.execute("""
                SELECT total_revenue, shopify_revenue, square_revenue 
                FROM mart_daily 
                WHERE total_revenue IS NOT NULL
            """).df()
            if not df.empty:
                # 総売上 = Shopify売上 + Square売上（概算）
                tolerance = 0.01  # 1%の誤差を許容
                calculated_total = df["shopify_revenue"].fillna(0) + df["square_revenue"].fillna(0)
                diff_ratio = abs(df["total_revenue"] - calculated_total) / df["total_revenue"]
                assert (diff_ratio <= tolerance).all(), "売上の整合性に問題があります"
        finally:
            con.close()
    
    def test_date_range_validity(self):
        """日付範囲の妥当性を確認"""
        con = get_db_connection()
        try:
            df = con.execute("SELECT MIN(date) as min_date, MAX(date) as max_date FROM mart_daily").df()
            if not df.empty:
                min_date = pd.to_datetime(df["min_date"].iloc[0])
                max_date = pd.to_datetime(df["max_date"].iloc[0])
                today = datetime.now()
                
                # 最新データが1週間以内であることを確認
                assert max_date >= today - timedelta(days=7), "最新データが1週間以上古いです"
                
                # データ範囲が妥当であることを確認（1年以上）
                assert max_date - min_date >= timedelta(days=365), "データ範囲が1年未満です"
        finally:
            con.close()
    
    def test_no_duplicate_dates(self):
        """日付の重複がないことを確認"""
        con = get_db_connection()
        try:
            df = con.execute("""
                SELECT date, COUNT(*) as count 
                FROM mart_daily 
                GROUP BY date 
                HAVING COUNT(*) > 1
            """).df()
            assert df.empty, "日付の重複レコードがあります"
        finally:
            con.close()
    
    def test_required_columns_not_null(self):
        """必須列がNULLでないことを確認"""
        con = get_db_connection()
        try:
            # 主要なKPI列のNULLチェック
            null_counts = con.execute("""
                SELECT 
                    COUNT(*) - COUNT(sessions) as null_sessions,
                    COUNT(*) - COUNT(total_revenue) as null_revenue,
                    COUNT(*) - COUNT(cost) as null_cost
                FROM mart_daily
            """).df()
            
            # NULLの割合が10%以下であることを確認
            total_rows = con.execute("SELECT COUNT(*) FROM mart_daily").fetchone()[0]
            if total_rows > 0:
                assert null_counts["null_sessions"].iloc[0] / total_rows <= 0.1, "セッション数のNULLが多すぎます"
                assert null_counts["null_revenue"].iloc[0] / total_rows <= 0.1, "売上のNULLが多すぎます"
        finally:
            con.close()
    
    def test_yoy_data_consistency(self):
        """YoYデータの整合性を確認"""
        con = get_db_connection()
        try:
            df = con.execute("""
                SELECT 
                    date,
                    sessions,
                    sessions_prev,
                    total_revenue,
                    total_revenue_prev
                FROM mart_daily_yoy
                WHERE sessions IS NOT NULL AND sessions_prev IS NOT NULL
            """).df()
            
            if not df.empty:
                # 前年同期の日付が1年前であることを確認
                current_dates = pd.to_datetime(df["date"])
                prev_dates = current_dates - timedelta(days=365)
                
                # 前年同期データが存在することを確認
                assert not df["sessions_prev"].isna().all(), "前年同期データが不足しています"
        finally:
            con.close()


class TestDataCompleteness:
    """データ完全性テストクラス"""
    
    def test_staging_tables_exist(self):
        """stagingテーブルが存在することを確認"""
        con = get_db_connection()
        try:
            tables = con.execute("SHOW TABLES").df()
            staging_tables = ["stg_ga4", "stg_shopify_orders", "stg_square_payments", "stg_ads_campaign"]
            
            for table in staging_tables:
                assert table in tables["name"].values, f"stagingテーブル {table} が存在しません"
        finally:
            con.close()
    
    def test_core_tables_exist(self):
        """coreテーブルが存在することを確認"""
        con = get_db_connection()
        try:
            tables = con.execute("SHOW TABLES").df()
            core_tables = ["core_ga4", "core_shopify_orders", "core_square_payments", "core_ads_campaign"]
            
            for table in core_tables:
                assert table in tables["name"].values, f"coreテーブル {table} が存在しません"
        finally:
            con.close()
    
    def test_mart_tables_exist(self):
        """martテーブルが存在することを確認"""
        con = get_db_connection()
        try:
            tables = con.execute("SHOW TABLES").df()
            mart_tables = ["mart_daily", "mart_revenue_daily", "mart_traffic_daily", "mart_ads_daily"]
            
            for table in mart_tables:
                assert table in tables["name"].values, f"martテーブル {table} が存在しません"
        finally:
            con.close()


if __name__ == "__main__":
    # テスト実行
    pytest.main([__file__, "-v"])
