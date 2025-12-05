#!/usr/bin/env python3
"""
2025年8月のShopifyデータを確認し、スクリーンショットと比較する
"""

import os
import sys
import pandas as pd
import duckdb
from datetime import datetime, timedelta
from dotenv import load_dotenv

# 環境変数を読み込み
load_dotenv()

# データベースパス
DUCKDB_PATH = os.getenv('DUCKDB_PATH', 'data/duckdb/commerce.duckdb')

def check_august_shopify_data():
    """2025年8月のShopifyデータを確認"""
    print("=== 2025年8月のShopifyデータ確認 ===")
    
    try:
        conn = duckdb.connect(DUCKDB_PATH)
        
        # まずcreated_atの型を確認
        type_query = "DESCRIBE core_shopify"
        type_result = conn.execute(type_query).fetchdf()
        print("core_shopifyテーブルの構造:")
        print(type_result.to_string(index=False))
        
        # 2025年8月のデータを取得（型キャストを追加）
        query = """
        SELECT 
            COUNT(*) as total_records,
            COUNT(DISTINCT order_id) as unique_orders,
            SUM(total_price) as sum_total_price,
            SUM(order_total) as sum_order_total,
            SUM(subtotal_price) as sum_subtotal_price,
            SUM(total_line_items_price) as sum_total_line_items_price,
            SUM(total_discounts) as sum_total_discounts,
            SUM(total_tax) as sum_total_tax,
            SUM(shipping_price) as sum_shipping_price,
            SUM(refunds_total) as sum_refunds_total,
            AVG(order_total) as avg_order_total,
            MIN(created_at) as min_date,
            MAX(created_at) as max_date
        FROM core_shopify 
        WHERE strftime(CAST(created_at AS TIMESTAMP), '%Y-%m') = '2025-08'
        """
        
        result = conn.execute(query).fetchone()
        
        if result:
            print(f"期間: {result[11]} 〜 {result[12]}")
            print(f"総レコード数: {result[0]}")
            print(f"ユニーク注文数: {result[1]}")
            print(f"total_price合計: ¥{result[2]:,.0f}")
            print(f"order_total合計: ¥{result[3]:,.0f}")
            print(f"subtotal_price合計: ¥{result[4]:,.0f}")
            print(f"total_line_items_price合計: ¥{result[5]:,.0f}")
            print(f"total_discounts合計: ¥{result[6]:,.0f}")
            print(f"total_tax合計: ¥{result[7]:,.0f}")
            print(f"shipping_price合計: ¥{result[8]:,.0f}")
            print(f"refunds_total合計: ¥{result[9]:,.0f}")
            print(f"平均注文金額: ¥{result[10]:,.0f}")
            
            # スクリーンショットとの比較
            print("\n=== スクリーンショットとの比較 ===")
            print("スクリーンショット（2025年8月）:")
            print("- 総売上: ¥67,470")
            print("- 注文数: 15件")
            print("- 平均注文金額: ¥4,183")
            
            print("\nデータベース（2025年8月）:")
            print(f"- order_total合計: ¥{result[3]:,.0f}")
            print(f"- ユニーク注文数: {result[1]}")
            print(f"- 平均注文金額: ¥{result[10]:,.0f}")
            
            # 差異の計算
            screenshot_total = 67470
            db_total = result[3] if result[3] else 0
            difference = abs(db_total - screenshot_total)
            percentage_diff = (difference / screenshot_total) * 100 if screenshot_total > 0 else 0
            
            print(f"\n差異分析:")
            print(f"- 金額差異: ¥{difference:,.0f}")
            print(f"- 差異率: {percentage_diff:.1f}%")
            
        else:
            print("2025年8月のデータが見つかりません")
            
        # 個別注文の詳細も確認
        print("\n=== 個別注文の詳細 ===")
        detail_query = """
        SELECT 
            order_id,
            order_total,
            total_price,
            subtotal_price,
            total_line_items_price,
            total_discounts,
            total_tax,
            shipping_price,
            refunds_total,
            created_at
        FROM core_shopify 
        WHERE strftime(CAST(created_at AS TIMESTAMP), '%Y-%m') = '2025-08'
        ORDER BY created_at
        """
        
        detail_df = conn.execute(detail_query).fetchdf()
        if not detail_df.empty:
            print(f"注文詳細（{len(detail_df)}件）:")
            print(detail_df.to_string(index=False))
        else:
            print("注文詳細が見つかりません")
            
        conn.close()
        
    except Exception as e:
        print(f"エラー: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    check_august_shopify_data()
