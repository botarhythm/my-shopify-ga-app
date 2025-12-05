#!/usr/bin/env python3
"""
正しいShopify売上集計方法を実装
重複カウントを排除し、純売上を計算する
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

def calculate_correct_shopify_revenue():
    """正しいShopify売上を計算"""
    print("=== 正しいShopify売上計算 ===")
    
    try:
        conn = duckdb.connect(DUCKDB_PATH)
        
        # 2025年8月の正しい売上計算（重複排除）
        query = """
        SELECT 
            COUNT(DISTINCT order_id) as unique_orders,
            SUM(DISTINCT order_total) as net_revenue,
            AVG(order_total) as avg_order_value,
            MIN(CAST(created_at AS TIMESTAMP)) as min_date,
            MAX(CAST(created_at AS TIMESTAMP)) as max_date
        FROM core_shopify 
        WHERE strftime(CAST(created_at AS TIMESTAMP), '%Y-%m') = '2025-08'
        """
        
        result = conn.execute(query).fetchone()
        
        if result:
            print(f"期間: {result[3]} 〜 {result[4]}")
            print(f"ユニーク注文数: {result[0]}")
            print(f"純売上（重複排除）: ¥{result[1]:,.0f}")
            print(f"平均注文金額: ¥{result[2]:,.0f}")
            
            # スクリーンショットとの比較
            print("\n=== スクリーンショットとの比較 ===")
            print("スクリーンショット（2025年8月）:")
            print("- 総売上: ¥67,470")
            print("- 注文数: 15件")
            print("- 平均注文金額: ¥4,183")
            
            print("\nデータベース（修正後）:")
            print(f"- 純売上: ¥{result[1]:,.0f}")
            print(f"- ユニーク注文数: {result[0]}")
            print(f"- 平均注文金額: ¥{result[2]:,.0f}")
            
            # 差異の計算
            screenshot_total = 67470
            db_total = result[1] if result[1] else 0
            difference = abs(db_total - screenshot_total)
            percentage_diff = (difference / screenshot_total) * 100 if screenshot_total > 0 else 0
            
            print(f"\n差異分析:")
            print(f"- 金額差異: ¥{difference:,.0f}")
            print(f"- 差異率: {percentage_diff:.1f}%")
            
            # 個別注文の詳細（重複排除）
            print("\n=== 個別注文の詳細（重複排除） ===")
            detail_query = """
            SELECT 
                order_id,
                order_total,
                subtotal_price,
                total_discounts,
                total_tax,
                shipping_price,
                refunds_total,
                created_at
            FROM core_shopify 
            WHERE strftime(CAST(created_at AS TIMESTAMP), '%Y-%m') = '2025-08'
            GROUP BY order_id, order_total, subtotal_price, total_discounts, total_tax, shipping_price, refunds_total, created_at
            ORDER BY created_at
            """
            
            detail_df = conn.execute(detail_query).fetchdf()
            if not detail_df.empty:
                print(f"注文詳細（{len(detail_df)}件）:")
                print(detail_df.to_string(index=False))
                
                # 手動で合計を確認
                manual_total = detail_df['order_total'].sum()
                print(f"\n手動合計確認: ¥{manual_total:,.0f}")
            else:
                print("注文詳細が見つかりません")
            
        else:
            print("2025年8月のデータが見つかりません")
            
        conn.close()
        
    except Exception as e:
        print(f"エラー: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    calculate_correct_shopify_revenue()
