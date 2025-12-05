#!/usr/bin/env python3
"""
Shopify 7月データの再取得と分析
"""
import os
import sys
from dotenv import load_dotenv

load_dotenv()
sys.path.append('.')

from src.connectors.shopify import fetch_orders_by_paid_range
import pandas as pd

def main():
    print('=== Shopify APIから7月データを再取得 ===')
    
    # 7月1日〜31日のデータを取得（created_atベース）
    start_iso = '2025-07-01T00:00:00+09:00'
    end_iso = '2025-07-31T23:59:59+09:00'
    
    try:
        # created_atベースで取得
        from src.connectors.shopify import fetch_orders_incremental
        df = fetch_orders_incremental('2025-07-01T00:00:00+09:00')
        
        # 7月のデータのみフィルタリング
        df['date'] = pd.to_datetime(df['date'])
        july_df = df[(df['date'] >= '2025-07-01') & (df['date'] <= '2025-07-31') & (df['financial_status'] == 'paid')]
        
        print(f'取得したレコード数: {len(df)}')
        print(f'7月のレコード数: {len(july_df)}')
        
        if len(july_df) > 0:
            # 7月の集計
            july_summary = july_df.groupby('order_id').agg({
                'total_price': 'sum',
                'order_total': 'first',
                'date': 'first'
            }).reset_index()
            
            print()
            print('=== APIから取得した7月データ（フィルタリング後） ===')
            print(f'注文数: {len(july_summary)}件')
            print(f'ラインアイテム合計: ¥{july_df["total_price"].sum():,.0f}')
            print(f'注文合計の合計: ¥{july_summary["order_total"].sum():,.0f}')
            print(f'期間: {july_df["date"].min().date()} 〜 {july_df["date"].max().date()}')
            
            # 上位10注文
            top_orders = july_summary.nlargest(10, 'total_price')
            print()
            print('=== 上位10注文（APIデータ） ===')
            for _, order in top_orders.iterrows():
                print(f'注文ID: {order["order_id"]}, ラインアイテム合計: ¥{order["total_price"]:,.0f}, 注文合計: ¥{order["order_total"]:,.0f}, 日付: {order["date"].date()}')
            
            # 日別集計
            daily_summary = july_df.groupby('date').agg({
                'order_id': 'nunique',
                'total_price': 'sum'
            }).reset_index()
            
            print()
            print('=== 日別集計（APIデータ） ===')
            for _, day in daily_summary.iterrows():
                print(f'{day["date"].date()}: {day["order_id"]}件, ¥{day["total_price"]:,.0f}')
            
            # スクリーンショットとの比較
            screenshot_amount = 67470
            api_amount = july_df["total_price"].sum()
            difference = api_amount - screenshot_amount
            
            print()
            print('=== スクリーンショットとの比較 ===')
            print(f'スクリーンショット: ¥{screenshot_amount:,}')
            print(f'APIデータ: ¥{api_amount:,.0f}')
            print(f'差: ¥{difference:,.0f}')
            print(f'差の割合: {difference/screenshot_amount*100:+.1f}%')
        
        else:
            print('7月のデータが取得できませんでした')
            
    except Exception as e:
        print(f'エラー: {e}')
        import traceback
        traceback.print_exc()

if __name__ == '__main__':
    main()
