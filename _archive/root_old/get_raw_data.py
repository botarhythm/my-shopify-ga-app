#!/usr/bin/env python3
"""
各プラットフォームから生データを正しく取得
"""
import os
import sys
from dotenv import load_dotenv

load_dotenv()
sys.path.append('.')

from src.connectors.shopify import fetch_orders_incremental
import pandas as pd
from datetime import datetime, timedelta

def main():
    print('=== 各プラットフォームから生データを正しく取得 ===')

    # 1. Shopifyから生データを取得
    print('1. Shopifyから生データを取得中...')
    start_date = '2025-07-01T00:00:00+09:00'

    try:
        df = fetch_orders_incremental(start_date)
        print(f'取得したレコード数: {len(df)}')
        
        if len(df) > 0:
            # 7月のデータのみフィルタリング
            df['date'] = pd.to_datetime(df['date'])
            july_df = df[(df['date'] >= '2025-07-01') & (df['date'] <= '2025-07-31') & (df['financial_status'] == 'paid')]
            
            print(f'7月のレコード数: {len(july_df)}')
            
            if len(july_df) > 0:
                print()
                print('=== Shopify生データの詳細 ===')
                print(f'期間: {july_df["date"].min().date()} 〜 {july_df["date"].max().date()}')
                print(f'ユニーク日数: {july_df["date"].nunique()}日')
                print(f'総注文数: {july_df["order_id"].nunique()}件')
                print(f'総ラインアイテム数: {len(july_df)}件')
                
                # 生データの詳細確認
                print()
                print('=== 生データの詳細確認 ===')
                print('カラム一覧:', list(july_df.columns))
                print()
                print('データ型:')
                print(july_df.dtypes)
                print()
                print('最初の5行:')
                print(july_df.head())
                
                # 売上関連の生データ
                print()
                print('=== 売上関連の生データ ===')
                print('total_price:', july_df['total_price'].sum())
                print('order_total:', july_df.groupby('order_id')['order_total'].first().sum())
                print('subtotal_price:', july_df['subtotal_price'].sum())
                print('total_line_items_price:', july_df['total_line_items_price'].sum())
                print('total_discounts:', july_df['total_discounts'].sum())
                print('total_tax:', july_df['total_tax'].sum())
                print('shipping_price:', july_df['shipping_price'].sum())
                print('refunds_total:', july_df['refunds_total'].sum())
                
                # 注文ごとの詳細
                print()
                print('=== 注文ごとの詳細（上位10件） ===')
                order_details = july_df.groupby('order_id').agg({
                    'total_price': 'sum',
                    'order_total': 'first',
                    'subtotal_price': 'sum',
                    'total_line_items_price': 'sum',
                    'total_discounts': 'sum',
                    'total_tax': 'sum',
                    'shipping_price': 'sum',
                    'refunds_total': 'sum',
                    'date': 'first'
                }).reset_index()
                
                order_details = order_details.sort_values('total_price', ascending=False).head(10)
                for _, order in order_details.iterrows():
                    print(f'注文ID: {order["order_id"]}, 日付: {order["date"].date()}')
                    print(f'  total_price: ¥{order["total_price"]:,.0f}')
                    print(f'  order_total: ¥{order["order_total"]:,.0f}')
                    print(f'  subtotal_price: ¥{order["subtotal_price"]:,.0f}')
                    print(f'  total_line_items_price: ¥{order["total_line_items_price"]:,.0f}')
                    print(f'  total_discounts: ¥{order["total_discounts"]:,.0f}')
                    print(f'  total_tax: ¥{order["total_tax"]:,.0f}')
                    print(f'  shipping_price: ¥{order["shipping_price"]:,.0f}')
                    print(f'  refunds_total: ¥{order["refunds_total"]:,.0f}')
                    print()
                
            else:
                print('7月のデータが取得できませんでした')
        else:
            print('データが取得できませんでした')
            
    except Exception as e:
        print(f'エラー: {e}')
        import traceback
        traceback.print_exc()

if __name__ == '__main__':
    main()
