#!/usr/bin/env python3
"""
Shopify 7月データの根本的修正
スクリーンショットと完全に一致するデータを取得
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
    print('=== Shopify 7月データの根本的修正 ===')
    
    # 1. 7月1日から8月1日までのデータを取得（created_atベース）
    start_date = '2025-07-01T00:00:00+09:00'
    
    try:
        print('7月データを再取得中...')
        df = fetch_orders_incremental(start_date)
        
        print(f'取得したレコード数: {len(df)}')
        
        if len(df) > 0:
            # 7月のデータのみフィルタリング
            df['date'] = pd.to_datetime(df['date'])
            july_df = df[(df['date'] >= '2025-07-01') & (df['date'] <= '2025-07-31') & (df['financial_status'] == 'paid')]
            
            print(f'7月のレコード数: {len(july_df)}')
            
            if len(july_df) > 0:
                # 7月の詳細分析
                print()
                print('=== 7月データの詳細分析 ===')
                print(f'期間: {july_df["date"].min().date()} 〜 {july_df["date"].max().date()}')
                print(f'ユニーク日数: {july_df["date"].nunique()}日')
                print(f'総注文数: {july_df["order_id"].nunique()}件')
                print(f'総ラインアイテム数: {len(july_df)}件')
                
                # 売上計算（複数の方法で比較）
                lineitem_total = july_df['total_price'].sum()
                order_total = july_df.groupby('order_id')['order_total'].first().sum()
                
                print()
                print('=== 売上計算の比較 ===')
                print(f'ラインアイテム合計: ¥{lineitem_total:,.0f}')
                print(f'注文合計: ¥{order_total:,.0f}')
                
                # 日別データ
                daily_summary = july_df.groupby('date').agg({
                    'order_id': 'nunique',
                    'total_price': 'sum',
                    'order_total': 'first'
                }).reset_index()
                
                print()
                print('=== 日別データ ===')
                for _, day in daily_summary.iterrows():
                    print(f'{day["date"].date()}: {day["order_id"]}件, ラインアイテム: ¥{day["total_price"]:,.0f}, 注文合計: ¥{day["order_total"]:,.0f}')
                
                # スクリーンショットとの比較
                screenshot_amount = 67470
                
                print()
                print('=== スクリーンショットとの比較 ===')
                print(f'スクリーンショット: ¥{screenshot_amount:,}')
                print(f'ラインアイテム合計: ¥{lineitem_total:,.0f} (差: {lineitem_total-screenshot_amount:+,.0f})')
                print(f'注文合計: ¥{order_total:,.0f} (差: {order_total-screenshot_amount:+,.0f})')
                
                # 最も近い値を特定
                lineitem_diff = abs(lineitem_total - screenshot_amount)
                order_diff = abs(order_total - screenshot_amount)
                
                if lineitem_diff < order_diff:
                    best_method = 'ラインアイテム合計'
                    best_amount = lineitem_total
                else:
                    best_method = '注文合計'
                    best_amount = order_total
                
                print()
                print(f'=== 最適な集計方法 ===')
                print(f'推奨方法: {best_method}')
                print(f'推奨金額: ¥{best_amount:,.0f}')
                print(f'スクリーンショットとの差: {best_amount-screenshot_amount:+,.0f} ({((best_amount-screenshot_amount)/screenshot_amount*100):+.1f}%)')
                
                # 7月29日〜31日のデータ確認
                july_end = july_df[july_df['date'] >= '2025-07-29']
                if len(july_end) > 0:
                    print()
                    print('=== 7月29日〜31日のデータ ===')
                    end_summary = july_end.groupby('date').agg({
                        'order_id': 'nunique',
                        'total_price': 'sum'
                    }).reset_index()
                    for _, day in end_summary.iterrows():
                        print(f'{day["date"].date()}: {day["order_id"]}件, ¥{day["total_price"]:,.0f}')
                else:
                    print()
                    print('=== 7月29日〜31日のデータ ===')
                    print('データなし - これが主要な問題の可能性')
            
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
