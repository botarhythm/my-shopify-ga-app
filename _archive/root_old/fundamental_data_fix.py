#!/usr/bin/env python3
"""
Shopify データの根本的修正 - 完全なデータ取得
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
import duckdb

def main():
    print('=== Shopify データの根本的修正 ===')
    
    # 1. データベーススキーマの初期化
    print('データベーススキーマを初期化中...')
    db_path = os.getenv('DUCKDB_PATH', './data/duckdb/commerce_fresh.duckdb')
    
    # スキーマ初期化
    import subprocess
    result = subprocess.run(['python', 'scripts/bootstrap_duckdb.py'], capture_output=True, text=True)
    if result.returncode != 0:
        print(f'スキーマ初期化エラー: {result.stderr}')
        return
    print('スキーマ初期化完了')
    
    # 2. より広範囲な期間でデータを取得
    # 2023年1月から現在まで（3年間）
    start_date = '2023-01-01T00:00:00+09:00'
    
    try:
        print('広範囲な期間でデータを取得中...')
        df = fetch_orders_incremental(start_date)
        
        print(f'取得したレコード数: {len(df)}')
        
        if len(df) > 0:
            # データをデータベースに保存
            con = duckdb.connect(db_path)
            
            # core_shopifyテーブルにデータを挿入
            print('データベースに保存中...')
            con.execute("DELETE FROM core_shopify")  # 既存データをクリア
            
            # データを挿入（NaN値を適切に処理）
            for _, row in df.iterrows():
                # NaN値をNoneに変換
                def safe_value(val):
                    if pd.isna(val):
                        return None
                    return val
                
                con.execute("""
                    INSERT INTO core_shopify VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, [
                    safe_value(row['date']), safe_value(row['order_id']), safe_value(row['lineitem_id']), 
                    safe_value(row['product_id']), safe_value(row['variant_id']), safe_value(row['sku']), 
                    safe_value(row['title']), safe_value(row['qty']), safe_value(row['price']), 
                    safe_value(row['order_total']), safe_value(row['created_at']), safe_value(row['currency']), 
                    safe_value(row['total_price']), safe_value(row['subtotal_price']), safe_value(row['total_line_items_price']), 
                    safe_value(row['total_discounts']), safe_value(row['total_tax']), safe_value(row['total_tip']), 
                    safe_value(row['shipping_price']), safe_value(row['shipping_lines']), safe_value(row['tax_lines']), 
                    safe_value(row['financial_status']), safe_value(row['cancelled_at']), safe_value(row['refunds_total'])
                ])
            
            con.close()
            
            # 3. 7月データの詳細分析
            df['date'] = pd.to_datetime(df['date'])
            july_df = df[(df['date'] >= '2025-07-01') & (df['date'] <= '2025-07-31') & (df['financial_status'] == 'paid')]
            
            print()
            print('=== 修正後の7月データ分析 ===')
            print(f'期間: {july_df["date"].min().date()} 〜 {july_df["date"].max().date()}')
            print(f'ユニーク日数: {july_df["date"].nunique()}日')
            print(f'総注文数: {july_df["order_id"].nunique()}件')
            print(f'総ラインアイテム数: {len(july_df)}件')
            
            # 売上計算
            lineitem_total = july_df['total_price'].sum()
            order_total = july_df.groupby('order_id')['order_total'].first().sum()
            
            print()
            print('=== 売上計算 ===')
            print(f'ラインアイテム合計: ¥{lineitem_total:,.0f}')
            print(f'注文合計: ¥{order_total:,.0f}')
            
            # スクリーンショットとの比較
            screenshot_amount = 67470
            
            print()
            print('=== スクリーンショットとの比較 ===')
            print(f'スクリーンショット: ¥{screenshot_amount:,}')
            print(f'ラインアイテム合計: ¥{lineitem_total:,.0f} (差: {lineitem_total-screenshot_amount:+,.0f})')
            print(f'注文合計: ¥{order_total:,.0f} (差: {order_total-screenshot_amount:+,.0f})')
            
            # 日別データ
            daily_summary = july_df.groupby('date').agg({
                'order_id': 'nunique',
                'total_price': 'sum'
            }).reset_index()
            
            print()
            print('=== 日別データ ===')
            for _, day in daily_summary.iterrows():
                print(f'{day["date"].date()}: {day["order_id"]}件, ¥{day["total_price"]:,.0f}')
            
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
                print('データなし')
            
            # 4. 全期間のデータ確認
            print()
            print('=== 全期間のデータ確認 ===')
            print(f'総レコード数: {len(df)}')
            print(f'期間: {df["date"].min().date()} 〜 {df["date"].max().date()}')
            print(f'総注文数: {df["order_id"].nunique()}件')
            print(f'総売上: ¥{df["total_price"].sum():,.0f}')
            
        else:
            print('データが取得できませんでした')
            
    except Exception as e:
        print(f'エラー: {e}')
        import traceback
        traceback.print_exc()

if __name__ == '__main__':
    main()
