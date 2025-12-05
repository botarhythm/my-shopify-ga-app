#!/usr/bin/env python3
"""
Squareから生データを正しく取得
"""
import os
import sys
from dotenv import load_dotenv

load_dotenv()
sys.path.append('.')

from src.connectors.square import fetch_payments
import pandas as pd
from datetime import datetime, timedelta

def main():
    print('=== Squareから生データを取得 ===')

    # Squareから生データを取得
    print('Squareから生データを取得中...')
    start_date = '2025-07-01T00:00:00+09:00'

    try:
        df = fetch_payments('2025-07-01', '2025-07-31')
        print(f'取得したレコード数: {len(df)}')
        
        if len(df) > 0:
            # 7月のデータのみフィルタリング
            df['date'] = pd.to_datetime(df['date'])
            july_df = df[(df['date'] >= '2025-07-01') & (df['date'] <= '2025-07-31') & (df['status'] == 'COMPLETED')]
            
            print(f'7月のレコード数: {len(july_df)}')
            
            if len(july_df) > 0:
                print()
                print('=== Square生データの詳細 ===')
                print(f'期間: {july_df["date"].min().date()} 〜 {july_df["date"].max().date()}')
                print(f'ユニーク日数: {july_df["date"].nunique()}日')
                print(f'総支払い数: {len(july_df)}件')
                
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
                print('amount:', july_df['amount'].sum())
                print('processing_fee:', july_df['processing_fee'].sum())
                
                # 支払いごとの詳細
                print()
                print('=== 支払いごとの詳細（上位10件） ===')
                payment_details = july_df.sort_values('amount', ascending=False).head(10)
                for _, payment in payment_details.iterrows():
                    print(f'支払いID: {payment["payment_id"]}, 日付: {payment["date"].date()}')
                    print(f'  amount: ¥{payment["amount"]:,.0f}')
                    print(f'  processing_fee: ¥{payment["processing_fee"]:,.0f}')
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
