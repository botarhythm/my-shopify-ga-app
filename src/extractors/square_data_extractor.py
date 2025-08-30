#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Square決済データ取得スクリプト
Square Payments APIから決済データを取得し、CSVファイルとして出力します。

必要なライブラリのインストール:
pip install squareup pandas python-dotenv

または
pip install -r requirements.txt
"""

import os
import pandas as pd
from datetime import datetime, timedelta
from dotenv import load_dotenv
from square.client import Client

# 環境変数を読み込み
load_dotenv()

# Square設定
SQUARE_ACCESS_TOKEN = os.getenv('SQUARE_ACCESS_TOKEN')
SQUARE_LOCATION_ID = os.getenv('SQUARE_LOCATION_ID')
SQUARE_ENVIRONMENT = 'production'  # 本番環境でテスト

def get_square_client():
    """Squareクライアントを取得します。"""
    if not SQUARE_ACCESS_TOKEN:
        raise ValueError("SQUARE_ACCESS_TOKEN環境変数が設定されていません。")
    
    return Client(
        access_token=SQUARE_ACCESS_TOKEN,  # 正しい認証方法
        environment=SQUARE_ENVIRONMENT
    )

def get_square_payments():
    """Squareから決済データを取得します。"""
    print("決済データを取得中...")
    
    # 過去30日の期間を設定
    end_date = datetime.now()
    start_date = end_date - timedelta(days=30)
    
    # ISO 8601形式の日時文字列
    start_date_str = start_date.strftime('%Y-%m-%dT%H:%M:%SZ')
    end_date_str = end_date.strftime('%Y-%m-%dT%H:%M:%SZ')
    
    print(f"取得期間: {start_date.strftime('%Y年%m月%d日')} 〜 {end_date.strftime('%Y年%m月%d日')}")
    
    client = get_square_client()
    payments = []
    
    try:
        # 決済データを取得
        result = client.payments.list_payments(
            begin_time=start_date_str,
            end_time=end_date_str,
            location_id=SQUARE_LOCATION_ID
        )
        
        if result.is_success():
            payments = result.body.get('payments', [])
            print(f"取得した決済数: {len(payments)}")
        else:
            print(f"決済データ取得でエラー: {result.errors}")
            
    except Exception as e:
        print(f"決済データ取得中にエラーが発生しました: {e}")
    
    return payments

def process_payments_data(payments):
    """決済データを処理してDataFrameに変換します。"""
    if not payments:
        return pd.DataFrame()
    
    processed_payments = []
    
    for payment in payments:
        # ネストしたデータの安全な取得
        amount_money = payment.get('amount_money', {})
        card_details = payment.get('card_details', {})
        card = card_details.get('card', {}) if isinstance(card_details, dict) else {}
        refunded_money = payment.get('refunded_money', {})
        processing_fee = payment.get('processing_fee', {})
        total_money = payment.get('total_money', {})
        approved_money = payment.get('approved_money', {})
        
        payment_data = {
            'id': payment.get('id'),
            'created_at': payment.get('created_at'),
            'updated_at': payment.get('updated_at'),
            'amount_money_amount': amount_money.get('amount') if isinstance(amount_money, dict) else None,
            'amount_money_currency': amount_money.get('currency') if isinstance(amount_money, dict) else None,
            'status': payment.get('status'),
            'receipt_number': payment.get('receipt_number'),
            'order_id': payment.get('order_id'),
            'reference_id': payment.get('reference_id'),
            'customer_id': payment.get('customer_id'),
            'location_id': payment.get('location_id'),
            'source_type': payment.get('source_type'),
            'card_details_card_brand': card.get('card_brand') if isinstance(card, dict) else None,
            'card_details_card_last_4': card.get('last_4') if isinstance(card, dict) else None,
            'card_details_entry_method': card_details.get('entry_method') if isinstance(card_details, dict) else None,
            'card_details_status': card_details.get('status') if isinstance(card_details, dict) else None,
            'refunded_money_amount': refunded_money.get('amount') if isinstance(refunded_money, dict) else None,
            'refunded_money_currency': refunded_money.get('currency') if isinstance(refunded_money, dict) else None,
            'processing_fee_amount': processing_fee.get('amount') if isinstance(processing_fee, dict) else None,
            'processing_fee_currency': processing_fee.get('currency') if isinstance(processing_fee, dict) else None,
            'total_money_amount': total_money.get('amount') if isinstance(total_money, dict) else None,
            'total_money_currency': total_money.get('currency') if isinstance(total_money, dict) else None,
            'approved_money_amount': approved_money.get('amount') if isinstance(approved_money, dict) else None,
            'approved_money_currency': approved_money.get('currency') if isinstance(approved_money, dict) else None,
        }
        
        processed_payments.append(payment_data)
    
    return pd.DataFrame(processed_payments)

def main():
    """メイン処理を実行します。"""
    print("Square決済データ取得を開始します...")
    
    try:
        # 環境変数の確認
        if not SQUARE_ACCESS_TOKEN:
            print("エラー: SQUARE_ACCESS_TOKEN環境変数が設定されていません。")
            print("以下のコマンドで環境変数を設定してください:")
            print("set SQUARE_ACCESS_TOKEN=your_access_token_here")
            print("set SQUARE_LOCATION_ID=your_location_id_here")
            return
        
        if not SQUARE_LOCATION_ID:
            print("エラー: SQUARE_LOCATION_ID環境変数が設定されていません。")
            print("以下のコマンドで環境変数を設定してください:")
            print("set SQUARE_LOCATION_ID=your_location_id_here")
            return
        
        print(f"環境: {SQUARE_ENVIRONMENT}")
        print(f"ロケーションID: {SQUARE_LOCATION_ID}")
        
        # データ取得
        payments = get_square_payments()
        
        if not payments:
            print("決済データの取得に失敗しました。")
            return
        
        # データ処理
        print("\nデータを処理中...")
        
        payments_df = process_payments_data(payments)
        if not payments_df.empty:
            # 決済データをCSV出力
            payments_filename = f"square_payments_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
            payments_df.to_csv(payments_filename, index=False, encoding='utf-8-sig')
            print(f"決済データを {payments_filename} に保存しました。")
            print(f"決済データ件数: {len(payments_df)}")
            
            # 基本統計情報を表示
            print("\n基本統計情報:")
            if 'amount_money_amount' in payments_df.columns:
                total_amount = payments_df['amount_money_amount'].sum()
                currency = payments_df['amount_money_currency'].iloc[0] if not payments_df.empty else 'USD'
                print(f"総決済額: {total_amount:,.0f} {currency}")
                print(f"平均決済額: {payments_df['amount_money_amount'].mean():,.0f} {currency}")
            
            # ステータス別集計
            if 'status' in payments_df.columns:
                status_summary = payments_df['status'].value_counts()
                print("\nステータス別集計:")
                print(status_summary)
            
            # カードブランド別集計
            if 'card_details_card_brand' in payments_df.columns:
                brand_summary = payments_df['card_details_card_brand'].value_counts()
                print("\nカードブランド別集計:")
                print(brand_summary)
            
            print("\n決済データのプレビュー:")
            print(payments_df.head())
        
        print("\nデータ取得が完了しました。")
        
    except Exception as e:
        print(f"エラーが発生しました: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
