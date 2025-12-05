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
    
    try:
        from square.client import Client
        return Client(
            access_token=SQUARE_ACCESS_TOKEN,
            environment=SQUARE_ENVIRONMENT
        )
    except ImportError:
        raise ImportError("squareupライブラリがインストールされていません。")

def get_square_payments():
    """Squareから決済データを取得します。"""
    print("決済データを取得中...")
    
    # 8月の期間を設定
    start_date = datetime(2025, 8, 1)
    end_date = datetime(2025, 8, 31)
    
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

def get_square_invoices():
    """Squareから請求書データを取得します。"""
    print("請求書データを取得中...")
    
    # 8月の期間を設定
    start_date = datetime(2025, 8, 1)
    end_date = datetime(2025, 8, 31)
    
    # ISO 8601形式の日時文字列
    start_date_str = start_date.strftime('%Y-%m-%dT%H:%M:%SZ')
    end_date_str = end_date.strftime('%Y-%m-%dT%H:%M:%SZ')
    
    print(f"請求書取得期間: {start_date.strftime('%Y年%m月%d日')} 〜 {end_date.strftime('%Y年%m月%d日')}")
    
    client = get_square_client()
    invoices = []
    
    try:
        # Invoices APIを使用して請求書データを取得
        result = client.invoices.list_invoices(
            location_id=SQUARE_LOCATION_ID,
            limit=500
        )
        
        if result.is_success():
            invoices = result.body.get('invoices', [])
            # 期間でフィルタリング
            filtered_invoices = []
            for invoice in invoices:
                created_at = invoice.get('created_at', '')
                if created_at:
                    invoice_date = datetime.fromisoformat(created_at.replace('Z', '+00:00'))
                    if start_date <= invoice_date.replace(tzinfo=None) <= end_date:
                        filtered_invoices.append(invoice)
            
            invoices = filtered_invoices
            print(f"取得した請求書数: {len(invoices)}")
        else:
            print(f"請求書データの取得に失敗: {result.errors}")
            
    except Exception as e:
        print(f"請求書データ取得エラー: {e}")
    
    return invoices

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
            'payment_method': payment.get('source_type'),
            'data_type': 'PAYMENT',
            'location_id': payment.get('location_id'),
            'merchant_id': payment.get('merchant_id'),
            'customer_id': payment.get('customer_id'),
            'total_money_amount': total_money.get('amount') if isinstance(total_money, dict) else None,
            'total_money_currency': total_money.get('currency') if isinstance(total_money, dict) else None,
            'approved_money_amount': approved_money.get('amount') if isinstance(approved_money, dict) else None,
            'approved_money_currency': approved_money.get('currency') if isinstance(approved_money, dict) else None,
            'processing_fee_amount': processing_fee.get('amount') if isinstance(processing_fee, dict) else None,
            'processing_fee_currency': processing_fee.get('currency') if isinstance(processing_fee, dict) else None,
            'refunded_money_amount': refunded_money.get('amount') if isinstance(refunded_money, dict) else None,
            'refunded_money_currency': refunded_money.get('currency') if isinstance(refunded_money, dict) else None,
            'tip_money_amount': payment.get('tip_money', {}).get('amount') if isinstance(payment.get('tip_money'), dict) else None,
            'tip_money_currency': payment.get('tip_money', {}).get('currency') if isinstance(payment.get('tip_money'), dict) else None,
            'card_brand': card.get('card_brand'),
            'card_last_4': card.get('last_4'),
            'card_exp_month': card.get('exp_month'),
            'card_exp_year': card.get('exp_year'),
            'card_type': card.get('card_type'),
            'entry_method': card_details.get('entry_method'),
            'receipt_url': payment.get('receipt_url'),
            'note': payment.get('note')
        }
        
        processed_payments.append(payment_data)
    
    return pd.DataFrame(processed_payments)

def process_invoices_data(invoices):
    """請求書データを処理してDataFrameに変換します。"""
    if not invoices:
        return pd.DataFrame()
    
    processed_invoices = []
    
    for invoice in invoices:
        # 請求書の基本情報
        invoice_data = {
            'id': invoice.get('id'),
            'created_at': invoice.get('created_at'),
            'updated_at': invoice.get('updated_at'),
            'amount_money_amount': invoice.get('amount_money', {}).get('amount') if isinstance(invoice.get('amount_money'), dict) else None,
            'amount_money_currency': invoice.get('amount_money', {}).get('currency') if isinstance(invoice.get('amount_money'), dict) else 'JPY',
            'status': invoice.get('status'),
            'payment_method': 'INVOICE',
            'data_type': 'INVOICE',
            'order_id': invoice.get('order_id'),
            'location_id': invoice.get('location_id'),
            'merchant_id': invoice.get('merchant_id'),
            'customer_id': invoice.get('customer_id'),
            'invoice_number': invoice.get('invoice_number'),
            'title': invoice.get('title'),
            'description': invoice.get('description'),
            'scheduled_at': invoice.get('scheduled_at'),
            'public_url': invoice.get('public_url'),
            'next_payment_amount_money': invoice.get('next_payment_amount_money', {}).get('amount') if isinstance(invoice.get('next_payment_amount_money'), dict) else None,
            'next_payment_amount_currency': invoice.get('next_payment_amount_money', {}).get('currency') if isinstance(invoice.get('next_payment_amount_money'), dict) else None,
            'primary_recipient_email': invoice.get('primary_recipient', {}).get('email_address') if isinstance(invoice.get('primary_recipient'), dict) else None,
            'primary_recipient_name': invoice.get('primary_recipient', {}).get('given_name') if isinstance(invoice.get('primary_recipient'), dict) else None,
            'payment_requests': len(invoice.get('payment_requests', [])),
            'delivery_method': invoice.get('delivery_method'),
            'sale_or_service_date': invoice.get('sale_or_service_date'),
            'store_payment_method_enabled': invoice.get('store_payment_method_enabled'),
            'custom_fields': len(invoice.get('custom_fields', [])),
            'payment_conditions': invoice.get('payment_conditions'),
            'accepted_payment_methods': ', '.join(invoice.get('accepted_payment_methods', [])),
            'version': invoice.get('version'),
            'recipients': len(invoice.get('recipients', [])),
            'payment_requests_count': len(invoice.get('payment_requests', [])),
            'custom_fields_count': len(invoice.get('custom_fields', [])),
            'recipients_count': len(invoice.get('recipients', []))
        }
        
        processed_invoices.append(invoice_data)
    
    return pd.DataFrame(processed_invoices)

def main():
    """メイン実行関数"""
    print("Square決済データ取得を開始します...")
    
    # 環境変数チェック
    if not SQUARE_ACCESS_TOKEN:
        print("❌ SQUARE_ACCESS_TOKEN環境変数が設定されていません")
        print("以下のコマンドで環境変数を設定してください:")
        print("set SQUARE_ACCESS_TOKEN=your_access_token_here")
        return
    
    if not SQUARE_LOCATION_ID:
        print("❌ SQUARE_LOCATION_ID環境変数が設定されていません")
        print("以下のコマンドで環境変数を設定してください:")
        print("set SQUARE_LOCATION_ID=your_location_id_here")
        return
    
    try:
        # 実際のAPIからデータを取得
        payments = get_square_payments()
        invoices = get_square_invoices()
        
        if not payments and not invoices:
            print("❌ 決済データと請求書データの取得に失敗しました")
            return
        
        # データを処理
        df_payments = process_payments_data(payments)
        df_invoices = process_invoices_data(invoices)
        
        # 決済データと請求書データを結合
        df_combined = pd.concat([df_payments, df_invoices], ignore_index=True)
        
        if df_combined.empty:
            print("❌ データ処理に失敗しました")
            return
        
        # CSVファイルとして保存
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f'square_combined_{timestamp}.csv'
        filepath = os.path.join('data', 'raw', filename)
        
        df_combined.to_csv(filepath, index=False, encoding='utf-8')
        print(f"✅ 決済・請求書データを {filename} に保存しました")
        print(f"データ件数: {len(df_combined)} (決済: {len(df_payments)}, 請求書: {len(df_invoices)})")
        
        # 基本統計情報を表示
        print("\n📊 基本統計情報:")
        if 'amount_money_amount' in df_combined.columns:
            total_amount = df_combined['amount_money_amount'].sum()
            currency = df_combined['amount_money_currency'].iloc[0] if not df_combined.empty else 'JPY'
            print(f"総売上額: {total_amount:,.0f} {currency}")
            print(f"平均売上額: {df_combined['amount_money_amount'].mean():,.0f} {currency}")
        
        # データタイプ別集計
        if 'data_type' in df_combined.columns:
            type_summary = df_combined['data_type'].value_counts()
            print("\n📈 データタイプ別集計:")
            print(type_summary)
        
        # ステータス別集計
        if 'status' in df_combined.columns:
            status_summary = df_combined['status'].value_counts()
            print("\n📈 ステータス別集計:")
            print(status_summary)
        
        # 決済方法別集計
        if 'payment_method' in df_combined.columns:
            method_summary = df_combined['payment_method'].value_counts()
            print("\n💳 決済方法別集計:")
            print(method_summary)
        
        # データのプレビュー
        print("\n決済・請求書データのプレビュー:")
        print(df_combined[['id', 'created_at', 'amount_money_amount', 'status', 'data_type']].head())
        
    except Exception as e:
        print(f"❌ データ取得に失敗しました: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
