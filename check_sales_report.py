#!/usr/bin/env python3
"""
Shopify Analytics/Reports APIを使用して売上データを確認
"""
import os
import requests
from datetime import datetime
from dotenv import load_dotenv

# 環境変数を読み込み
load_dotenv()

def get_headers():
    token = os.getenv("SHOPIFY_ACCESS_TOKEN")
    if not token:
        raise ValueError("SHOPIFY_ACCESS_TOKEN 環境変数が設定されていません")
    return {"X-Shopify-Access-Token": token}

def get_base_url():
    shop_url = os.getenv("SHOPIFY_SHOP_URL")
    if not shop_url:
        raise ValueError("SHOPIFY_SHOP_URL 環境変数が設定されていません")
    return f"https://{shop_url}/admin/api/2024-10"

def check_reports_api():
    """Reports APIで売上データを確認"""
    print("=== Shopify Reports API 確認 ===")
    
    base_url = get_base_url()
    headers = get_headers()
    
    try:
        # Reports一覧を取得
        reports_url = f"{base_url}/reports.json"
        response = requests.get(reports_url, headers=headers)
        response.raise_for_status()
        reports_data = response.json()
        
        reports = reports_data.get("reports", [])
        print(f"利用可能なレポート数: {len(reports)}")
        
        # 売上関連のレポートを探す
        sales_reports = []
        for report in reports:
            name = report.get("name", "").lower()
            if any(keyword in name for keyword in ["sales", "revenue", "order", "financial"]):
                sales_reports.append(report)
                print(f"  - {report['name']} (ID: {report['id']})")
        
        # 最初の売上レポートの詳細を取得
        if sales_reports:
            report_id = sales_reports[0]['id']
            print(f"\\n=== レポート詳細 (ID: {report_id}) ===")
            
            report_detail_url = f"{base_url}/reports/{report_id}.json"
            detail_response = requests.get(report_detail_url, headers=headers)
            detail_response.raise_for_status()
            detail_data = detail_response.json()
            
            report_detail = detail_data.get("report", {})
            print(f"レポート名: {report_detail.get('name')}")
            print(f"カテゴリ: {report_detail.get('category')}")
            print(f"更新日: {report_detail.get('updated_at')}")
        
    except Exception as e:
        print(f"Reports API エラー: {e}")

def check_analytics_api():
    """Analytics APIで売上データを確認"""
    print("\\n=== Shopify Analytics API 確認 ===")
    
    base_url = get_base_url()
    headers = get_headers()
    
    try:
        # 2025年8月のAnalyticsデータを取得
        analytics_url = f"{base_url}/analytics/reports/orders.json?start_date=2025-08-01&end_date=2025-08-31"
        response = requests.get(analytics_url, headers=headers)
        
        if response.status_code == 200:
            analytics_data = response.json()
            print(f"Analyticsデータ: {analytics_data}")
        else:
            print(f"Analytics API 応答コード: {response.status_code}")
            print(f"エラーメッセージ: {response.text}")
        
    except Exception as e:
        print(f"Analytics API エラー: {e}")

def check_transactions():
    """Transactions APIで取引データを確認"""
    print("\\n=== Transactions 確認 ===")
    
    base_url = get_base_url()
    headers = get_headers()
    
    try:
        # 8月の取引を取得
        transactions_url = f"{base_url}/orders.json?status=any&limit=250&created_at_min=2025-08-01T00:00:00+09:00&created_at_max=2025-08-31T23:59:59+09:00&fields=id,total_price,created_at,financial_status,transactions"
        response = requests.get(transactions_url, headers=headers)
        response.raise_for_status()
        orders_data = response.json()
        
        orders = orders_data.get("orders", [])
        print(f"注文数: {len(orders)}")
        
        total_transactions_amount = 0
        for order in orders:
            transactions = order.get("transactions", [])
            order_transaction_total = 0
            
            for transaction in transactions:
                if transaction.get("status") == "success" and transaction.get("kind") in ["sale", "capture"]:
                    amount = float(transaction.get("amount", 0) or 0)
                    order_transaction_total += amount
            
            total_transactions_amount += order_transaction_total
            
            if order_transaction_total > 0:
                print(f"注文 {order['id']}: 取引合計 {order_transaction_total:,.0f}円 (注文合計: {float(order.get('total_price', 0)):,.0f}円)")
        
        print(f"\\n取引合計: {total_transactions_amount:,.0f}円")
        print(f"目標: 67,470円")
        print(f"差額: {67470 - total_transactions_amount:,.0f}円")
        
    except Exception as e:
        print(f"Transactions確認エラー: {e}")

def main():
    try:
        check_reports_api()
        check_analytics_api()
        check_transactions()
    except Exception as e:
        print(f"エラー: {e}")

if __name__ == "__main__":
    main()