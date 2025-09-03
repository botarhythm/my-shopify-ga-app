#!/usr/bin/env python3
"""
取引データの詳細分析
"""
import os
import requests
import json
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

def analyze_order_transactions():
    """各注文の取引詳細を分析"""
    print("=== 注文別取引詳細分析 ===")
    
    base_url = get_base_url()
    headers = get_headers()
    
    try:
        # 8月の注文を取得（取引データ含む）
        orders_url = f"{base_url}/orders.json?status=any&limit=250&created_at_min=2025-08-01T00:00:00+09:00&created_at_max=2025-08-31T23:59:59+09:00"
        response = requests.get(orders_url, headers=headers)
        response.raise_for_status()
        orders_data = response.json()
        
        orders = orders_data.get("orders", [])
        print(f"注文数: {len(orders)}")
        
        total_order_amount = 0
        total_successful_transactions = 0
        
        for i, order in enumerate(orders, 1):
            order_id = order['id']
            total_price = float(order.get('total_price', 0) or 0)
            financial_status = order.get('financial_status')
            
            total_order_amount += total_price
            
            print(f"\\n--- 注文 {i}: {order_id} ---")
            print(f"合計金額: {total_price:,.0f}円")
            print(f"財務ステータス: {financial_status}")
            print(f"作成日: {order.get('created_at')}")
            print(f"支払い方法: {order.get('payment_gateway_names', [])}")
            print(f"処理日: {order.get('processed_at', 'N/A')}")
            
            # 個別の注文の取引詳細を取得
            order_detail_url = f"{base_url}/orders/{order_id}.json"
            detail_response = requests.get(order_detail_url, headers=headers)
            
            if detail_response.status_code == 200:
                detail_data = detail_response.json()
                order_detail = detail_data.get("order", {})
                transactions = order_detail.get("transactions", [])
                
                print(f"取引数: {len(transactions)}")
                
                order_transaction_total = 0
                for j, transaction in enumerate(transactions, 1):
                    tx_amount = float(transaction.get("amount", 0) or 0)
                    tx_kind = transaction.get("kind")
                    tx_status = transaction.get("status")
                    tx_gateway = transaction.get("gateway")
                    
                    print(f"  取引{j}: {tx_kind} - {tx_status} - {tx_amount:,.0f}円 ({tx_gateway})")
                    
                    if tx_status == "success" and tx_kind in ["sale", "capture", "authorization"]:
                        order_transaction_total += tx_amount
                
                total_successful_transactions += order_transaction_total
                print(f"成功取引合計: {order_transaction_total:,.0f}円")
                
                # 支払い詳細情報
                payment_details = order_detail.get("payment_details", {})
                if payment_details:
                    print(f"支払い詳細:")
                    for key, value in payment_details.items():
                        print(f"  {key}: {value}")
            else:
                print(f"注文詳細取得エラー: {detail_response.status_code}")
        
        print(f"\\n=== 総合計 ===")
        print(f"注文合計金額: {total_order_amount:,.0f}円")
        print(f"成功取引合計: {total_successful_transactions:,.0f}円")
        print(f"目標金額: 67,470円")
        print(f"注文との差額: {67470 - total_order_amount:,.0f}円")
        print(f"取引との差額: {67470 - total_successful_transactions:,.0f}円")
        
    except Exception as e:
        print(f"分析エラー: {e}")

def main():
    try:
        analyze_order_transactions()
    except Exception as e:
        print(f"エラー: {e}")

if __name__ == "__main__":
    main()