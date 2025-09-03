#!/usr/bin/env python3
"""
Draft OrderとRegular Orderの関係を確認
"""
import os
import requests
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

def check_draft_order_details():
    """Draft Order 1218218426671の詳細を確認"""
    base_url = get_base_url()
    headers = get_headers()
    
    # Draft Orderの詳細取得
    draft_url = f"{base_url}/draft_orders/1218218426671.json"
    try:
        response = requests.get(draft_url, headers=headers)
        response.raise_for_status()
        draft_data = response.json()
        
        draft_order = draft_data.get("draft_order", {})
        print("=== Draft Order 詳細 ===")
        print(f"ID: {draft_order.get('id')}")
        print(f"作成日: {draft_order.get('created_at')}")
        print(f"完了日: {draft_order.get('completed_at')}")
        print(f"合計金額: {draft_order.get('total_price')}円")
        print(f"ステータス: {draft_order.get('status')}")
        print(f"関連注文ID: {draft_order.get('order_id')}")  # これが重要
        print(f"請求先: {draft_order.get('billing_address', {}).get('name', 'N/A')}")
        
        # もし関連注文IDがあれば、それを確認
        related_order_id = draft_order.get('order_id')
        if related_order_id:
            print(f"\\n=== 関連注文 {related_order_id} の確認 ===")
            order_url = f"{base_url}/orders/{related_order_id}.json"
            try:
                order_response = requests.get(order_url, headers=headers)
                order_response.raise_for_status()
                order_data = order_response.json()
                
                order = order_data.get("order", {})
                print(f"注文ID: {order.get('id')}")
                print(f"作成日: {order.get('created_at')}")
                print(f"合計金額: {order.get('total_price')}円")
                print(f"財務ステータス: {order.get('financial_status')}")
                print(f"注文番号: {order.get('order_number')}")
                
            except Exception as e:
                print(f"関連注文の取得エラー: {e}")
        
    except Exception as e:
        print(f"Draft Order取得エラー: {e}")

def check_regular_order_details():
    """Regular Order 6438413697327の詳細を確認"""
    base_url = get_base_url()
    headers = get_headers()
    
    # Regular Orderの詳細取得
    order_url = f"{base_url}/orders/6438413697327.json"
    try:
        response = requests.get(order_url, headers=headers)
        response.raise_for_status()
        order_data = response.json()
        
        order = order_data.get("order", {})
        print("\\n=== Regular Order 詳細 ===")
        print(f"ID: {order.get('id')}")
        print(f"作成日: {order.get('created_at')}")
        print(f"合計金額: {order.get('total_price')}円")
        print(f"財務ステータス: {order.get('financial_status')}")
        print(f"注文番号: {order.get('order_number')}")
        print(f"請求先: {order.get('billing_address', {}).get('name', 'N/A')}")
        print(f"ソース名: {order.get('source_name')}")
        
    except Exception as e:
        print(f"Regular Order取得エラー: {e}")

def main():
    print("=== Draft OrderとRegular Orderの関係確認 ===")
    check_draft_order_details()
    check_regular_order_details()

if __name__ == "__main__":
    main()