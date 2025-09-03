#!/usr/bin/env python3
"""
Draft Orders（下書き注文）を確認して、不足している売上を特定する
"""
import os
import requests
import pandas as pd
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

def fetch_draft_orders_august():
    """2025年8月のDraft Ordersを取得"""
    base_url = get_base_url()
    url = f"{base_url}/draft_orders.json?status=completed&limit=250&created_at_min=2025-08-01T00:00:00+09:00&created_at_max=2025-08-31T23:59:59+09:00"
    
    headers = get_headers()
    draft_orders = []
    
    while True:
        try:
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            data = response.json()
            
            batch_orders = data.get("draft_orders", [])
            
            # 2025年8月の範囲内のみをフィルタリング
            for order in batch_orders:
                created_at = order.get("created_at", "")
                if "2025-08" in created_at:
                    draft_orders.append(order)
            
            # 次のページがあるかチェック
            link_header = response.headers.get("Link", "")
            if 'rel="next"' not in link_header:
                break
                
            # 次のURLを抽出
            for part in link_header.split(","):
                if 'rel="next"' in part:
                    start = part.find("<") + 1
                    end = part.find(">")
                    url = part[start:end]
                    break
            
        except Exception as e:
            print(f"Draft Orders API エラー: {e}")
            break
    
    return draft_orders

def main():
    print("=== Draft Orders 確認 ===")
    
    try:
        draft_orders = fetch_draft_orders_august()
        
        if not draft_orders:
            print("2025年8月のDraft Ordersは見つかりませんでした。")
            return
        
        print(f"Draft Orders件数: {len(draft_orders)}")
        
        total_draft_revenue = 0
        for order in draft_orders:
            total_price = float(order.get("total_price", 0) or 0)
            total_draft_revenue += total_price
            
            print(f"Draft Order ID: {order['id']}")
            print(f"  作成日: {order['created_at']}")
            print(f"  更新日: {order.get('updated_at', 'N/A')}")
            print(f"  完了日: {order.get('completed_at', 'N/A')}")
            print(f"  合計金額: {total_price:,.0f}円")
            print(f"  ステータス: {order.get('status', 'N/A')}")
            print()
        
        print(f"Draft Orders 合計売上: {total_draft_revenue:,.0f}円")
        print(f"通常Orders売上: 60,610円")
        print(f"合計予想売上: {60610 + total_draft_revenue:,.0f}円")
        print(f"目標売上: 67,470円")
        print(f"差額: {67470 - (60610 + total_draft_revenue):,.0f}円")
        
    except Exception as e:
        print(f"エラー: {e}")

if __name__ == "__main__":
    main()