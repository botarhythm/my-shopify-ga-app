#!/usr/bin/env python3
"""
不足している¥6,860の売上を特定
"""
import os
import requests
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv
from src.connectors.shopify import fetch_orders_by_processed_range, fetch_orders_by_created_range

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

def check_different_financial_statuses():
    """異なる財務ステータスの注文を確認"""
    print("=== 異なる財務ステータスの注文確認 ===")
    
    statuses = ["pending", "authorized", "partially_paid", "paid", "partially_refunded", "refunded", "voided"]
    
    for status in statuses:
        try:
            df = fetch_orders_by_processed_range(
                start_iso="2025-08-01T00:00:00+09:00",
                end_iso="2025-08-31T23:59:59+09:00",
                financial_statuses=[status]
            )
            
            if not df.empty:
                order_count = df['order_id'].nunique()
                total_revenue = df.groupby('order_id')['order_total'].first().sum()
                print(f"{status}: {order_count}件, 合計 {total_revenue:,.0f}円")
            else:
                print(f"{status}: 0件")
                
        except Exception as e:
            print(f"{status} 確認エラー: {e}")

def check_created_vs_processed():
    """created_at vs processed_at の差異を確認"""
    print("\\n=== created_at vs processed_at 比較 ===")
    
    # created_at範囲
    df_created = fetch_orders_by_created_range(
        start_iso="2025-08-01T00:00:00+09:00",
        end_iso="2025-08-31T23:59:59+09:00",
        financial_statuses=["paid"]
    )
    
    # processed_at範囲
    df_processed = fetch_orders_by_processed_range(
        start_iso="2025-08-01T00:00:00+09:00",
        end_iso="2025-08-31T23:59:59+09:00",
        financial_statuses=["paid"]
    )
    
    created_orders = set(df_created['order_id'].unique()) if not df_created.empty else set()
    processed_orders = set(df_processed['order_id'].unique()) if not df_processed.empty else set()
    
    created_only = created_orders - processed_orders
    processed_only = processed_orders - created_orders
    
    print(f"created_at範囲のみ: {len(created_only)}件")
    print(f"processed_at範囲のみ: {len(processed_only)}件")
    
    if created_only:
        print("created_at範囲のみの注文:")
        for order_id in created_only:
            order_data = df_created[df_created['order_id'] == order_id].iloc[0]
            print(f"  注文ID: {order_id}, 金額: {order_data['order_total']:,.0f}円")
    
    if processed_only:
        print("processed_at範囲のみの注文:")
        for order_id in processed_only:
            order_data = df_processed[df_processed['order_id'] == order_id].iloc[0]
            print(f"  注文ID: {order_id}, 金額: {order_data['order_total']:,.0f}円")

def check_all_august_orders():
    """8月の全ての注文（ステータス不問）を確認"""
    print("\\n=== 8月の全注文（ステータス不問）===")
    
    base_url = get_base_url()
    headers = get_headers()
    
    # 全ステータスの注文を取得
    url = f"{base_url}/orders.json?status=any&limit=250&created_at_min=2025-08-01T00:00:00+09:00&created_at_max=2025-08-31T23:59:59+09:00"
    
    all_orders = []
    while True:
        try:
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            data = response.json()
            
            batch_orders = data.get("orders", [])
            all_orders.extend(batch_orders)
            
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
            print(f"全注文取得エラー: {e}")
            break
    
    print(f"全注文件数: {len(all_orders)}")
    
    # ステータス別集計
    status_summary = {}
    total_all = 0
    
    for order in all_orders:
        status = order.get('financial_status', 'unknown')
        total_price = float(order.get('total_price', 0) or 0)
        
        if status not in status_summary:
            status_summary[status] = {'count': 0, 'total': 0}
        
        status_summary[status]['count'] += 1
        status_summary[status]['total'] += total_price
        total_all += total_price
    
    print("\\nステータス別集計:")
    for status, data in status_summary.items():
        print(f"  {status}: {data['count']}件, {data['total']:,.0f}円")
    
    print(f"\\n全注文合計: {total_all:,.0f}円")
    print(f"目標: 67,470円")
    print(f"差額: {67470 - total_all:,.0f}円")

def main():
    try:
        check_different_financial_statuses()
        check_created_vs_processed()
        check_all_august_orders()
    except Exception as e:
        print(f"エラー: {e}")

if __name__ == "__main__":
    main()