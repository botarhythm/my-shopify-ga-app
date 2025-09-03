#!/usr/bin/env python3
"""
2025年8月の詳細な売上分析
"""
import os
import requests
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv
from src.connectors.shopify import fetch_orders_by_processed_range

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

def analyze_august_orders():
    """2025年8月の注文を詳細分析"""
    print("=== 2025年8月 詳細売上分析 ===")
    
    # 1. 通常の注文データ（processed_at範囲）
    df = fetch_orders_by_processed_range(
        start_iso="2025-08-01T00:00:00+09:00",
        end_iso="2025-08-31T23:59:59+09:00",
        financial_statuses=["paid"]
    )
    
    if not df.empty:
        print(f"通常注文データ件数: {len(df)} 行")
        print(f"ユニーク注文数: {df['order_id'].nunique()}")
        
        # 注文単位での売上計算
        order_summary = df.groupby('order_id').agg({
            'order_total': 'first',
            'subtotal_price': 'first',
            'total_discounts': 'first',
            'total_tax': 'first',
            'shipping_price': 'first',
            'total_tip': 'first',
            'refunds_total': 'first',
            'created_at': 'first',
            'financial_status': 'first'
        }).reset_index()
        
        total_order_total = order_summary['order_total'].sum()
        total_subtotal = order_summary['subtotal_price'].sum()
        total_discounts = order_summary['total_discounts'].sum()
        total_tax = order_summary['total_tax'].sum()
        total_shipping = order_summary['shipping_price'].sum()
        total_tip = order_summary['total_tip'].sum()
        total_refunds = order_summary['refunds_total'].sum()
        
        print(f"\\n=== 通常注文 内訳 ===")
        print(f"注文合計 (order_total): {total_order_total:,.0f}円")
        print(f"小計 (subtotal_price): {total_subtotal:,.0f}円")
        print(f"割引 (total_discounts): {total_discounts:,.0f}円")
        print(f"税金 (total_tax): {total_tax:,.0f}円")
        print(f"送料 (shipping_price): {total_shipping:,.0f}円")
        print(f"チップ (total_tip): {total_tip:,.0f}円")
        print(f"返金 (refunds_total): {total_refunds:,.0f}円")
        
        # 純売上の計算パターン
        pure_revenue_1 = total_subtotal - total_discounts + total_tax + total_shipping + total_tip - total_refunds
        pure_revenue_2 = total_order_total - total_refunds
        
        print(f"\\n=== 純売上計算 ===")
        print(f"方式1 (小計-割引+税+送料+チップ-返金): {pure_revenue_1:,.0f}円")
        print(f"方式2 (注文合計-返金): {pure_revenue_2:,.0f}円")
    
    # 2. Draft Order分析
    print(f"\\n=== Draft Order ===")
    print(f"Draft Order売上: 4,982円")
    
    # 3. 合計
    regular_revenue = total_order_total if not df.empty else 0
    draft_revenue = 4982
    total_revenue = regular_revenue + draft_revenue
    
    print(f"\\n=== 総合計 ===")
    print(f"通常注文: {regular_revenue:,.0f}円")
    print(f"Draft Order: {draft_revenue:,.0f}円")
    print(f"合計: {total_revenue:,.0f}円")
    print(f"目標: 67,470円")
    print(f"差額: {67470 - total_revenue:,.0f}円")
    
    # 4. 個別注文の詳細表示（上位10件）
    if not df.empty:
        print(f"\\n=== 高額注文 TOP10 ===")
        top_orders = order_summary.nlargest(10, 'order_total')
        for _, order in top_orders.iterrows():
            print(f"注文ID: {order['order_id']}")
            print(f"  作成日: {order['created_at']}")
            print(f"  注文合計: {order['order_total']:,.0f}円")
            print(f"  返金: {order['refunds_total']:,.0f}円")
            print(f"  ステータス: {order['financial_status']}")
            print()

def main():
    try:
        analyze_august_orders()
    except Exception as e:
        print(f"エラー: {e}")

if __name__ == "__main__":
    main()