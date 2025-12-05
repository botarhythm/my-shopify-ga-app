#!/usr/bin/env python3
"""
最新8月データ確認スクリプト
"""

import pandas as pd
import os

def check_latest_data():
    """最新の8月データを確認"""
    print("=== 最新8月データ確認 ===")
    
    # 最新のShopify注文データを読み込み
    raw_dir = "data/raw"
    shopify_files = [f for f in os.listdir(raw_dir) if f.startswith("shopify_orders_202508")]
    latest_file = max(shopify_files)
    
    print(f"最新ファイル: {latest_file}")
    
    # データ読み込み
    orders_df = pd.read_csv(os.path.join(raw_dir, latest_file))
    
    # 基本統計
    total_orders = len(orders_df)
    total_revenue = orders_df['total_price'].sum()
    avg_order_value = orders_df['total_price'].mean()
    
    print(f"\n📊 最新データ統計")
    print(f"注文数: {total_orders}件")
    print(f"総売上: ¥{total_revenue:,}")
    print(f"平均注文額: ¥{avg_order_value:,.0f}")
    print(f"期間: {orders_df['created_at'].min()[:10]} 〜 {orders_df['created_at'].max()[:10]}")
    
    # 日別売上分析
    orders_df['created_at'] = pd.to_datetime(orders_df['created_at'])
    orders_df['date'] = orders_df['created_at'].dt.date
    daily_sales = orders_df.groupby('date')['total_price'].sum().reset_index()
    
    print(f"\n📈 日別売上トップ5")
    top_days = daily_sales.nlargest(5, 'total_price')
    for _, row in top_days.iterrows():
        print(f"  {row['date'].strftime('%m/%d')}: ¥{row['total_price']:,}")
    
    return {
        'total_orders': total_orders,
        'total_revenue': total_revenue,
        'avg_order_value': avg_order_value,
        'daily_sales': daily_sales
    }

if __name__ == "__main__":
    check_latest_data()
