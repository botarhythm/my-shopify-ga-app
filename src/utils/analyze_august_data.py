#!/usr/bin/env python3
"""
8月売上データ分析スクリプト
"""

import pandas as pd
import os
from datetime import datetime
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots

def analyze_august_data():
    """8月の売上データを分析"""
    print("=== 8月売上データ分析 ===")
    
    # 最新のShopify注文データを読み込み
    raw_dir = "data/raw"
    shopify_files = [f for f in os.listdir(raw_dir) if f.startswith("shopify_orders_202508")]
    latest_file = max(shopify_files)
    
    print(f"分析対象ファイル: {latest_file}")
    
    # データ読み込み
    orders_df = pd.read_csv(os.path.join(raw_dir, latest_file))
    
    # 日付列の処理
    orders_df['created_at'] = pd.to_datetime(orders_df['created_at'])
    orders_df['date'] = orders_df['created_at'].dt.date
    
    # 基本統計
    total_orders = len(orders_df)
    total_revenue = orders_df['total_price'].sum()
    avg_order_value = orders_df['total_price'].mean()
    date_range = f"{orders_df['created_at'].min().strftime('%Y-%m-%d')} 〜 {orders_df['created_at'].max().strftime('%Y-%m-%d')}"
    
    print(f"\n📊 基本統計")
    print(f"注文数: {total_orders}件")
    print(f"総売上: ¥{total_revenue:,}")
    print(f"平均注文額: ¥{avg_order_value:,.0f}")
    print(f"期間: {date_range}")
    
    # 日別売上分析
    daily_sales = orders_df.groupby('date')['total_price'].sum().reset_index()
    daily_sales['date'] = pd.to_datetime(daily_sales['date'])
    
    print(f"\n📈 日別売上トップ5")
    top_days = daily_sales.nlargest(5, 'total_price')
    for _, row in top_days.iterrows():
        print(f"  {row['date'].strftime('%m/%d')}: ¥{row['total_price']:,}")
    
    # 商品別分析
    if 'line_items' in orders_df.columns:
        # line_itemsがJSON形式の場合の処理
        import json
        product_sales = {}
        
        for _, order in orders_df.iterrows():
            try:
                line_items = json.loads(order['line_items'])
                for item in line_items:
                    product_name = item.get('title', 'Unknown')
                    quantity = item.get('quantity', 0)
                    price = item.get('price', 0)
                    total = quantity * float(price)
                    
                    if product_name in product_sales:
                        product_sales[product_name]['quantity'] += quantity
                        product_sales[product_name]['revenue'] += total
                    else:
                        product_sales[product_name] = {
                            'quantity': quantity,
                            'revenue': total
                        }
            except:
                continue
        
        if product_sales:
            product_df = pd.DataFrame([
                {'product': k, 'quantity': v['quantity'], 'revenue': v['revenue']}
                for k, v in product_sales.items()
            ])
            
            print(f"\n🏆 商品別売上トップ5")
            top_products = product_df.nlargest(5, 'revenue')
            for _, row in top_products.iterrows():
                print(f"  {row['product']}: ¥{row['revenue']:,} ({row['quantity']}個)")
    
    # レポート生成
    report_content = f"""
# 📊 8月売上データ分析レポート
生成日時: {datetime.now().strftime('%Y年%m月%d日 %H:%M:%S')}

## 📈 基本統計
- **注文数**: {total_orders}件
- **総売上**: ¥{total_revenue:,}
- **平均注文額**: ¥{avg_order_value:,.0f}
- **分析期間**: {date_range}

## 📅 日別売上トップ5
"""
    
    for _, row in top_days.iterrows():
        report_content += f"- {row['date'].strftime('%m/%d')}: ¥{row['total_price']:,}\n"
    
    if 'product_sales' in locals() and product_sales:
        report_content += "\n## 🏆 商品別売上トップ5\n"
        for _, row in top_products.iterrows():
            report_content += f"- {row['product']}: ¥{row['revenue']:,} ({row['quantity']}個)\n"
    
    report_content += f"""
## 📊 分析サマリー
- 8月の総売上は¥{total_revenue:,}で、{total_orders}件の注文がありました
- 平均注文額は¥{avg_order_value:,.0f}です
- 最も売上が高かった日は{top_days.iloc[0]['date'].strftime('%m月%d日')}で¥{top_days.iloc[0]['total_price']:,}でした

---
*このレポートは自動生成されました*
"""
    
    # レポート保存
    report_filename = f"data/reports/august_sales_analysis_{datetime.now().strftime('%Y%m%d_%H%M%S')}.md"
    os.makedirs("data/reports", exist_ok=True)
    
    with open(report_filename, 'w', encoding='utf-8') as f:
        f.write(report_content)
    
    print(f"\n✅ 分析レポートを {report_filename} に保存しました")
    
    return {
        'total_orders': total_orders,
        'total_revenue': total_revenue,
        'avg_order_value': avg_order_value,
        'date_range': date_range,
        'daily_sales': daily_sales,
        'report_file': report_filename
    }

if __name__ == "__main__":
    analyze_august_data()
