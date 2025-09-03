import os
import sys
from datetime import datetime, date
from dotenv import load_dotenv

# .envファイルを読み込み
load_dotenv()

def analyze_shopify_data():
    """Shopifyデータの詳細分析"""
    print("=== Shopifyデータ詳細分析 ===")
    
    try:
        from src.connectors.shopify import fetch_orders_incremental
        
        # 2025年8月のデータを取得
        start_date = "2025-08-01T00:00:00Z"
        
        print(f"期間: 2025-08-01 以降")
        
        orders_df = fetch_orders_incremental(start_date)
        print(f"取得した注文数: {len(orders_df)}")
        
        if not orders_df.empty:
            # 2025年8月のデータのみフィルタ
            august_orders = orders_df[orders_df['date'] >= date(2025, 8, 1)]
            august_orders = august_orders[august_orders['date'] <= date(2025, 8, 31)]
            
            print(f"2025年8月の注文数: {len(august_orders)}")
            
            if not august_orders.empty:
                print("\n=== 売上分析 ===")
                
                # 注文ID別の売上集計
                order_revenue = august_orders.groupby('order_id')['order_total'].sum().reset_index()
                print(f"ユニーク注文数: {len(order_revenue)}")
                
                total_revenue = order_revenue['order_total'].sum()
                print(f"総売上（注文ID別集計）: ¥{total_revenue:,.0f}")
                
                # 商品別の売上集計
                product_revenue = august_orders.groupby('title')['order_total'].sum().reset_index()
                print(f"商品数: {len(product_revenue)}")
                
                # 重複チェック
                print("\n=== 重複チェック ===")
                duplicate_check = august_orders.groupby(['date', 'title', 'order_total', 'qty']).size().reset_index(name='count')
                duplicates = duplicate_check[duplicate_check['count'] > 1]
                
                if not duplicates.empty:
                    print(f"重複レコード数: {len(duplicates)}")
                    print("重複データ:")
                    for _, row in duplicates.head().iterrows():
                        print(f"  {row['date']}: {row['title']} - ¥{row['order_total']:,.0f} x{row['qty']} (重複{row['count']}回)")
                else:
                    print("重複データなし")
                
                # 日付別売上
                print("\n=== 日付別売上 ===")
                daily_revenue = august_orders.groupby('date')['order_total'].sum().reset_index()
                for _, row in daily_revenue.iterrows():
                    print(f"  {row['date']}: ¥{row['order_total']:,.0f}")
                
                # 商品別売上（上位10件）
                print("\n=== 商品別売上（上位10件） ===")
                top_products = product_revenue.sort_values('order_total', ascending=False).head(10)
                for _, row in top_products.iterrows():
                    print(f"  {row['title']}: ¥{row['order_total']:,.0f}")
        
        return True
        
    except Exception as e:
        print(f"❌ Shopify分析エラー: {e}")
        import traceback
        traceback.print_exc()
        return False

def main():
    """メイン実行"""
    print("🚀 Shopifyデータ詳細分析開始")
    print(f"📁 作業ディレクトリ: {os.getcwd()}")
    
    # Shopifyデータ分析
    analyze_shopify_data()

if __name__ == "__main__":
    main()
