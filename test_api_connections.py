import os
import sys
from datetime import datetime, date
from dotenv import load_dotenv

# .envファイルを読み込み
load_dotenv()

def test_shopify_connection():
    """Shopify API接続テスト"""
    print("=== Shopify API接続テスト ===")
    
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
                total_revenue = august_orders['order_total'].sum()
                print(f"2025年8月総売上: ¥{total_revenue:,.0f}")
                
                # 最新5件を表示
                print("\n最新5件の注文:")
                for _, order in august_orders.head().iterrows():
                    print(f"  {order['date']}: {order['title']} - ¥{order['order_total']:,.0f}")
            else:
                print("2025年8月のデータが見つかりません")
        
        return True
        
    except Exception as e:
        print(f"❌ Shopify接続エラー: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_square_connection():
    """Square API接続テスト"""
    print("\n=== Square API接続テスト ===")
    
    try:
        from src.connectors.square import fetch_payments
        
        # 2025年8月のデータを取得
        start_date = date(2025, 8, 1)
        end_date = date(2025, 8, 31)
        
        print(f"期間: {start_date} 〜 {end_date}")
        
        payments_df = fetch_payments(start_date, end_date)
        print(f"取得した支払い数: {len(payments_df)}")
        
        if not payments_df.empty:
            total_revenue = payments_df['amount'].sum()
            print(f"2025年8月総売上: ¥{total_revenue:,.0f}")
            
            # 最新5件を表示
            print("\n最新5件の支払い:")
            for _, payment in payments_df.head().iterrows():
                print(f"  {payment['date']}: {payment['payment_id']} - ¥{payment['amount']:,.0f}")
        else:
            print("2025年8月のデータが見つかりません")
        
        return True
        
    except Exception as e:
        print(f"❌ Square接続エラー: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_ga4_connection():
    """GA4 API接続テスト"""
    print("\n=== GA4 API接続テスト ===")
    
    try:
        from src.connectors.ga4 import fetch_ga4_data
        
        # 2025年8月のデータを取得
        start_date = "2025-08-01"
        end_date = "2025-08-31"
        
        print(f"期間: {start_date} 〜 {end_date}")
        
        data = fetch_ga4_data(start_date, end_date)
        print(f"取得したデータ行数: {len(data)}")
        
        if data:
            total_sessions = sum(row['sessions'] for row in data)
            total_revenue = sum(row['total_revenue'] for row in data)
            print(f"総セッション数: {total_sessions:,}")
            print(f"総売上: ¥{total_revenue:,.0f}")
        
        return True
        
    except Exception as e:
        print(f"❌ GA4接続エラー: {e}")
        import traceback
        traceback.print_exc()
        return False

def main():
    """メイン実行"""
    print("🚀 API接続テスト開始")
    print(f"📁 作業ディレクトリ: {os.getcwd()}")
    
    # 環境変数確認
    print("\n🔍 環境変数確認:")
    print(f"  GA4_PROPERTY_ID: {os.getenv('GA4_PROPERTY_ID', '未設定')}")
    print(f"  GOOGLE_ADS_CUSTOMER_ID: {os.getenv('GOOGLE_ADS_CUSTOMER_ID', '未設定')}")
    print(f"  SHOPIFY_ACCESS_TOKEN: {'設定済み' if os.getenv('SHOPIFY_ACCESS_TOKEN') else '未設定'}")
    print(f"  SQUARE_ACCESS_TOKEN: {'設定済み' if os.getenv('SQUARE_ACCESS_TOKEN') else '未設定'}")
    
    # 各API接続テスト
    shopify_ok = test_shopify_connection()
    square_ok = test_square_connection()
    ga4_ok = test_ga4_connection()
    
    print("\n=== テスト結果 ===")
    print(f"Shopify: {'✅' if shopify_ok else '❌'}")
    print(f"Square: {'✅' if square_ok else '❌'}")
    print(f"GA4: {'✅' if ga4_ok else '❌'}")

if __name__ == "__main__":
    main()
