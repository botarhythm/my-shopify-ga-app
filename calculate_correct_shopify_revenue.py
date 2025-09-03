import os
import sys
from datetime import datetime, date
from dotenv import load_dotenv

# .envファイルを読み込み
load_dotenv()

def get_correct_shopify_revenue():
    """正しいShopify売上を計算"""
    print("=== 正しいShopify売上計算（processed_at, JST） ===")
    
    try:
        from src.connectors.shopify import fetch_orders_by_processed_range, fetch_orders_by_paid_range
        
        # 2025年8月（JST）
        start_iso = "2025-08-01T00:00:00+09:00"
        end_iso = "2025-08-31T23:59:59+09:00"
        
        print(f"期間: {start_iso} 〜 {end_iso}")
        
        fs_all = ["paid","partially_paid","partially_refunded"]
        df_proc = fetch_orders_by_processed_range(start_iso, end_iso, financial_statuses=fs_all)
        print(f"processed_at取得行数: {len(df_proc)}")
        df_paid = fetch_orders_by_paid_range(start_iso, end_iso, financial_statuses=fs_all)
        print(f"paid_at取得行数: {len(df_paid)}")
        
        def summarize(df, label):
            if df.empty:
                print(f"{label}: データなし")
                return 0,0,0
            by_order = df.groupby('order_id').agg({
                'subtotal_price':'first','total_discounts':'first','total_tax':'first','total_tip':'first',
                'shipping_price':'first','order_total':'first','refunds_total':'first'
            }).reset_index()
            total_order_total = by_order['order_total'].sum()
            net1 = (by_order['subtotal_price'] - by_order['total_discounts'] + by_order['total_tax'] + by_order['shipping_price'] + by_order['total_tip'] - by_order['refunds_total']).sum()
            net2 = (by_order['order_total'] - by_order['refunds_total']).sum()
            print(f"[{label}] order_total合計: ¥{total_order_total:,.0f} | 純売上1: ¥{net1:,.0f} | 純売上2: ¥{net2:,.0f}")
            return total_order_total, net1, net2
        
        t1,n1,m1 = summarize(df_proc, "processed_at")
        t2,n2,m2 = summarize(df_paid, "paid_at")
        
        return int(round(max(n1,n2,m1,m2)))
        
    except Exception as e:
        print(f"❌ Shopify売上計算エラー: {e}")
        import traceback
        traceback.print_exc()
        return 0

def main():
    """メイン実行"""
    print("🚀 正しいShopify売上計算開始")
    print(f"📁 作業ディレクトリ: {os.getcwd()}")
    
    # 正しいShopify売上を計算
    correct_revenue = get_correct_shopify_revenue()
    
    print(f"\n=== 結果 ===")
    print(f"正しいShopify売上（2025年8月）: ¥{correct_revenue:,.0f}")

if __name__ == "__main__":
    main()
