import os
import sys
from datetime import datetime, date
from dotenv import load_dotenv

# .envファイルを読み込み
load_dotenv()

def analyze_square_data():
    """Squareデータの詳細分析"""
    print("=== Squareデータ詳細分析 ===")
    
    try:
        from src.connectors.square import fetch_payments
        
        # 2025年8月のデータを取得
        start_date = "2025-08-01"
        end_date = "2025-08-31"
        
        print(f"期間: {start_date} 〜 {end_date}")
        
        payments_df = fetch_payments(start_date, end_date)
        print(f"取得した支払い数: {len(payments_df)}")
        
        if not payments_df.empty:
            print("\n=== 支払い詳細 ===")
            for _, payment in payments_df.iterrows():
                print(f"  {payment['date']}: {payment['payment_id']} - ¥{payment['amount']:,.0f} ({payment['status']})")
                print(f"    通貨: {payment['currency']}")
                print(f"    カードブランド: {payment['card_brand']}")
                print(f"    エントリーメソッド: {payment['entry_method']}")
                print(f"    手数料: {payment['processing_fee']}")
                print()
            
            total_revenue = payments_df['amount'].sum()
            print(f"総売上: ¥{total_revenue:,.0f}")
            
            # 通貨別集計
            currency_summary = payments_df.groupby('currency')['amount'].sum()
            print("\n=== 通貨別集計 ===")
            for currency, amount in currency_summary.items():
                print(f"  {currency}: ¥{amount:,.0f}")
            
            # ステータス別集計
            status_summary = payments_df.groupby('status')['amount'].sum()
            print("\n=== ステータス別集計 ===")
            for status, amount in status_summary.items():
                print(f"  {status}: ¥{amount:,.0f}")
        
        return True
        
    except Exception as e:
        print(f"❌ Square分析エラー: {e}")
        import traceback
        traceback.print_exc()
        return False

def main():
    """メイン実行"""
    print("🚀 Squareデータ詳細分析開始")
    print(f"📁 作業ディレクトリ: {os.getcwd()}")
    
    # Squareデータ分析
    analyze_square_data()

if __name__ == "__main__":
    main()
