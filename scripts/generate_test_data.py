# scripts/generate_test_data.py
import os, sys, datetime as dt
import duckdb, pandas as pd
import numpy as np
from pathlib import Path

# プロジェクトルートをパスに追加
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

DB = os.getenv("DUCKDB_PATH", "./data/duckdb/commerce.duckdb")

def generate_test_data():
    """テスト用データを生成してDuckDBに挿入"""
    con = duckdb.connect(DB, read_only=False)
    
    # テスト期間（過去30日）
    end_date = dt.date.today()
    start_date = end_date - dt.timedelta(days=30)
    date_range = pd.date_range(start_date, end_date, freq='D')
    
    print("🔧 テストデータ生成開始...")
    
    # 1. GA4データ生成
    ga4_data = []
    for date in date_range:
        # ランダムな変動を加えたベースライン
        base_sessions = 1000 + np.random.randint(-200, 200)
        base_users = int(base_sessions * 0.8) + np.random.randint(-50, 50)
        base_purchases = int(base_sessions * 0.05) + np.random.randint(-10, 10)
        base_revenue = base_purchases * (50 + np.random.randint(-20, 30))
        
        # 曜日効果（週末は売上が高い）
        if date.weekday() >= 5:  # 土日
            base_sessions *= 1.3
            base_revenue *= 1.4
        
        ga4_data.append({
            'date': date.date(),
            'source': np.random.choice(['google', 'direct', 'facebook', 'instagram']),
            'channel': np.random.choice(['organic', 'paid', 'social', 'email']),
            'page_path': np.random.choice(['/', '/products', '/collections', '/cart']),
            'sessions': max(0, int(base_sessions)),
            'users': max(0, int(base_users)),
            'purchases': max(0, int(base_purchases)),
            'revenue': max(0, float(base_revenue))
        })
    
    # 2. Shopifyデータ生成
    shopify_data = []
    order_id = 10000
    for date in date_range:
        daily_orders = np.random.randint(5, 20)
        for _ in range(daily_orders):
            lineitem_id = order_id * 100
            order_total = 0
            
            # 1オーダーあたり1-3商品
            items = np.random.randint(1, 4)
            for item in range(items):
                qty = np.random.randint(1, 4)
                price = np.random.randint(20, 200)
                item_total = qty * price
                order_total += item_total
                
                shopify_data.append({
                    'date': date.date(),
                    'order_id': order_id,
                    'lineitem_id': lineitem_id + item,
                    'product_id': np.random.randint(1000, 9999),
                    'variant_id': np.random.randint(10000, 99999),
                    'sku': f'SKU{np.random.randint(1000, 9999)}',
                    'title': f'商品{np.random.randint(1, 100)}',
                    'qty': qty,
                    'price': price,
                    'order_total': item_total
                })
            order_id += 1
    
    # 3. Squareデータ生成
    square_data = []
    for date in date_range:
        daily_payments = np.random.randint(3, 15)
        for _ in range(daily_payments):
            amount = np.random.randint(500, 5000)
            square_data.append({
                'date': date.date(),
                'payment_id': f'SQ{np.random.randint(100000, 999999)}',
                'amount': amount,
                'currency': 'JPY',
                'card_brand': np.random.choice(['visa', 'mastercard', 'amex']),
                'status': 'COMPLETED'
            })
    
    # 4. Google Adsデータ生成
    ads_data = []
    for date in date_range:
        daily_campaigns = np.random.randint(2, 8)
        for _ in range(daily_campaigns):
            cost = np.random.randint(1000, 10000)
            clicks = np.random.randint(10, 100)
            impressions = clicks * np.random.randint(5, 15)
            conversions = int(clicks * np.random.uniform(0.01, 0.05))
            conv_value = conversions * np.random.randint(1000, 5000)
            
            ads_data.append({
                'date': date.date(),
                'campaign_id': np.random.randint(100000000, 999999999),
                'campaign_name': f'キャンペーン{np.random.randint(1, 20)}',
                'cost': cost,
                'clicks': clicks,
                'impressions': impressions,
                'conversions': conversions,
                'conv_value': conv_value
            })
    
    # データをDataFrameに変換
    ga4_df = pd.DataFrame(ga4_data)
    shopify_df = pd.DataFrame(shopify_data)
    square_df = pd.DataFrame(square_data)
    ads_df = pd.DataFrame(ads_data)
    
    # DuckDBに挿入
    print(f"📊 GA4データ: {len(ga4_df)}行")
    print(f"🛒 Shopifyデータ: {len(shopify_df)}行")
    print(f"💳 Squareデータ: {len(square_df)}行")
    print(f"📈 Google Adsデータ: {len(ads_df)}行")
    
    # 既存データをクリアしてから挿入
    con.execute("DELETE FROM core_ga4")
    con.execute("DELETE FROM core_shopify")
    con.execute("DELETE FROM core_square")
    con.execute("DELETE FROM core_ads_campaign")
    
    # データ挿入
    con.register("ga4_df", ga4_df)
    con.register("shopify_df", shopify_df)
    con.register("square_df", square_df)
    con.register("ads_df", ads_df)
    
    con.execute("INSERT INTO core_ga4 SELECT * FROM ga4_df")
    con.execute("INSERT INTO core_shopify SELECT * FROM shopify_df")
    con.execute("INSERT INTO core_square SELECT * FROM square_df")
    con.execute("INSERT INTO core_ads_campaign SELECT * FROM ads_df")
    
    # 登録解除
    con.unregister("ga4_df")
    con.unregister("shopify_df")
    con.unregister("square_df")
    con.unregister("ads_df")
    
    con.close()
    print("✅ テストデータ生成完了")

if __name__ == "__main__":
    generate_test_data()
