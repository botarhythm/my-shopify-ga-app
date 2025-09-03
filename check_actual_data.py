import duckdb
import os
from datetime import datetime

# データベースファイルの存在確認
db_path = './data/duckdb/commerce_test.duckdb'
if not os.path.exists(db_path):
    print(f"データベースファイルが存在しません: {db_path}")
    exit(1)

print(f"データベースファイルサイズ: {os.path.getsize(db_path)} bytes")

try:
    con = duckdb.connect(db_path)
    
    print('=== データベース接続テスト ===')
    
    # テーブル一覧
    print('テーブル一覧:')
    tables = con.execute('SHOW TABLES').fetchall()
    for table in tables:
        print(f"  - {table[0]}")
    
    print('\n=== Shopifyデータ詳細 ===')
    shopify_count = con.execute('SELECT COUNT(*) FROM core_shopify').fetchone()[0]
    print(f'Shopifyレコード数: {shopify_count}')
    
    if shopify_count > 0:
        print('\nShopifyデータの日付範囲:')
        date_range = con.execute('SELECT MIN(date), MAX(date) FROM core_shopify').fetchone()
        print(f"  開始日: {date_range[0]}")
        print(f"  終了日: {date_range[1]}")
        
        print('\n=== Shopify重複チェック ===')
        # 重複チェック（同じ日付、同じ商品、同じ金額の組み合わせ）
        duplicates = con.execute("""
            SELECT date, title, order_total, qty, COUNT(*) as duplicate_count
            FROM core_shopify 
            GROUP BY date, title, order_total, qty
            HAVING COUNT(*) > 1
            ORDER BY date DESC, duplicate_count DESC
        """).fetchall()
        
        if duplicates:
            print(f'重複レコード数: {len(duplicates)}')
            print('重複データ:')
            for row in duplicates[:10]:  # 最初の10件のみ表示
                print(f"  {row[0]}: {row[1]} - ¥{row[2]:,.0f} x{row[3]} (重複{row[4]}回)")
        else:
            print('重複データなし')
        
        print('\n=== Shopify 2025年8月データ詳細 ===')
        august_data = con.execute("""
            SELECT date, title, order_total, qty, order_id
            FROM core_shopify 
            WHERE date >= '2025-08-01' AND date <= '2025-08-31'
            ORDER BY date DESC, order_total DESC
        """).fetchall()
        
        print(f'2025年8月のレコード数: {len(august_data)}')
        if august_data:
            print('2025年8月のデータ（最新10件）:')
            for row in august_data[:10]:
                print(f"  {row[0]}: {row[1]} - ¥{row[2]:,.0f} x{row[3]} (ID: {row[4]})")
        
        print('\n=== Shopify売上集計（日付別・重複除外） ===')
        daily_revenue_unique = con.execute("""
            SELECT date, SUM(order_total) as daily_revenue, COUNT(DISTINCT order_id) as unique_orders
            FROM core_shopify 
            WHERE date >= '2025-08-01' AND date <= '2025-08-31'
            GROUP BY date 
            ORDER BY date DESC
        """).fetchall()
        
        total_revenue = 0
        for row in daily_revenue_unique:
            print(f"  {row[0]}: ¥{row[1]:,.0f} ({row[2]}件)")
            total_revenue += row[1]
        
        print(f'\n2025年8月総売上（重複除外）: ¥{total_revenue:,.0f}')
    
    print('\n=== Squareデータ詳細 ===')
    square_count = con.execute('SELECT COUNT(*) FROM core_square').fetchone()[0]
    print(f'Squareレコード数: {square_count}')
    
    if square_count > 0:
        print('\nSquareデータの日付範囲:')
        date_range = con.execute('SELECT MIN(date), MAX(date) FROM core_square').fetchone()
        print(f"  開始日: {date_range[0]}")
        print(f"  終了日: {date_range[1]}")
        
        print('\nSquareデータの全件:')
        all_square = con.execute('SELECT date, payment_id, amount, status, created_at FROM core_square ORDER BY date DESC').fetchall()
        for row in all_square:
            print(f"  {row[0]}: {row[1]} - ¥{row[2]:,.0f} ({row[3]}) - {row[4]}")
        
        print('\n=== Square 2024年7月データ ===')
        july_2024_data = con.execute("""
            SELECT date, payment_id, amount, status, created_at
            FROM core_square 
            WHERE date >= '2024-07-01' AND date <= '2024-07-31'
            ORDER BY date DESC
        """).fetchall()
        
        print(f'2024年7月のレコード数: {len(july_2024_data)}')
        if july_2024_data:
            total_july_2024 = sum(row[2] for row in july_2024_data)
            print(f'2024年7月総売上: ¥{total_july_2024:,.0f}')
    
    print('\n=== 実際の売上集計（2025年8月） ===')
    # Shopify売上（2025年8月・重複除外）
    shopify_revenue = con.execute("""
        SELECT SUM(order_total) 
        FROM core_shopify 
        WHERE date >= '2025-08-01' AND date <= '2025-08-31'
    """).fetchone()[0]
    print(f'Shopify売上 (2025年8月): {shopify_revenue}')
    
    # Square売上（2025年8月）
    square_revenue = con.execute("""
        SELECT SUM(amount) 
        FROM core_square 
        WHERE date >= '2025-08-01' AND date <= '2025-08-31'
    """).fetchone()[0]
    print(f'Square売上 (2025年8月): {square_revenue}')
    
    con.close()
    
except Exception as e:
    print(f"エラー: {e}")
    import traceback
    traceback.print_exc()
