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
    
    print('=== Shopify重複データの除去 ===')
    
    # 重複データの確認
    duplicate_count = con.execute("""
        SELECT COUNT(*) FROM (
            SELECT date, title, order_total, qty, COUNT(*) as cnt
            FROM core_shopify 
            GROUP BY date, title, order_total, qty
            HAVING COUNT(*) > 1
        )
    """).fetchone()[0]
    
    print(f'重複レコード数: {duplicate_count}')
    
    if duplicate_count > 0:
        # 重複データを削除（order_idが異なる場合、最新のものを残す）
        deleted_count = con.execute("""
            DELETE FROM core_shopify 
            WHERE (date, title, order_total, qty) IN (
                SELECT date, title, order_total, qty
                FROM core_shopify 
                GROUP BY date, title, order_total, qty
                HAVING COUNT(*) > 1
            )
            AND order_id NOT IN (
                SELECT MAX(order_id)
                FROM core_shopify 
                GROUP BY date, title, order_total, qty
                HAVING COUNT(*) > 1
            )
        """).fetchone()[0]
        
        print(f'削除された重複レコード数: {deleted_count}')
    
    print('\n=== Square古いデータの削除 ===')
    
    # 2024年のSquareデータを削除
    deleted_square = con.execute("DELETE FROM core_square WHERE date < '2025-01-01'").fetchone()[0]
    print(f'削除されたSquareレコード数: {deleted_square}')
    
    print('\n=== 修正後のデータ確認 ===')
    
    # Shopifyデータ確認
    shopify_count = con.execute('SELECT COUNT(*) FROM core_shopify').fetchone()[0]
    print(f'Shopifyレコード数: {shopify_count}')
    
    # 2025年8月のShopify売上（重複除去後）
    shopify_revenue = con.execute("""
        SELECT SUM(order_total) 
        FROM core_shopify 
        WHERE date >= '2025-08-01' AND date <= '2025-08-31'
    """).fetchone()[0]
    print(f'Shopify売上 (2025年8月・重複除去後): {shopify_revenue}')
    
    # Squareデータ確認
    square_count = con.execute('SELECT COUNT(*) FROM core_square').fetchone()[0]
    print(f'Squareレコード数: {square_count}')
    
    if square_count > 0:
        square_date_range = con.execute('SELECT MIN(date), MAX(date) FROM core_square').fetchone()
        print(f'Squareデータ期間: {square_date_range[0]} 〜 {square_date_range[1]}')
    
    # 2025年8月のSquare売上
    square_revenue = con.execute("""
        SELECT SUM(amount) 
        FROM core_square 
        WHERE date >= '2025-08-01' AND date <= '2025-08-31'
    """).fetchone()[0]
    print(f'Square売上 (2025年8月): {square_revenue}')
    
    con.close()
    
    print('\n=== 修正完了 ===')
    print('データベースの重複データと古いデータを削除しました。')
    
except Exception as e:
    print(f"エラー: {e}")
    import traceback
    traceback.print_exc()
