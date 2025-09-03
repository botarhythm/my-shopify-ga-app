import duckdb
import os

# データベースファイルの存在確認
db_path = './data/duckdb/commerce_test.duckdb'
if not os.path.exists(db_path):
    print(f"データベースファイルが存在しません: {db_path}")
    exit(1)

print(f"データベースファイルサイズ: {os.path.getsize(db_path)} bytes")

try:
    con = duckdb.connect(db_path)
    
    print('=== 古いデータの削除 ===')
    
    # 2025年8月以外のSquareデータを削除
    deleted_count = con.execute("DELETE FROM core_square WHERE date < '2025-08-01' OR date > '2025-08-31'").fetchone()[0]
    print(f'削除されたSquareレコード数: {deleted_count}')
    
    # 2025年8月以外のShopifyデータを削除
    deleted_count = con.execute("DELETE FROM core_shopify WHERE date < '2025-08-01' OR date > '2025-08-31'").fetchone()[0]
    print(f'削除されたShopifyレコード数: {deleted_count}')
    
    print('\n=== データベース接続テスト ===')
    
    # テーブル一覧
    print('テーブル一覧:')
    tables = con.execute('SHOW TABLES').fetchall()
    for table in tables:
        print(f"  - {table[0]}")
    
    print('\n=== Shopifyデータ ===')
    shopify_count = con.execute('SELECT COUNT(*) FROM core_shopify').fetchone()[0]
    print(f'Shopifyレコード数: {shopify_count}')
    
    if shopify_count > 0:
        print('Shopifyサンプルデータ:')
        sample = con.execute('SELECT * FROM core_shopify LIMIT 3').fetchall()
        for row in sample:
            print(f"  {row}")
    
    print('\n=== Squareデータ ===')
    square_count = con.execute('SELECT COUNT(*) FROM core_square').fetchone()[0]
    print(f'Squareレコード数: {square_count}')
    
    if square_count > 0:
        print('Squareサンプルデータ:')
        sample = con.execute('SELECT * FROM core_square LIMIT 3').fetchall()
        for row in sample:
            print(f"  {row}")
    
    print('\n=== 売上集計 ===')
    # Shopify売上
    shopify_revenue = con.execute("SELECT SUM(order_total) FROM core_shopify WHERE date >= '2025-08-01' AND date <= '2025-08-31'").fetchone()[0]
    print(f'Shopify売上 (2025年8月): {shopify_revenue}')
    
    # Square売上
    square_revenue = con.execute("SELECT SUM(amount) FROM core_square WHERE date >= '2025-08-01' AND date <= '2025-08-31'").fetchone()[0]
    print(f'Square売上 (2025年8月): {square_revenue}')
    
    con.close()
    
except Exception as e:
    print(f"エラー: {e}")
    import traceback
    traceback.print_exc()
