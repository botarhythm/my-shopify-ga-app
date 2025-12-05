import duckdb
import os
from dotenv import load_dotenv

load_dotenv()
db_path = os.getenv('DUCKDB_PATH', './data/duckdb/commerce.duckdb')

con = duckdb.connect(db_path)

print('=== データベース構造の詳細確認 ===')

# 1. core_shopifyテーブルの詳細確認
print('\n1. core_shopifyテーブル:')
try:
    columns = con.execute('DESCRIBE core_shopify').fetchall()
    print('列:', [col[0] for col in columns])
    
    # サンプルデータ
    sample = con.execute('SELECT * FROM core_shopify LIMIT 5').fetchall()
    print('サンプルデータ:')
    for i, row in enumerate(sample):
        print(f'  Row {i+1}: {row}')
    
    # 8月のデータ件数
    count = con.execute("SELECT COUNT(*) FROM core_shopify WHERE date BETWEEN '2025-08-01' AND '2025-08-31'").fetchone()[0]
    print(f'8月のデータ件数: {count}')
    
    # 利用可能な列での売上計算
    if 'price' in [col[0] for col in columns] and 'qty' in [col[0] for col in columns]:
        revenue = con.execute("SELECT SUM(price * qty) FROM core_shopify WHERE date BETWEEN '2025-08-01' AND '2025-08-31'").fetchone()[0]
        print(f'price * qty での8月売上: ¥{revenue:,.0f}')
    
    if 'order_total' in [col[0] for col in columns]:
        revenue = con.execute("SELECT SUM(order_total) FROM core_shopify WHERE date BETWEEN '2025-08-01' AND '2025-08-31'").fetchone()[0]
        print(f'order_total での8月売上: ¥{revenue:,.0f}')
    
except Exception as e:
    print(f'エラー: {e}')

# 2. core_squareテーブルの詳細確認
print('\n2. core_squareテーブル:')
try:
    columns = con.execute('DESCRIBE core_square').fetchall()
    print('列:', [col[0] for col in columns])
    
    # サンプルデータ
    sample = con.execute('SELECT * FROM core_square LIMIT 5').fetchall()
    print('サンプルデータ:')
    for i, row in enumerate(sample):
        print(f'  Row {i+1}: {row}')
    
    # 8月のデータ件数
    count = con.execute("SELECT COUNT(*) FROM core_square WHERE date BETWEEN '2025-08-01' AND '2025-08-31'").fetchone()[0]
    print(f'8月のデータ件数: {count}')
    
    # 請求書データの確認
    invoice_data = con.execute("SELECT payment_id, amount, date FROM core_square WHERE payment_id = '7LLQ5fDGvIYCk5xP44N9iARtzBfZY'").fetchall()
    print(f'請求書データ: {invoice_data}')
    
    # 8月の総売上
    total_revenue = con.execute("SELECT SUM(amount) FROM core_square WHERE date BETWEEN '2025-08-01' AND '2025-08-31'").fetchone()[0]
    print(f'8月の総売上: ¥{total_revenue:,.0f}')
    
    # 請求書以外の売上
    coffee_revenue = con.execute("SELECT SUM(amount) FROM core_square WHERE date BETWEEN '2025-08-01' AND '2025-08-31' AND payment_id != '7LLQ5fDGvIYCk5xP44N9iARtzBfZY'").fetchone()[0]
    print(f'8月のコーヒー売上: ¥{coffee_revenue:,.0f}')
    
except Exception as e:
    print(f'エラー: {e}')

con.close()

