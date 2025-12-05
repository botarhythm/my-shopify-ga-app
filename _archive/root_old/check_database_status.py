import duckdb
import os
from dotenv import load_dotenv

load_dotenv()
db_path = os.getenv('DUCKDB_PATH', './data/duckdb/commerce.duckdb')

con = duckdb.connect(db_path)

print('=== データベースの現在の状態確認 ===')

# 1. core_shopifyテーブルの詳細確認
print('\n1. core_shopifyテーブル:')
try:
    columns = con.execute('DESCRIBE core_shopify').fetchall()
    print('列:', [col[0] for col in columns])
    
    # サンプルデータ
    sample = con.execute('SELECT * FROM core_shopify LIMIT 3').fetchall()
    print('サンプルデータ:')
    for i, row in enumerate(sample):
        print(f'  Row {i+1}: {row}')
    
    # 総件数
    count = con.execute('SELECT COUNT(*) FROM core_shopify').fetchone()[0]
    print(f'総件数: {count}')
    
except Exception as e:
    print(f'エラー: {e}')

# 2. core_squareテーブルの詳細確認
print('\n2. core_squareテーブル:')
try:
    columns = con.execute('DESCRIBE core_square').fetchall()
    print('列:', [col[0] for col in columns])
    
    # サンプルデータ
    sample = con.execute('SELECT * FROM core_square LIMIT 3').fetchall()
    print('サンプルデータ:')
    for i, row in enumerate(sample):
        print(f'  Row {i+1}: {row}')
    
    # 総件数
    count = con.execute('SELECT COUNT(*) FROM core_square').fetchone()[0]
    print(f'総件数: {count}')
    
    # 請求書データの確認
    invoice_data = con.execute("SELECT payment_id, amount, date FROM core_square WHERE payment_id = '7LLQ5fDGvIYCk5xP44N9iARtzBfZY'").fetchall()
    print(f'請求書データ: {invoice_data}')
    
except Exception as e:
    print(f'エラー: {e}')

con.close()

