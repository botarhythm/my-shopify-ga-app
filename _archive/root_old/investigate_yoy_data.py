import duckdb
import os
from dotenv import load_dotenv

load_dotenv()
db_path = os.getenv('DUCKDB_PATH', './data/duckdb/commerce.duckdb')

con = duckdb.connect(db_path)

print('=== YoYデータの詳細調査 ===')

# 1. mart_daily_yoyテーブルの構造確認
print('\n1. mart_daily_yoyテーブル構造:')
try:
    columns = con.execute('DESCRIBE mart_daily_yoy').fetchall()
    print('列:', [col[0] for col in columns])
    
    # サンプルデータ
    sample = con.execute('SELECT * FROM mart_daily_yoy LIMIT 5').fetchall()
    print('サンプルデータ:')
    for i, row in enumerate(sample):
        print(f'  Row {i+1}: {row}')
    
    # 2024年8月のデータ確認
    yoy_2024_data = con.execute("""
    SELECT date, sessions, sessions_prev, total_revenue, total_revenue_prev, roas, roas_prev
    FROM mart_daily_yoy 
    WHERE date >= '2024-08-01' AND date <= '2024-08-31'
    ORDER BY date
    """).fetchall()
    
    print(f'\n2024年8月のYoYデータ件数: {len(yoy_2024_data)}')
    if yoy_2024_data:
        print('2024年8月のデータ:')
        for row in yoy_2024_data[:5]:  # 最初の5件のみ表示
            print(f'  {row}')
    else:
        print('2024年8月のデータが存在しません')
    
    # 2025年8月のデータ確認
    yoy_2025_data = con.execute("""
    SELECT date, sessions, sessions_prev, total_revenue, total_revenue_prev, roas, roas_prev
    FROM mart_daily_yoy 
    WHERE date >= '2025-08-01' AND date <= '2025-08-31'
    ORDER BY date
    """).fetchall()
    
    print(f'\n2025年8月のYoYデータ件数: {len(yoy_2025_data)}')
    if yoy_2025_data:
        print('2025年8月のデータ:')
        for row in yoy_2025_data[:5]:  # 最初の5件のみ表示
            print(f'  {row}')
    else:
        print('2025年8月のデータが存在しません')
    
except Exception as e:
    print(f'エラー: {e}')

# 2. 他のテーブルで2024年データがあるか確認
print('\n2. 他のテーブルでの2024年データ確認:')

# core_shopifyで2024年データ
try:
    shopify_2024_count = con.execute("SELECT COUNT(*) FROM core_shopify WHERE date >= '2024-08-01' AND date <= '2024-08-31'").fetchone()[0]
    print(f'core_shopify 2024年8月データ件数: {shopify_2024_count}')
except Exception as e:
    print(f'core_shopify 2024年データ確認エラー: {e}')

# core_squareで2024年データ
try:
    square_2024_count = con.execute("SELECT COUNT(*) FROM core_square WHERE date >= '2024-08-01' AND date <= '2024-08-31'").fetchone()[0]
    print(f'core_square 2024年8月データ件数: {square_2024_count}')
except Exception as e:
    print(f'core_square 2024年データ確認エラー: {e}')

con.close()

