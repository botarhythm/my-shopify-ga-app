#!/usr/bin/env python3
"""
拡張データ取得結果確認スクリプト
"""
import duckdb
import os
from dotenv import load_dotenv

load_dotenv()
db_path = os.getenv('DUCKDB_PATH', './data/duckdb/commerce_fresh.duckdb')

con = duckdb.connect(db_path, read_only=True)

print('=== 拡張データ取得結果確認 ===')

# データの日付範囲確認
print()
print('--- データの日付範囲 ---')
shopify_range = con.execute("SELECT MIN(date), MAX(date) FROM core_shopify WHERE financial_status = 'paid'").fetchone()
square_range = con.execute("SELECT MIN(date), MAX(date) FROM core_square WHERE status = 'COMPLETED'").fetchone()

print('Shopify:', shopify_range[0], '～', shopify_range[1])
print('Square:', square_range[0], '～', square_range[1])

# 年別データ件数
print()
print('--- 年別データ件数 ---')
shopify_yearly = con.execute('''
    SELECT 
        strftime(date, '%Y') as year,
        COUNT(DISTINCT order_id) as orders,
        SUM(total_price) as revenue
    FROM core_shopify 
    WHERE financial_status = 'paid'
    GROUP BY strftime(date, '%Y')
    ORDER BY year
''').fetchall()

square_yearly = con.execute('''
    SELECT 
        strftime(date, '%Y') as year,
        COUNT(*) as payments,
        SUM(amount) as revenue
    FROM core_square 
    WHERE status = 'COMPLETED'
    GROUP BY strftime(date, '%Y')
    ORDER BY year
''').fetchall()

print('Shopify年別データ:')
for row in shopify_yearly:
    print('  {}: {}件, ¥{:,}'.format(row[0], row[1], row[2]))

print('Square年別データ:')
for row in square_yearly:
    print('  {}: {}件, ¥{:,}'.format(row[0], row[1], row[2]))

# 2023年のデータ確認
print()
print('--- 2023年データ確認 ---')
shopify_2023 = con.execute('''
    SELECT 
        strftime(date, '%Y-%m') as month,
        COUNT(DISTINCT order_id) as orders,
        SUM(total_price) as revenue
    FROM core_shopify 
    WHERE financial_status = 'paid'
    AND strftime(date, '%Y') = '2023'
    GROUP BY strftime(date, '%Y-%m')
    ORDER BY month
''').fetchall()

square_2023 = con.execute('''
    SELECT 
        strftime(date, '%Y-%m') as month,
        COUNT(*) as payments,
        SUM(amount) as revenue
    FROM core_square 
    WHERE status = 'COMPLETED'
    AND strftime(date, '%Y') = '2023'
    GROUP BY strftime(date, '%Y-%m')
    ORDER BY month
''').fetchall()

print('2023年Shopifyデータ:')
for row in shopify_2023:
    print('  {}: {}件, ¥{:,}'.format(row[0], row[1], row[2]))

print('2023年Squareデータ:')
for row in square_2023:
    print('  {}: {}件, ¥{:,}'.format(row[0], row[1], row[2]))

con.close()
