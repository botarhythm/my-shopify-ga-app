import duckdb

con = duckdb.connect('./data/duckdb/commerce_test.duckdb')

print('2025年8月の売上内訳:')
print('Shopify売上:')
print(con.execute("SELECT SUM(order_total) FROM core_shopify WHERE date >= '2025-08-01' AND date <= '2025-08-31'").fetchall())

print('Square売上:')
print(con.execute("SELECT SUM(amount) FROM core_square WHERE date >= '2025-08-01' AND date <= '2025-08-31'").fetchall())

print('日別売上内訳:')
print('Shopify日別:')
print(con.execute("SELECT date, SUM(order_total) as daily_revenue FROM core_shopify WHERE date >= '2025-08-01' AND date <= '2025-08-31' GROUP BY date ORDER BY date").fetchall())

print('Square日別:')
print(con.execute("SELECT date, SUM(amount) as daily_revenue FROM core_square WHERE date >= '2025-08-01' AND date <= '2025-08-31' GROUP BY date ORDER BY date").fetchall())

con.close()
