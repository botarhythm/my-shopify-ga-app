import duckdb

con = duckdb.connect('./data/duckdb/commerce_test.duckdb')

print('全期間の売上:')
print(con.execute('SELECT SUM(total_revenue) FROM mart_daily').fetchall())

print('2025年8月の売上:')
print(con.execute("SELECT SUM(total_revenue) FROM mart_daily WHERE date >= '2025-08-01' AND date <= '2025-08-31'").fetchall())

print('日付範囲:')
print(con.execute('SELECT MIN(date), MAX(date) FROM mart_daily').fetchall())

print('2025年8月の日別売上:')
print(con.execute("SELECT date, total_revenue FROM mart_daily WHERE date >= '2025-08-01' AND date <= '2025-08-31' ORDER BY date").fetchall())

con.close()
