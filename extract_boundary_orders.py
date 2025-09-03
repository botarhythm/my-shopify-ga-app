import os
from datetime import datetime
from dotenv import load_dotenv
import pandas as pd

load_dotenv()

from src.connectors.shopify import fetch_orders_by_processed_range

start_iso = "2025-08-01T00:00:00+09:00"
end_iso = "2025-08-31T23:59:59+09:00"
fs_all = ["paid","partially_paid","partially_refunded"]

df = fetch_orders_by_processed_range(start_iso, end_iso, financial_statuses=fs_all)

if df.empty:
    print('データなし')
    raise SystemExit(0)

# 境界近傍（±1時間）
def to_dt(s):
    try:
        return datetime.fromisoformat(str(s).replace('Z','+00:00'))
    except Exception:
        return None

df['created_dt'] = df['created_at'].apply(to_dt)

a = datetime.fromisoformat('2025-08-01T00:00:00+09:00')
b = datetime.fromisoformat('2025-08-31T23:59:59+09:00')

near_start = df[(df['created_dt'] >= a) & (df['created_dt'] <= a.replace(hour=1))]
near_end = df[(df['created_dt'] >= b.replace(hour=22)) & (df['created_dt'] <= b)]

print('=== 開始境界 1時間内 ===')
print(near_start[['order_id','created_at','processed_at','paid_at','order_total']].drop_duplicates().to_string(index=False))

print('\n=== 終了境界 1時間内 ===')
print(near_end[['order_id','created_at','processed_at','paid_at','order_total']].drop_duplicates().to_string(index=False))
