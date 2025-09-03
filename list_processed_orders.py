import os
from datetime import date
from dotenv import load_dotenv
import pandas as pd

load_dotenv()

from src.connectors.shopify import fetch_orders_by_processed_range

START = "2025-08-01T00:00:00+09:00"
END = "2025-08-31T23:59:59+09:00"
FS = ["paid", "partially_paid", "partially_refunded"]

df = fetch_orders_by_processed_range(START, END, financial_statuses=FS)

if df.empty:
    print("データなし")
    raise SystemExit(0)

by = df.groupby("order_id").agg({
    "order_total": "first",
    "subtotal_price": "first",
    "total_discounts": "first",
    "total_tax": "first",
    "total_tip": "first",
    "shipping_price": "first",
    "refunds_total": "first",
    "created_at": "first",
    "processed_at": "first",
    "financial_status": "first",
}).reset_index().sort_values("created_at")

order_total_sum = by["order_total"].sum()
net_v1 = (by["subtotal_price"] - by["total_discounts"] + by["total_tax"] + by["shipping_price"] + by["total_tip"] - by["refunds_total"]).sum()
net_v2 = (by["order_total"] - by["refunds_total"]).sum()

print(f"件数: {len(by)}")
print(f"order_total合計: {order_total_sum:,.0f}")
print(f"純売上1(小計-割引+税+送料+チップ-返金): {net_v1:,.0f}")
print(f"純売上2(order_total-返金): {net_v2:,.0f}")

print("\n=== 明細 ===")
print(by[["created_at","processed_at","order_id","financial_status","order_total","subtotal_price","total_discounts","total_tax","shipping_price","total_tip","refunds_total"]].to_string(index=False))
