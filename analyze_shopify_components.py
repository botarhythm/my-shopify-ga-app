import os
from datetime import date
from dotenv import load_dotenv
import pandas as pd

load_dotenv()

from src.connectors.shopify import fetch_orders_incremental

start_iso = "2025-08-01T00:00:00Z"
orders_df = fetch_orders_incremental(start_iso)

aug = orders_df[(orders_df['date'] >= date(2025,8,1)) & (orders_df['date'] <= date(2025,8,31))].copy()

if aug.empty:
    print('2025-08 の注文なし')
    raise SystemExit(0)

# 注文単位に圧縮
by_order = aug.groupby('order_id').agg({
    'subtotal_price':'first',
    'total_line_items_price':'first',
    'total_discounts':'first',
    'total_tax':'first',
    'shipping_price':'first',
    'order_total':'first',
    'refunds_total':'first',
    'date':'first'
}).reset_index()

# 各構成要素の合計
totals = {
    'orders': len(by_order),
    'subtotal': by_order['subtotal_price'].sum(),
    'items_total': by_order['total_line_items_price'].sum(),
    'discounts': by_order['total_discounts'].sum(),
    'tax': by_order['total_tax'].sum(),
    'shipping': by_order['shipping_price'].sum(),
    'order_total': by_order['order_total'].sum(),
    'refunds': by_order['refunds_total'].sum(),
}

print('=== 構成要素合計 ===')
for k,v in totals.items():
    if k=='orders':
        print(f'{k}: {v}')
    else:
        print(f'{k}: ¥{v:,.0f}')

# 複数の純売上式を検証
candidates = {
    'total_price(=order_total)': by_order['order_total'].sum(),
    'subtotal - discounts': (by_order['subtotal_price'] - by_order['total_discounts']).sum(),
    'subtotal - discounts + tax': (by_order['subtotal_price'] - by_order['total_discounts'] + by_order['total_tax']).sum(),
    'subtotal - discounts + tax + shipping': (by_order['subtotal_price'] - by_order['total_discounts'] + by_order['total_tax'] + by_order['shipping_price']).sum(),
    'order_total - refunds': (by_order['order_total'] - by_order['refunds_total']).sum(),
    'subtotal - discounts + shipping - refunds': (by_order['subtotal_price'] - by_order['total_discounts'] + by_order['shipping_price'] - by_order['refunds_total']).sum(),
    'subtotal - discounts + tax + shipping - refunds': (by_order['subtotal_price'] - by_order['total_discounts'] + by_order['total_tax'] + by_order['shipping_price'] - by_order['refunds_total']).sum(),
}

print('\n=== 候補式の比較 ===')
for name, val in candidates.items():
    print(f'{name}: ¥{val:,.0f}')
