import os
from dotenv import load_dotenv
from src.connectors.shopify import (
    fetch_orders_by_created_range,
    fetch_orders_by_processed_range,
)


def main() -> None:
    load_dotenv()
    start = "2025-08-01T00:00:00+09:00"
    end = "2025-08-31T23:59:59+09:00"
    fs = ["paid", "partially_paid", "partially_refunded"]

    created = fetch_orders_by_created_range(start, end, financial_statuses=fs)
    processed = fetch_orders_by_processed_range(start, end, financial_statuses=fs)

    def summarize(df):
        if df.empty:
            return 0, set()
        g = df.groupby("order_id")["order_total"].first()
        return float(g.sum()), set(g.index)

    cb, cids = summarize(created)
    pb, pids = summarize(processed)

    print(f"created_at合計: {int(cb):,}")
    print(f"processed_at合計: {int(pb):,}")
    only_c = sorted(list(cids - pids))
    only_p = sorted(list(pids - cids))
    print(f"created-only件数: {len(only_c)}")
    print(f"processed-only件数: {len(only_p)}")
    if only_c:
        print("created-only IDs:", only_c[:50])
    if only_p:
        print("processed-only IDs:", only_p[:50])


if __name__ == "__main__":
    main()


