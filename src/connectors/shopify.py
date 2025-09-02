"""
Shopify データ取得コネクタ
増分取得によるShopify統合
"""
import os
import requests
import pandas as pd
from datetime import datetime, timezone
from typing import Optional, List
from tenacity import retry, stop_after_attempt, wait_exponential


def _get_base_url() -> str:
    """Shopify API ベースURLを取得"""
    shop_url = os.getenv("SHOPIFY_SHOP_URL")
    if not shop_url:
        raise ValueError("SHOPIFY_SHOP_URL 環境変数が設定されていません")
    return f"https://{shop_url}/admin/api/2024-10"


def _get_headers() -> dict:
    """認証ヘッダーを取得"""
    token = os.getenv("SHOPIFY_ACCESS_TOKEN")
    if not token:
        raise ValueError("SHOPIFY_ACCESS_TOKEN 環境変数が設定されていません")
    return {"X-Shopify-Access-Token": token}


@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
def _make_request(url: str) -> dict:
    """Shopify API リクエストを実行"""
    response = requests.get(url, headers=_get_headers())
    response.raise_for_status()
    return response.json()


def _extract_next_url(link_header: str) -> Optional[str]:
    """Link ヘッダーから次のページURLを抽出"""
    if 'rel="next"' not in link_header:
        return None
    
    for part in link_header.split(","):
        if 'rel="next"' in part:
            start = part.find("<") + 1
            end = part.find(">")
            return part[start:end]
    return None


def fetch_orders_incremental(created_at_min: str, limit: int = 250) -> pd.DataFrame:
    """
    増分でShopify注文データを取得
    
    Args:
        created_at_min: 最小作成日時 (ISO 8601形式)
        limit: 1回のリクエストで取得する件数
    
    Returns:
        DataFrame: 正規化された注文・商品データ
    """
    base_url = _get_base_url()
    url = f"{base_url}/orders.json?status=any&limit={limit}&created_at_min={created_at_min}"
    
    orders = []
    
    while True:
        try:
            data = _make_request(url)
            batch_orders = data.get("orders", [])
            orders.extend(batch_orders)
            
            # 次のページがあるかチェック
            link_header = requests.get(url, headers=_get_headers()).headers.get("Link", "")
            next_url = _extract_next_url(link_header)
            
            if not next_url:
                break
            url = next_url
            
        except Exception as e:
            print(f"Shopify API エラー: {e}")
            break
    
    # LineItems の正規化
    rows = []
    for order in orders:
        for line_item in order.get("line_items", []):
            # created_atをDATE型に変換
            created_date = datetime.fromisoformat(order["created_at"].replace('Z', '+00:00')).date()
            
            row = {
                "date": created_date,
                "order_id": order["id"],
                "lineitem_id": line_item["id"],
                "product_id": line_item.get("product_id"),
                "variant_id": line_item.get("variant_id"),
                "sku": line_item.get("sku"),
                "title": line_item.get("title"),
                "qty": line_item.get("quantity"),
                "price": float(line_item.get("price", 0)),
                "order_total": float(order["total_price"]),
                "created_at": order["created_at"],
                "currency": order["currency"],
                "total_price": float(order["total_price"]),
                "total_discounts": float(order.get("total_discounts", 0)),
                "shipping_lines": len(order.get("shipping_lines", [])),
                "tax_lines": len(order.get("tax_lines", [])),
            }
            rows.append(row)
    
    return pd.DataFrame(rows)


def fetch_products_incremental(updated_at_min: str, limit: int = 250) -> pd.DataFrame:
    """
    増分でShopify商品データを取得
    
    Args:
        updated_at_min: 最小更新日時 (ISO 8601形式)
        limit: 1回のリクエストで取得する件数
    
    Returns:
        DataFrame: 商品データ
    """
    base_url = _get_base_url()
    url = f"{base_url}/products.json?limit={limit}&updated_at_min={updated_at_min}"
    
    products = []
    
    while True:
        try:
            data = _make_request(url)
            batch_products = data.get("products", [])
            products.extend(batch_products)
            
            # 次のページがあるかチェック
            link_header = requests.get(url, headers=_get_headers()).headers.get("Link", "")
            next_url = _extract_next_url(link_header)
            
            if not next_url:
                break
            url = next_url
            
        except Exception as e:
            print(f"Shopify API エラー: {e}")
            break
    
    # 商品データの正規化
    rows = []
    for product in products:
        for variant in product.get("variants", []):
            row = {
                "product_id": product["id"],
                "product_title": product["title"],
                "product_handle": product["handle"],
                "product_type": product.get("product_type"),
                "vendor": product.get("vendor"),
                "variant_id": variant["id"],
                "variant_title": variant.get("title"),
                "sku": variant.get("sku"),
                "price": float(variant.get("price", 0)),
                "compare_at_price": float(variant.get("compare_at_price", 0)) if variant.get("compare_at_price") else None,
                "inventory_quantity": variant.get("inventory_quantity", 0),
                "weight": variant.get("weight"),
                "weight_unit": variant.get("weight_unit"),
                "created_at": product["created_at"],
                "updated_at": product["updated_at"],
            }
            rows.append(row)
    
    return pd.DataFrame(rows)


def fetch_customers_incremental(updated_at_min: str, limit: int = 250) -> pd.DataFrame:
    """
    増分でShopify顧客データを取得
    
    Args:
        updated_at_min: 最小更新日時 (ISO 8601形式)
        limit: 1回のリクエストで取得する件数
    
    Returns:
        DataFrame: 顧客データ
    """
    base_url = _get_base_url()
    url = f"{base_url}/customers.json?limit={limit}&updated_at_min={updated_at_min}"
    
    customers = []
    
    while True:
        try:
            data = _make_request(url)
            batch_customers = data.get("customers", [])
            customers.extend(batch_customers)
            
            # 次のページがあるかチェック
            link_header = requests.get(url, headers=_get_headers()).headers.get("Link", "")
            next_url = _extract_next_url(link_header)
            
            if not next_url:
                break
            url = next_url
            
        except Exception as e:
            print(f"Shopify API エラー: {e}")
            break
    
    # 顧客データの正規化
    rows = []
    for customer in customers:
        row = {
            "customer_id": customer["id"],
            "email": customer.get("email"),
            "first_name": customer.get("first_name"),
            "last_name": customer.get("last_name"),
            "orders_count": customer.get("orders_count", 0),
            "total_spent": float(customer.get("total_spent", 0)),
            "created_at": customer["created_at"],
            "updated_at": customer["updated_at"],
        }
        rows.append(row)
    
    return pd.DataFrame(rows)


def fetch_shopify_all_incremental(start_date: str, end_date: str = None) -> dict:
    """
    全Shopifyデータを増分で取得
    
    Args:
        start_date: 開始日 (YYYY-MM-DD)
        end_date: 終了日 (YYYY-MM-DD) - オプション
    
    Returns:
        dict: 各データタイプのDataFrame
    """
    return {
        "orders": fetch_orders_incremental(start_date),
        "products": fetch_products_incremental(start_date),
        "customers": fetch_customers_incremental(start_date),
    }
