import requests
import pandas as pd
from datetime import datetime
from typing import Optional, List, Dict, Any, Generator
from tenacity import retry, stop_after_attempt, wait_exponential
from src.config.settings import settings

class ShopifyClient:
    def __init__(self):
        if not settings.SHOPIFY_SHOP_URL or not settings.SHOPIFY_ACCESS_TOKEN:
            raise ValueError("Shopify credentials not configured")
        
        self.base_url = f"https://{settings.SHOPIFY_SHOP_URL}/admin/api/2024-10"
        self.headers = {
            "X-Shopify-Access-Token": settings.SHOPIFY_ACCESS_TOKEN,
            "Content-Type": "application/json"
        }

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
    def _make_request(self, url: str) -> Dict[str, Any]:
        """Execute Shopify API request with retry logic."""
        response = requests.get(url, headers=self.headers)
        response.raise_for_status()
        return response.json()

    def _extract_next_url(self, link_header: str) -> Optional[str]:
        """Extract next page URL from Link header."""
        if not link_header or 'rel="next"' not in link_header:
            return None
        
        for part in link_header.split(","):
            if 'rel="next"' in part:
                start = part.find("<") + 1
                end = part.find(">")
                return part[start:end]
        return None

    def _fetch_all_pages(self, url: str, key: str) -> Generator[List[Dict[str, Any]], None, None]:
        """Generator to fetch all pages of data."""
        while True:
            try:
                data = self._make_request(url)
                items = data.get(key, [])
                yield items

                link_header = requests.get(url, headers=self.headers).headers.get("Link", "")
                next_url = self._extract_next_url(link_header)
                
                if not next_url:
                    break
                url = next_url
            except Exception as e:
                print(f"Shopify API Error: {e}")
                break

    def fetch_orders(self, start_date: str, limit: int = 250) -> pd.DataFrame:
        """
        Fetch orders incrementally from start_date.
        start_date: ISO 8601 string (e.g., '2025-01-01T00:00:00')
        """
        url = f"{self.base_url}/orders.json?status=any&limit={limit}&created_at_min={start_date}"
        all_orders = []

        for batch in self._fetch_all_pages(url, "orders"):
            all_orders.extend(batch)

        return self._normalize_orders(all_orders)

    def _normalize_orders(self, orders: List[Dict[str, Any]]) -> pd.DataFrame:
        """Normalize order data into a flat DataFrame."""
        rows = []
        for order in orders:
            if order.get("cancelled_at"):
                continue
            
            for line_item in order.get("line_items", []):
                # Helper to safely get amounts
                def _get_amount(obj, key, fallback):
                    val = obj.get(key)
                    if val is not None:
                        try:
                            return float(val)
                        except (ValueError, TypeError):
                            pass
                    return float(obj.get(fallback, 0) or 0)

                created_at_dt = datetime.fromisoformat(order["created_at"].replace('Z', '+00:00'))
                
                row = {
                    "date": created_at_dt.date(),
                    "order_id": str(order["id"]),
                    "lineitem_id": str(line_item["id"]),
                    "product_id": str(line_item.get("product_id")) if line_item.get("product_id") else None,
                    "variant_id": str(line_item.get("variant_id")) if line_item.get("variant_id") else None,
                    "sku": line_item.get("sku"),
                    "title": line_item.get("title"),
                    "qty": int(line_item.get("quantity", 0)),
                    "price": float(line_item.get("price", 0)),
                    "order_total": _get_amount(order, "current_total_price", "total_price"),
                    "created_at": order["created_at"],
                    "currency": order["currency"],
                    "total_price": float(line_item.get("price", 0)) * float(line_item.get("quantity", 0)),
                    "financial_status": order.get("financial_status"),
                }
                rows.append(row)
        
        return pd.DataFrame(rows)

    def fetch_products(self, limit: int = 250) -> pd.DataFrame:
        """
        Fetch all products with inventory levels.
        """
        url = f"{self.base_url}/products.json?limit={limit}"
        all_products = []

        for batch in self._fetch_all_pages(url, "products"):
            all_products.extend(batch)

        return self._normalize_products(all_products)

    def _normalize_products(self, products: List[Dict[str, Any]]) -> pd.DataFrame:
        """Normalize product data to extract inventory."""
        rows = []
        for p in products:
            for v in p.get("variants", []):
                rows.append({
                    "product_id": str(p["id"]),
                    "variant_id": str(v["id"]),
                    "title": p["title"],
                    "variant_title": v["title"],
                    "sku": v.get("sku"),
                    "inventory_quantity": v.get("inventory_quantity", 0),
                    "status": p.get("status"), # active, archived, draft
                    "updated_at": p.get("updated_at")
                })
        return pd.DataFrame(rows)

    def fetch_customers(self, limit: int = 250) -> pd.DataFrame:
        """Fetch customers with contact info."""
        url = f"{self.base_url}/customers.json?limit={limit}"
        all_customers = []
        
        for batch in self._fetch_all_pages(url, "customers"):
            all_customers.extend(batch)
            
        rows = []
        for c in all_customers:
            rows.append({
                "customer_id": str(c["id"]),
                "email": c.get("email"),
                "phone": c.get("phone"),
                "first_name": c.get("first_name"),
                "last_name": c.get("last_name"),
                "total_spent": float(c.get("total_spent", 0)),
                "orders_count": int(c.get("orders_count", 0)),
                "created_at": c.get("created_at")
            })
        return pd.DataFrame(rows)

shopify_client = ShopifyClient()
