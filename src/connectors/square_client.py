import pandas as pd
from typing import Optional, List, Dict, Any, Generator
from square.client import Client
from tenacity import retry, stop_after_attempt, wait_exponential
from src.config.settings import settings

class SquareClient:
    def __init__(self):
        if not settings.SQUARE_ACCESS_TOKEN or not settings.SQUARE_LOCATION_ID:
            raise ValueError("Square credentials not configured")
        
        self.client = Client(access_token=settings.SQUARE_ACCESS_TOKEN)
        self.location_id = settings.SQUARE_LOCATION_ID

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
    def fetch_payments(self, start_date: str, end_date: str) -> pd.DataFrame:
        """
        Fetch payments from Square.
        start_date: YYYY-MM-DD
        end_date: YYYY-MM-DD
        """
        cursor = None
        all_payments = []

        while True:
            try:
                result = self.client.payments.list_payments(
                    begin_time=f"{start_date}T00:00:00Z",
                    end_time=f"{end_date}T23:59:59Z",
                    location_id=self.location_id,
                    cursor=cursor,
                    sort_order="ASC"
                )

                if result.is_success():
                    payments = result.body.get("payments", [])
                    all_payments.extend(payments)
                    cursor = result.body.get("cursor")
                    if not cursor:
                        break
                else:
                    print(f"Square API Error: {result.errors}")
                    break
            except Exception as e:
                print(f"Square API Exception: {e}")
                break
        
        return self._normalize_payments(all_payments)

    def _normalize_payments(self, payments: List[Dict[str, Any]]) -> pd.DataFrame:
        """Normalize payments data."""
        rows = []
        for payment in payments:
            amount_money = payment.get("amount_money", {})
            amount = float(amount_money.get("amount", 0))
            currency = amount_money.get("currency")
            
            # Adjust for currency decimals (JPY is 0, others usually 2)
            if currency != "JPY":
                amount = amount / 100.0

            row = {
                "payment_id": payment["id"],
                "customer_id": payment.get("customer_id"), # Added customer_id
                "created_at": payment["created_at"],
                "date": payment["created_at"][:10],
                "amount": amount,
                "currency": currency,
                "status": payment.get("status"),
                "order_id": payment.get("order_id"),
                "source_type": payment.get("source_type"),
                "card_brand": payment.get("card_details", {}).get("card", {}).get("card_brand"),
            }
            rows.append(row)
        
        return pd.DataFrame(rows)

    def fetch_customers(self) -> pd.DataFrame:
        """Fetch customers from Square."""
        cursor = None
        all_customers = []
        
        while True:
            try:
                result = self.client.customers.list_customers(cursor=cursor)
                if result.is_success():
                    customers = result.body.get("customers", [])
                    all_customers.extend(customers)
                    cursor = result.body.get("cursor")
                    if not cursor:
                        break
                else:
                    print(f"Square API Error: {result.errors}")
                    break
            except Exception as e:
                print(f"Square API Exception: {e}")
                break
                
        rows = []
        for c in all_customers:
            rows.append({
                "customer_id": c.get("id"),
                "email": c.get("email_address"),
                "phone": c.get("phone_number"),
                "given_name": c.get("given_name"),
                "family_name": c.get("family_name"),
                "created_at": c.get("created_at")
            })
        return pd.DataFrame(rows)

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
    def fetch_orders(self, start_date: str, end_date: str) -> pd.DataFrame:
        """
        Fetch orders with line items from Square.
        start_date: YYYY-MM-DD
        end_date: YYYY-MM-DD
        """
        cursor = None
        all_orders = []

        while True:
            try:
                # Use SearchOrders API
                result = self.client.orders.search_orders(
                    body={
                        "location_ids": [self.location_id],
                        "query": {
                            "filter": {
                                "date_time_filter": {
                                    "created_at": {
                                        "start_at": f"{start_date}T00:00:00Z",
                                        "end_at": f"{end_date}T23:59:59Z"
                                    }
                                },
                                "state_filter": {
                                    "states": ["COMPLETED"]
                                }
                            },
                            "sort": {
                                "sort_field": "CREATED_AT",
                                "sort_order": "ASC"
                            }
                        },
                        "limit": 500,
                        "cursor": cursor
                    }
                )

                if result.is_success():
                    orders = result.body.get("orders", [])
                    all_orders.extend(orders)
                    cursor = result.body.get("cursor")
                    if not cursor:
                        break
                else:
                    print(f"Square Orders API Error: {result.errors}")
                    break
            except Exception as e:
                print(f"Square Orders API Exception: {e}")
                break

        return self._normalize_orders(all_orders)

    def _normalize_orders(self, orders: List[Dict[str, Any]]) -> pd.DataFrame:
        """Normalize orders data to product-level rows."""
        rows = []
        
        for order in orders:
            order_id = order.get("id")
            created_at = order.get("created_at", "")
            date = created_at[:10] if created_at else None
            
            # Extract line items (products)
            line_items = order.get("line_items", [])
            
            for item in line_items:
                # Get item details
                item_name = item.get("name", "Unknown")
                quantity = float(item.get("quantity", 0))
                
                # Get pricing
                base_price_money = item.get("base_price_money", {})
                total_money = item.get("total_money", {})
                
                base_price = float(base_price_money.get("amount", 0))
                total_price = float(total_money.get("amount", 0))
                currency = total_money.get("currency", "JPY")
                
                # Adjust for currency decimals
                if currency != "JPY":
                    base_price = base_price / 100.0
                    total_price = total_price / 100.0
                
                rows.append({
                    "order_id": order_id,
                    "created_at": created_at,
                    "date": date,
                    "product_name": item_name,
                    "quantity": quantity,
                    "base_price": base_price,
                    "total_price": total_price,
                    "currency": currency
                })
        
        return pd.DataFrame(rows)

square_client = SquareClient()
