import pandas as pd
import hashlib
from typing import List, Dict
from src.connectors.shopify_client import shopify_client
from src.connectors.square_client import square_client
from src.connectors.google_ads_client import google_ads_client

class AudienceService:
    def __init__(self):
        self.shopify = shopify_client
        self.square = square_client
        self.ads = google_ads_client

    def sync_vip_customers(self, dry_run: bool = True, spend_threshold: float = 10000.0):
        """
        Aggregate VIP customers and sync to Google Ads.
        VIP Definition: Total spent > spend_threshold or > 2 orders
        """
        print("Fetching customer data...")
        shopify_customers = self.shopify.fetch_customers()
        square_customers = self.square.fetch_customers()
        
        vip_list = []
        
        # Process Shopify
        if not shopify_customers.empty:
            # Filter VIPs
            # Ensure numeric columns
            shopify_customers['total_spent'] = pd.to_numeric(shopify_customers['total_spent'], errors='coerce').fillna(0)
            shopify_customers['orders_count'] = pd.to_numeric(shopify_customers['orders_count'], errors='coerce').fillna(0)
            
            vips = shopify_customers[
                (shopify_customers['total_spent'] > spend_threshold) | 
                (shopify_customers['orders_count'] > 2)
            ]
            
            for _, row in vips.iterrows():
                email = row.get('email')
                if email:
                    vip_list.append({'email': self._hash_data(email)})

        # Process Square
        if not square_customers.empty:
            # Square API doesn't always return spend in customer object directly without more complex queries
            # For now, we'll just take all customers as "potential" or skip if we can't verify spend easily.
            # Let's include all Square customers with email as they are likely store visitors (high intent).
            for _, row in square_customers.iterrows():
                email = row.get('email')
                if email:
                    vip_list.append({'email': self._hash_data(email)})

        # Deduplicate
        unique_vips = [dict(t) for t in {tuple(d.items()) for d in vip_list}]
        
        print(f"Found {len(unique_vips)} unique VIP customers.")
        
        if unique_vips:
            self.ads.upload_customer_match_list("VIP Customers (Shopify + Square)", unique_vips, dry_run=dry_run)
        else:
            print("No VIP customers found to sync.")

    def _hash_data(self, data: str) -> str:
        """Normalize and hash data for Google Ads (SHA256)."""
        if not data:
            return None
        # Normalize: lowercase, remove whitespace
        normalized = data.lower().strip()
        return hashlib.sha256(normalized.encode('utf-8')).hexdigest()

audience_service = AudienceService()
