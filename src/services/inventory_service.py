import pandas as pd
from typing import List, Dict
from src.connectors.shopify_client import shopify_client
from src.connectors.google_ads_client import google_ads_client

class InventoryService:
    def __init__(self):
        self.shopify = shopify_client
        self.ads = google_ads_client

    def check_and_update_ads(self, dry_run: bool = True, threshold: int = 0) -> List[Dict]:
        """
        Check inventory and update ad status.
        Returns a log of actions taken (or would be taken).
        """
        logs = []
        
        # 1. Fetch Data
        print("Fetching data for Inventory Check...")
        try:
            products_df = self.shopify.fetch_products()
            campaigns_df = self.ads.fetch_active_campaigns()
        except Exception as e:
            print(f"Error fetching data: {e}")
            return [{"status": "error", "message": str(e)}]

        if products_df.empty:
            logs.append({"status": "warning", "message": "No products found in Shopify."})
            return logs
            
        if campaigns_df.empty:
            logs.append({"status": "warning", "message": "No active campaigns found in Google Ads."})
            return logs

        # 2. Analyze Inventory
        # Group by product title to get total inventory across variants if needed, 
        # or just check if ANY variant is in stock? 
        # Usually, if a specific product is out of stock, we want to pause ads for it.
        # Let's assume we pause if ALL variants are out of stock (inventory <= 0).
        
        # Aggregate inventory by Product Title (since we match Campaign Name to Product Title)
        # Normalize titles for matching
        products_df['normalized_title'] = products_df['title'].str.lower().str.strip()
        
        inventory_map = products_df.groupby('normalized_title')['inventory_quantity'].sum().to_dict()
        
        # 3. Match and Decide
        for _, campaign in campaigns_df.iterrows():
            campaign_id = str(campaign['campaign_id'])
            campaign_name = campaign['campaign_name']
            normalized_campaign_name = campaign_name.lower().strip()
            
            # Simple matching logic: Does the campaign name contain a product title?
            matched_product = None
            current_inventory = 0
            
            for product_title, inventory in inventory_map.items():
                if product_title in normalized_campaign_name:
                    matched_product = product_title
                    current_inventory = inventory
                    break
            
            if matched_product:
                if current_inventory <= threshold:
                    action = "PAUSE"
                    reason = f"Out of stock (Inventory: {current_inventory} <= {threshold})"
                    
                    if not dry_run:
                        try:
                            self.ads.pause_campaign(campaign_id)
                            result = "Executed"
                        except Exception as e:
                            result = f"Failed: {e}"
                    else:
                        result = "Dry Run"
                        
                    logs.append({
                        "campaign_name": campaign_name,
                        "product": matched_product,
                        "inventory": int(current_inventory),
                        "action": action,
                        "result": result
                    })
                else:
                    # Optional: Enable if previously paused? 
                    # For now, we only look at ACTIVE campaigns to PAUSE them.
                    # To re-enable, we'd need to fetch PAUSED campaigns too.
                    pass
            else:
                # No product match found for this campaign
                pass

        if not logs:
            logs.append({"status": "info", "message": "No campaigns required updates."})
            
        return logs

inventory_service = InventoryService()
