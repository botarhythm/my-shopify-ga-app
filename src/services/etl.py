import pandas as pd
import os
from datetime import datetime, timedelta
from typing import Optional
from src.database.db import db
from src.connectors.shopify_client import shopify_client
from src.connectors.square_client import square_client
from src.connectors.ga4_client import ga4_client
from src.connectors.google_ads_client import google_ads_client

class ETLService:
    def __init__(self):
        self.db = db

    def run_init(self):
        """Initialize database schemas."""
        print("Initializing database schemas...")
        con = self.db.get_connection()
        try:
            with open("src/database/sql/init.sql", "r", encoding="utf-8") as f:
                con.execute(f.read())
            
            # Load seeds
            self._load_seeds(con)
        finally:
            con.close()

    def _load_seeds(self, con):
        """Load seed data."""
        print("Loading seeds...")
        try:
            # Product Mapping
            mapping_path = "src/seeds/product_mapping.csv"
            if os.path.exists(mapping_path):
                df_mapping = pd.read_csv(mapping_path)
                con.execute("DELETE FROM seeds.product_mapping") # Full refresh
                con.execute("INSERT INTO seeds.product_mapping SELECT * FROM df_mapping")
                print(f"  Loaded {len(df_mapping)} product mappings.")
            else:
                print("  No product mapping seed found.")
        except Exception as e:
            print(f"  Error loading seeds: {e}")

    def run_etl(self, start_date: str, end_date: str):
        """
        Run full ETL process.
        start_date: YYYY-MM-DD
        end_date: YYYY-MM-DD
        """
        print(f"Starting ETL for {start_date} to {end_date}...")
        
        # 1. Extract & Load (Raw)
        self._load_shopify(start_date) # Shopify uses created_at_min, so just start_date
        self._load_square(start_date, end_date)
        self._load_square_orders(start_date, end_date)  # New: Square product data
        self._load_ga4(start_date, end_date)
        self._load_ads(start_date, end_date)

        # 2. Transform (Core & Marts)
        self._run_transformations()
        
        print("ETL Completed Successfully.")

    def _load_shopify(self, start_date: str):
        print("Fetching Shopify data...")
        # Convert YYYY-MM-DD to ISO format for Shopify
        start_iso = f"{start_date}T00:00:00"
        df = shopify_client.fetch_orders(start_iso)
        if not df.empty:
            print(f"  Loaded {len(df)} Shopify orders.")
            
            if 'updated_at' not in df.columns:
                df['updated_at'] = datetime.now()
                
            # Schema: date, order_id, lineitem_id, product_id, variant_id, sku, title, qty, price, order_total, created_at, currency, total_price, financial_status, updated_at
            cols = ['date', 'order_id', 'lineitem_id', 'product_id', 'variant_id', 'sku', 'title', 'qty', 'price', 'order_total', 'created_at', 'currency', 'total_price', 'financial_status', 'updated_at']
            for c in cols:
                if c not in df.columns:
                    df[c] = None
            df = df[cols]
            
            self._append_to_table("raw.shopify_orders", df)
        else:
            print("  No Shopify data found.")

    def _load_square(self, start_date: str, end_date: str):
        print("Fetching Square data...")
        df = square_client.fetch_payments(start_date, end_date)
        if not df.empty:
            print(f"  Loaded {len(df)} Square payments.")
            # Ensure columns match raw.square_payments schema
            # Schema: payment_id, created_at, date, amount, currency, status, order_id, source_type, card_brand, updated_at
            # Add missing columns if needed
            if 'updated_at' not in df.columns:
                df['updated_at'] = datetime.now()
            
            # Select and order columns
            cols = ['payment_id', 'created_at', 'date', 'amount', 'currency', 'status', 'order_id', 'source_type', 'card_brand', 'updated_at']
            # Fill missing cols with None
            for c in cols:
                if c not in df.columns:
                    df[c] = None
            
            df = df[cols]
            self._append_to_table("raw.square_payments", df)
        else:
            print("  No Square data found.")

    def _load_square_orders(self, start_date: str, end_date: str):
        print("Fetching Square Orders (product data)...")
        df = square_client.fetch_orders(start_date, end_date)
        
        if not df.empty:
            print(f"  Loaded {len(df)} Square order line items.")
            
            # Add updated_at timestamp
            if 'updated_at' not in df.columns:
                df['updated_at'] = datetime.now()
            
            # Ensure columns match schema
            cols = ['order_id', 'created_at', 'date', 'product_name', 'quantity', 'base_price', 'total_price', 'currency', 'updated_at']
            for c in cols:
                if c not in df.columns:
                    df[c] = None
            
            df = df[cols]
            self._append_to_table("raw.square_orders", df)
        else:
            print("  No Square order data found.")

    def _load_ga4(self, start_date: str, end_date: str):
        print("Fetching GA4 data...")
        # Use correct GA4 API dimension names
        dims = ["date", "sessionSource", "sessionMedium", "sessionCampaignName"]
        metrics = ["sessions", "totalUsers", "totalRevenue"]
        df = ga4_client.fetch_report(start_date, end_date, dims, metrics)
        
        # Rename columns to match DB schema
        if not df.empty:
            df = df.rename(columns={
                "sessionSource": "source",
                "sessionMedium": "medium",
                "sessionCampaignName": "campaign",
                "totalUsers": "users",
                "totalRevenue": "revenue"
            })
            # Ensure date is YYYY-MM-DD (GA4 returns YYYYMMDD)
            if 'date' in df.columns and not df['date'].astype(str).str.contains('-').all():
                 df['date'] = pd.to_datetime(df['date'], format='%Y%m%d').dt.strftime('%Y-%m-%d')
            
            if 'updated_at' not in df.columns:
                df['updated_at'] = datetime.now()
                
            # Schema: date, source, medium, campaign, sessions, users, revenue, updated_at
            cols = ['date', 'source', 'medium', 'campaign', 'sessions', 'users', 'revenue', 'updated_at']
            for c in cols:
                if c not in df.columns:
                    df[c] = None
            df = df[cols]

            print(f"  Loaded {len(df)} GA4 rows.")
            self._append_to_table("raw.ga4_daily", df)
        else:
            print("  No GA4 data found.")

    def _load_ads(self, start_date: str, end_date: str):
        print("Fetching Google Ads data...")
        df = google_ads_client.fetch_campaign_performance(start_date, end_date)
        if not df.empty:
            print(f"  Loaded {len(df)} Ads rows.")
            
            if 'updated_at' not in df.columns:
                df['updated_at'] = datetime.now()
                
            # Schema: date, campaign_id, campaign_name, cost, clicks, impressions, conversions, conversions_value, updated_at
            cols = ['date', 'campaign_id', 'campaign_name', 'cost', 'clicks', 'impressions', 'conversions', 'conversions_value', 'updated_at']
            for c in cols:
                if c not in df.columns:
                    df[c] = None
            df = df[cols]
            
            self._append_to_table("raw.ads_campaign", df)
        else:
            print("  No Ads data found.")

    def _append_to_table(self, table_name: str, df: pd.DataFrame):
        """Append DataFrame to DuckDB table."""
        con = self.db.get_connection()
        try:
            # DuckDB's append is efficient
            con.execute(f"INSERT INTO {table_name} SELECT * FROM df")
        except Exception as e:
            print(f"Error appending to {table_name}: {e}")
            # Optional: Create table if not exists (though init.sql should handle it)
        finally:
            con.close()

    def _run_transformations(self):
        print("Running transformations...")
        con = self.db.get_connection()
        try:
            # Core
            with open("src/database/sql/transform_core.sql", "r", encoding="utf-8") as f:
                # Split by semicolon to execute multiple statements if needed
                # But read() and execute() usually handles one statement or script depending on driver.
                # DuckDB execute() handles multiple statements? 
                # Better to split.
                script = f.read()
                statements = [s.strip() for s in script.split(';') if s.strip()]
                for stmt in statements:
                    con.execute(stmt)
            
            # Marts
            with open("src/database/sql/transform_marts.sql", "r", encoding="utf-8") as f:
                script = f.read()
                statements = [s.strip() for s in script.split(';') if s.strip()]
                for stmt in statements:
                    con.execute(stmt)
                    
            print("  Transformations complete.")
        except Exception as e:
            print(f"Transformation Error: {e}")
        finally:
            con.close()

etl_service = ETLService()

if __name__ == "__main__":
    # Example usage
    etl_service.run_init()
    
    # Default to last 30 days if running directly
    end = datetime.now()
    start = end - timedelta(days=30)
    
    etl_service.run_etl(start.strftime('%Y-%m-%d'), end.strftime('%Y-%m-%d'))
