#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Generate synthetic Google Ads data for UI demonstration
Creates realistic test data when real Google Ads credentials are not available
"""

import os
import sys
import pandas as pd
import numpy as np
import logging
from datetime import datetime, timedelta
import json
from typing import Dict

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

logger = logging.getLogger(__name__)


class GoogleAdsFixtureGenerator:
    """Generates synthetic Google Ads data for testing and demonstration"""
    
    def __init__(self):
        """Initialize fixture generator"""
        self.campaign_names = [
            "コーヒー豆ブランド認知",
            "エチオピア豆プロモーション", 
            "ニカラグア豆セール",
            "新商品ローンチ",
            "ブランド検索",
            "季節商品キャンペーン",
            "リターゲティング",
            "ブランド名検索"
        ]
        
        self.ad_group_names = [
            "エチオピア シダモ",
            "ニカラグア マラゴジッペ",
            "ブラジル サントス",
            "コロンビア スプレモ",
            "グアテマラ アンティグア",
            "コスタリカ タラズ",
            "ペルー チャンチャマヨ",
            "メキシコ チアパス"
        ]
        
        self.keywords = [
            "コーヒー豆",
            "エチオピア コーヒー",
            "ニカラグア コーヒー",
            "スペシャルティ コーヒー",
            "焙煎 コーヒー豆",
            "コーヒー豆 通販",
            "コーヒー豆 オンライン",
            "コーヒー豆 お取り寄せ"
        ]
    
    def generate_campaign_data(self, start_date: str, end_date: str) -> pd.DataFrame:
        """Generate synthetic campaign level data"""
        start = datetime.strptime(start_date, "%Y-%m-%d")
        end = datetime.strptime(end_date, "%Y-%m-%d")
        days = (end - start).days + 1
        
        data = []
        for i in range(days):
            current_date = start + timedelta(days=i)
            date_str = current_date.strftime("%Y-%m-%d")
            
            for campaign_name in self.campaign_names:
                # Generate realistic metrics
                impressions = np.random.randint(1000, 10000)
                clicks = np.random.randint(50, 500)
                ctr = clicks / impressions if impressions > 0 else 0
                cost_micros = np.random.randint(5000000, 50000000)  # 5-50 JPY
                cost = cost_micros / 1_000_000
                avg_cpc = cost / clicks if clicks > 0 else 0
                conversions = np.random.randint(0, max(1, clicks // 10))
                conversion_value = conversions * np.random.randint(2000, 8000)
                
                # Calculate derived metrics
                cpc = cost / clicks if clicks > 0 else 0
                cpa = cost / conversions if conversions > 0 else 0
                roas = conversion_value / cost if cost > 0 else 0
                
                data.append({
                    'date': date_str,
                    'campaign_id': f"campaign_{hash(campaign_name) % 1000000}",
                    'campaign_name': campaign_name,
                    'impressions': impressions,
                    'clicks': clicks,
                    'ctr': round(ctr, 4),
                    'cost_micros': cost_micros,
                    'cost': round(cost, 2),
                    'avg_cpc': round(avg_cpc, 2),
                    'conversions': round(conversions, 2),
                    'conversion_value': round(conversion_value, 2),
                    'cpc': round(cpc, 2),
                    'cpa': round(cpa, 2),
                    'roas': round(roas, 2)
                })
        
        return pd.DataFrame(data)
    
    def generate_ad_group_data(self, start_date: str, end_date: str) -> pd.DataFrame:
        """Generate synthetic ad group level data"""
        start = datetime.strptime(start_date, "%Y-%m-%d")
        end = datetime.strptime(end_date, "%Y-%m-%d")
        days = (end - start).days + 1
        
        data = []
        for i in range(days):
            current_date = start + timedelta(days=i)
            date_str = current_date.strftime("%Y-%m-%d")
            
            for campaign_name in self.campaign_names[:4]:  # Use first 4 campaigns
                for ad_group_name in self.ad_group_names[:3]:  # Use first 3 ad groups per campaign
                    # Generate realistic metrics
                    impressions = np.random.randint(500, 5000)
                    clicks = np.random.randint(20, 200)
                    ctr = clicks / impressions if impressions > 0 else 0
                    cost_micros = np.random.randint(2000000, 20000000)  # 2-20 JPY
                    cost = cost_micros / 1_000_000
                    avg_cpc = cost / clicks if clicks > 0 else 0
                    conversions = np.random.randint(0, max(1, clicks // 15))
                    conversion_value = conversions * np.random.randint(1500, 6000)
                    
                    # Calculate derived metrics
                    cpc = cost / clicks if clicks > 0 else 0
                    cpa = cost / conversions if conversions > 0 else 0
                    roas = conversion_value / cost if cost > 0 else 0
                    
                    data.append({
                        'date': date_str,
                        'campaign_id': f"campaign_{hash(campaign_name) % 1000000}",
                        'campaign_name': campaign_name,
                        'ad_group_id': f"adgroup_{hash(ad_group_name) % 1000000}",
                        'ad_group_name': ad_group_name,
                        'impressions': impressions,
                        'clicks': clicks,
                        'ctr': round(ctr, 4),
                        'cost_micros': cost_micros,
                        'cost': round(cost, 2),
                        'avg_cpc': round(avg_cpc, 2),
                        'conversions': round(conversions, 2),
                        'conversion_value': round(conversion_value, 2),
                        'cpc': round(cpc, 2),
                        'cpa': round(cpa, 2),
                        'roas': round(roas, 2)
                    })
        
        return pd.DataFrame(data)
    
    def generate_keyword_data(self, start_date: str, end_date: str) -> pd.DataFrame:
        """Generate synthetic keyword level data"""
        start = datetime.strptime(start_date, "%Y-%m-%d")
        end = datetime.strptime(end_date, "%Y-%m-%d")
        days = (end - start).days + 1
        
        data = []
        for i in range(days):
            current_date = start + timedelta(days=i)
            date_str = current_date.strftime("%Y-%m-%d")
            
            for campaign_name in self.campaign_names[:3]:  # Use first 3 campaigns
                for ad_group_name in self.ad_group_names[:2]:  # Use first 2 ad groups per campaign
                    for keyword in self.keywords[:4]:  # Use first 4 keywords per ad group
                        # Generate realistic metrics
                        impressions = np.random.randint(200, 2000)
                        clicks = np.random.randint(10, 100)
                        ctr = clicks / impressions if impressions > 0 else 0
                        cost_micros = np.random.randint(1000000, 10000000)  # 1-10 JPY
                        cost = cost_micros / 1_000_000
                        avg_cpc = cost / clicks if clicks > 0 else 0
                        conversions = np.random.randint(0, max(1, clicks // 20))
                        conversion_value = conversions * np.random.randint(1000, 5000)
                        
                        # Calculate derived metrics
                        cpc = cost / clicks if clicks > 0 else 0
                        cpa = cost / conversions if conversions > 0 else 0
                        roas = conversion_value / cost if cost > 0 else 0
                        
                        data.append({
                            'date': date_str,
                            'campaign_id': f"campaign_{hash(campaign_name) % 1000000}",
                            'campaign_name': campaign_name,
                            'ad_group_id': f"adgroup_{hash(ad_group_name) % 1000000}",
                            'ad_group_name': ad_group_name,
                            'keyword': keyword,
                            'impressions': impressions,
                            'clicks': clicks,
                            'ctr': round(ctr, 4),
                            'cost_micros': cost_micros,
                            'cost': round(cost, 2),
                            'avg_cpc': round(avg_cpc, 2),
                            'conversions': round(conversions, 2),
                            'conversion_value': round(conversion_value, 2),
                            'cpc': round(cpc, 2),
                            'cpa': round(cpa, 2),
                            'roas': round(roas, 2)
                        })
        
        return pd.DataFrame(data)
    
    def generate_attribution_data(self, start_date: str, end_date: str) -> Dict[str, pd.DataFrame]:
        """Generate synthetic attribution data for Ads-GA4-Shopify bridge"""
        start = datetime.strptime(start_date, "%Y-%m-%d")
        end = datetime.strptime(end_date, "%Y-%m-%d")
        days = (end - start).days + 1
        
        # Generate Ads-GA4 bridge data
        ga4_bridge_data = []
        shopify_sales_data = []
        
        for i in range(days):
            current_date = start + timedelta(days=i)
            date_str = current_date.strftime("%Y-%m-%d")
            
            for campaign_name in self.campaign_names[:4]:
                # GA4 bridge data
                sessions = np.random.randint(100, 1000)
                engaged_sessions = int(sessions * np.random.uniform(0.6, 0.9))
                pdp_views = int(sessions * np.random.uniform(0.3, 0.7))
                add_to_cart = int(pdp_views * np.random.uniform(0.1, 0.3))
                purchases = int(add_to_cart * np.random.uniform(0.2, 0.5))
                ga4_revenue = purchases * np.random.randint(3000, 8000)
                
                ga4_bridge_data.append({
                    'date': date_str,
                    'campaign_name': campaign_name,
                    'source': 'google',
                    'medium': 'cpc',
                    'sessions': sessions,
                    'engaged_sessions': engaged_sessions,
                    'pdp_views': pdp_views,
                    'add_to_cart': add_to_cart,
                    'purchases': purchases,
                    'ga4_revenue': round(ga4_revenue, 2)
                })
                
                # Shopify sales data
                shopify_orders = int(purchases * np.random.uniform(0.8, 1.2))
                shopify_revenue = shopify_orders * np.random.randint(2500, 7500)
                top_products = [
                    {"product": "エチオピア シダモ", "quantity": np.random.randint(1, 5)},
                    {"product": "ニカラグア マラゴジッペ", "quantity": np.random.randint(1, 3)},
                    {"product": "ブラジル サントス", "quantity": np.random.randint(1, 4)}
                ]
                
                shopify_sales_data.append({
                    'date': date_str,
                    'campaign_name': campaign_name,
                    'shopify_orders': shopify_orders,
                    'shopify_revenue': round(shopify_revenue, 2),
                    'top_products': json.dumps(top_products, ensure_ascii=False)
                })
        
        return {
            'ga4_bridge': pd.DataFrame(ga4_bridge_data),
            'shopify_sales': pd.DataFrame(shopify_sales_data)
        }
    
    def save_fixtures(self, start_date: str, end_date: str) -> None:
        """Generate and save all fixture data"""
        try:
            cache_dir = "data/ads/cache"
            os.makedirs(cache_dir, exist_ok=True)
            
            # Generate campaign data
            campaign_df = self.generate_campaign_data(start_date, end_date)
            campaign_file = os.path.join(cache_dir, f"campaign_{start_date}_{end_date}.parquet")
            campaign_df.to_parquet(campaign_file, index=False)
            logger.info(f"Generated campaign fixture: {len(campaign_df)} rows")
            
            # Generate ad group data
            ad_group_df = self.generate_ad_group_data(start_date, end_date)
            ad_group_file = os.path.join(cache_dir, f"ad_group_{start_date}_{end_date}.parquet")
            ad_group_df.to_parquet(ad_group_file, index=False)
            logger.info(f"Generated ad group fixture: {len(ad_group_df)} rows")
            
            # Generate keyword data
            keyword_df = self.generate_keyword_data(start_date, end_date)
            keyword_file = os.path.join(cache_dir, f"keyword_{start_date}_{end_date}.parquet")
            keyword_df.to_parquet(keyword_file, index=False)
            logger.info(f"Generated keyword fixture: {len(keyword_df)} rows")
            
            # Generate attribution data
            attribution_data = self.generate_attribution_data(start_date, end_date)
            
            ga4_bridge_file = os.path.join(cache_dir, f"ga4_bridge_{start_date}_{end_date}.parquet")
            attribution_data['ga4_bridge'].to_parquet(ga4_bridge_file, index=False)
            logger.info(f"Generated GA4 bridge fixture: {len(attribution_data['ga4_bridge'])} rows")
            
            shopify_sales_file = os.path.join(cache_dir, f"shopify_sales_{start_date}_{end_date}.parquet")
            attribution_data['shopify_sales'].to_parquet(shopify_sales_file, index=False)
            logger.info(f"Generated Shopify sales fixture: {len(attribution_data['shopify_sales'])} rows")
            
            # Create rollup data
            rollup_data = self.create_rollup_data(campaign_df, attribution_data)
            rollup_file = os.path.join(cache_dir, f"rollup_{start_date}_{end_date}.parquet")
            rollup_data.to_parquet(rollup_file, index=False)
            logger.info(f"Generated rollup fixture: {len(rollup_data)} rows")
            
            print("✅ Fixture data generated successfully")
            
        except Exception as e:
            logger.error(f"Error generating fixtures: {e}")
            raise
    
    def create_rollup_data(self, campaign_df: pd.DataFrame, attribution_data: Dict[str, pd.DataFrame]) -> pd.DataFrame:
        """Create denormalized rollup data"""
        try:
            # Merge campaign data with GA4 bridge
            ga4_bridge = attribution_data['ga4_bridge']
            shopify_sales = attribution_data['shopify_sales']
            
            # Aggregate campaign data by date and campaign
            campaign_agg = campaign_df.groupby(['date', 'campaign_id', 'campaign_name']).agg({
                'cost': 'sum',
                'clicks': 'sum',
                'impressions': 'sum',
                'conversions': 'sum',
                'conversion_value': 'sum'
            }).reset_index()
            
            # Merge with GA4 bridge
            rollup = campaign_agg.merge(
                ga4_bridge[['date', 'campaign_name', 'sessions', 'purchases', 'ga4_revenue']],
                on=['date', 'campaign_name'],
                how='left'
            )
            
            # Merge with Shopify sales
            rollup = rollup.merge(
                shopify_sales[['date', 'campaign_name', 'shopify_orders', 'shopify_revenue']],
                on=['date', 'campaign_name'],
                how='left'
            )
            
            # Fill NaN values
            rollup = rollup.fillna(0)
            
            # Calculate derived metrics
            rollup['cpc'] = np.where(rollup['clicks'] > 0, rollup['cost'] / rollup['clicks'], 0)
            rollup['cpa'] = np.where(rollup['conversions'] > 0, rollup['cost'] / rollup['conversions'], 0)
            rollup['roas'] = np.where(rollup['cost'] > 0, rollup['shopify_revenue'] / rollup['cost'], 0)
            
            # Round numeric values
            numeric_cols = ['cost', 'cpc', 'cpa', 'roas', 'ga4_revenue', 'shopify_revenue']
            for col in numeric_cols:
                if col in rollup.columns:
                    rollup[col] = rollup[col].round(2)
            
            return rollup
            
        except Exception as e:
            logger.error(f"Error creating rollup data: {e}")
            return pd.DataFrame()


def generate_fixtures(start_date: str, end_date: str) -> None:
    """Convenience function to generate fixture data"""
    generator = GoogleAdsFixtureGenerator()
    generator.save_fixtures(start_date, end_date)


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Generate Google Ads fixture data")
    parser.add_argument("--start", default="2025-08-01", help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end", default="2025-08-31", help="End date (YYYY-MM-DD)")
    
    args = parser.parse_args()
    
    # Setup logging
    logging.basicConfig(level=logging.INFO)
    
    try:
        print(f"Generating Google Ads fixture data from {args.start} to {args.end}")
        generate_fixtures(args.start, args.end)
        
    except Exception as e:
        print(f"❌ Error generating fixtures: {e}")
        sys.exit(1)
