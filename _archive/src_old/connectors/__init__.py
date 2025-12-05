"""
データ取得コネクタ統合モジュール
"""
from .ga4 import fetch_ga4_daily_all, fetch_ga4_yoy, fetch_ga4_search_terms
from .google_ads import fetch_ads_campaign_daily, fetch_ads_adgroup_daily, fetch_ads_keyword_daily, fetch_ads_all_daily
from .shopify import fetch_orders_incremental, fetch_products_incremental, fetch_customers_incremental, fetch_shopify_all_incremental
from .square import fetch_payments, fetch_refunds, fetch_orders, fetch_square_all

__all__ = [
    # GA4
    'fetch_ga4_daily_all',
    'fetch_ga4_yoy', 
    'fetch_ga4_search_terms',
    
    # Google Ads
    'fetch_ads_campaign_daily',
    'fetch_ads_adgroup_daily',
    'fetch_ads_keyword_daily',
    'fetch_ads_all_daily',
    
    # Shopify
    'fetch_orders_incremental',
    'fetch_products_incremental',
    'fetch_customers_incremental',
    'fetch_shopify_all_incremental',
    
    # Square
    'fetch_payments',
    'fetch_refunds',
    'fetch_orders',
    'fetch_square_all',
]
