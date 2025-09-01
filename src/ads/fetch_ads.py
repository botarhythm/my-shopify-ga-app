#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Google Ads Data Fetcher
Fetches data from Google Ads API with caching and retry logic
"""

import os
import sys
import pandas as pd
import logging
from typing import List, Dict, Optional
from datetime import datetime, timedelta
from tenacity import retry, stop_after_attempt, wait_exponential
from google.ads.googleads.errors import GoogleAdsException
from google.ads.googleads.client import GoogleAdsClient

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.ads.google_ads_client import create_google_ads_client
from src.ads.gaql_queries import GAQLQueryBuilder, GAQLQueries
from src.ads.schemas import validate_dataframe

logger = logging.getLogger(__name__)


class GoogleAdsDataFetcher:
    """Fetches data from Google Ads API with caching and retry logic"""
    
    def __init__(self, config_path: str = "config/google_ads.yaml"):
        """Initialize with config file"""
        self.config_path = config_path
        self.query_builder = GAQLQueryBuilder(config_path)
        self.client: Optional[GoogleAdsClient] = None
        self.customer_id: Optional[str] = None
        self.client_factory: Optional['GoogleAdsClientFactory'] = None
    
    def _get_client(self) -> GoogleAdsClient:
        """Get or create Google Ads client"""
        if self.client is None:
            try:
                self.client = create_google_ads_client(self.config_path)
                # Get customer ID from client factory
                from src.ads.google_ads_client import GoogleAdsClientFactory
                self.client_factory = GoogleAdsClientFactory(self.config_path)
                self.customer_id = self.client_factory.get_customer_id()
                
                # MCCベーシック対応のログ出力
                if self.client_factory.is_basic_mcc():
                    logger.info(f"MCCベーシックアカウントでGoogle Ads client作成: {self.customer_id}")
                    restrictions = self.client_factory.get_mcc_restrictions()
                    if restrictions.get("rate_limit_conservative"):
                        logger.info("MCCベーシック: APIリクエスト頻度を制限します")
                else:
                    logger.info(f"Google Ads client created for customer: {self.customer_id}")
            except Exception as e:
                logger.error(f"Failed to create Google Ads client: {e}")
                raise
        
        return self.client
    
    def _get_retry_strategy(self):
        """Get retry strategy based on MCC account type"""
        if self.client_factory and self.client_factory.is_basic_mcc():
            restrictions = self.client_factory.get_mcc_restrictions()
            if restrictions.get("rate_limit_conservative"):
                # MCCベーシック: より控えめなリトライ戦略
                return retry(
                    stop=stop_after_attempt(2),
                    wait=wait_exponential(multiplier=2, min=8, max=20),
                    reraise=True
                )
        
        # 標準のリトライ戦略
        return retry(
            stop=stop_after_attempt(3),
            wait=wait_exponential(multiplier=1, min=4, max=10),
            reraise=True
        )
    
    def _execute_query(self, query: str, customer_id: str) -> List[Dict]:
        """Execute GAQL query with retry logic"""
        # MCCベーシック対応: 動的にリトライ戦略を適用
        retry_strategy = self._get_retry_strategy()
        
        @retry_strategy
        def _execute_with_retry():
            try:
                client = self._get_client()
                ga_service = client.get_service("GoogleAdsService")
                
                # MCCベーシック対応: リクエスト前の待機時間
                if self.client_factory and self.client_factory.is_basic_mcc():
                    restrictions = self.client_factory.get_mcc_restrictions()
                    if restrictions.get("rate_limit_conservative"):
                        import time
                        time.sleep(1)  # 1秒待機
                
                logger.debug(f"Executing query for customer {customer_id}")
                response = ga_service.search(customer_id=customer_id, query=query)
                
                # Convert response to list of dictionaries
                results = []
                for row in response:
                    row_dict = {}
                    
                    # Google Ads API v21の新しい構造に対応
                    try:
                        # キャンペーン情報
                        if hasattr(row, 'campaign') and row.campaign:
                            campaign = row.campaign
                            if hasattr(campaign, 'id') and campaign.id:
                                row_dict['campaign.id'] = campaign.id
                            if hasattr(campaign, 'name') and campaign.name:
                                row_dict['campaign.name'] = campaign.name
                        
                        # セグメント情報（日付など）
                        if hasattr(row, 'segments') and row.segments:
                            segments = row.segments
                            if hasattr(segments, 'date') and segments.date:
                                row_dict['segments.date'] = segments.date
                        
                        # メトリクス情報
                        if hasattr(row, 'metrics') and row.metrics:
                            metrics = row.metrics
                            if hasattr(metrics, 'impressions') and metrics.impressions:
                                row_dict['metrics.impressions'] = metrics.impressions
                            if hasattr(metrics, 'clicks') and metrics.clicks:
                                row_dict['metrics.clicks'] = metrics.clicks
                            if hasattr(metrics, 'cost_micros') and metrics.cost_micros:
                                row_dict['metrics.cost_micros'] = metrics.cost_micros
                            if hasattr(metrics, 'ctr') and metrics.ctr:
                                row_dict['metrics.ctr'] = metrics.ctr
                            if hasattr(metrics, 'average_cpc') and metrics.average_cpc:
                                row_dict['metrics.average_cpc'] = metrics.average_cpc
                            if hasattr(metrics, 'conversions') and metrics.conversions:
                                row_dict['metrics.conversions'] = metrics.conversions
                            if hasattr(metrics, 'conversions_value') and metrics.conversions_value:
                                row_dict['metrics.conversions_value'] = metrics.conversions_value
                        
                        # 広告グループ情報（広告グループデータの場合）
                        if hasattr(row, 'ad_group') and row.ad_group:
                            ad_group = row.ad_group
                            if hasattr(ad_group, 'id') and ad_group.id:
                                row_dict['ad_group.id'] = ad_group.id
                            if hasattr(ad_group, 'name') and ad_group.name:
                                row_dict['ad_group.name'] = ad_group.name
                        
                        # キーワード情報（キーワードデータの場合）
                        if hasattr(row, 'ad_group_criterion') and row.ad_group_criterion:
                            criterion = row.ad_group_criterion
                            if hasattr(criterion, 'keyword') and criterion.keyword:
                                keyword = criterion.keyword
                                if hasattr(keyword, 'text') and keyword.text:
                                    row_dict['ad_group_criterion.keyword.text'] = keyword.text
                    
                    except Exception as e:
                        logger.warning(f"行の処理中にエラー: {e}")
                        # フォールバック：基本的な属性のみ取得
                        try:
                            for attr_name in dir(row):
                                if not attr_name.startswith('_'):
                                    try:
                                        value = getattr(row, attr_name)
                                        if hasattr(value, 'value'):
                                            row_dict[attr_name] = value.value
                                        else:
                                            row_dict[attr_name] = value
                                    except:
                                        continue
                        except:
                            logger.error(f"フォールバック処理も失敗: {e}")
                            row_dict = {'error': str(e)}
                    
                    results.append(row_dict)
                
                account_type = "MCCベーシック" if (self.client_factory and self.client_factory.is_basic_mcc()) else "標準"
                logger.info(f"Query executed successfully ({account_type}), returned {len(results)} rows")
                return results
                
            except GoogleAdsException as e:
                logger.error(f"Google Ads API error: {e}")
                raise
            except Exception as e:
                logger.error(f"Error executing query: {e}")
                raise
        
        return _execute_with_retry()
    
    def _clean_campaign_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """キャンペーンデータのクリーニングと前処理"""
        if df.empty:
            return df
        
        try:
            # campaign_idを文字列に変換
            if 'campaign_id' in df.columns:
                df['campaign_id'] = df['campaign_id'].astype(str)
            
            # 数値フィールドのNaNを0に置換
            numeric_columns = ['impressions', 'clicks', 'cost_micros', 'conversions', 'conversion_value']
            for col in numeric_columns:
                if col in df.columns:
                    df[col] = df[col].fillna(0)
                    df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
            
            # CTRの計算（clicks / impressions）
            if 'impressions' in df.columns and 'clicks' in df.columns:
                df['ctr'] = df.apply(
                    lambda row: (row['clicks'] / row['impressions'] * 100) if row['impressions'] > 0 else 0, 
                    axis=1
                )
            
            # 平均CPCの計算（cost_micros / clicks / 1000000）
            if 'cost_micros' in df.columns and 'clicks' in df.columns:
                df['avg_cpc'] = df.apply(
                    lambda row: (row['cost_micros'] / row['clicks'] / 1000000) if row['clicks'] > 0 else 0, 
                    axis=1
                )
            
            # 日付の標準化（文字列形式に戻す）
            if 'date' in df.columns:
                df['date'] = pd.to_datetime(df['date'], errors='coerce')
                df = df.dropna(subset=['date'])
                # スキーマ検証用に文字列形式に戻す
                df['date'] = df['date'].dt.strftime('%Y-%m-%d')
            
            logger.info(f"データクリーニング完了: {len(df)}行")
            return df
            
        except Exception as e:
            logger.error(f"データクリーニング中にエラー: {e}")
            return df
    
    def fetch_campaign_data(self, start_date: str, end_date: str) -> pd.DataFrame:
        """Fetch campaign level data"""
        try:
            query = self.query_builder.build_campaign_query(start_date, end_date)
            customer_id = self.customer_id or os.getenv("GOOGLE_ADS_CUSTOMER_ID")
            
            if not customer_id:
                raise ValueError("Customer ID not found")
            
            results = self._execute_query(query, customer_id)
            
            if not results:
                logger.warning("No campaign data returned")
                return pd.DataFrame()
            
            # Convert to DataFrame
            df = pd.DataFrame(results)
            
            # Rename columns to match schema
            column_mapping = {
                'segments.date': 'date',
                'campaign.id': 'campaign_id',
                'campaign.name': 'campaign_name',
                'metrics.impressions': 'impressions',
                'metrics.clicks': 'clicks',
                'metrics.ctr': 'ctr',
                'metrics.cost_micros': 'cost_micros',
                'metrics.average_cpc': 'avg_cpc',
                'metrics.conversions': 'conversions',
                'metrics.conversions_value': 'conversion_value'
            }
            
            df = df.rename(columns=column_mapping)
            
            # データ前処理とクリーニング
            df = self._clean_campaign_data(df)
            
            # Validate data
            df = validate_dataframe(df, "campaign")
            
            logger.info(f"Fetched {len(df)} campaign rows")
            return df
            
        except Exception as e:
            logger.error(f"Error fetching campaign data: {e}")
            raise
    
    def fetch_ad_group_data(self, start_date: str, end_date: str) -> pd.DataFrame:
        """Fetch ad group level data"""
        try:
            query = self.query_builder.build_ad_group_query(start_date, end_date)
            customer_id = self.customer_id or os.getenv("GOOGLE_ADS_CUSTOMER_ID")
            
            if not customer_id:
                raise ValueError("Customer ID not found")
            
            results = self._execute_query(query, customer_id)
            
            if not results:
                logger.warning("No ad group data returned")
                return pd.DataFrame()
            
            # Convert to DataFrame
            df = pd.DataFrame(results)
            
            # Rename columns to match schema
            column_mapping = {
                'segments.date': 'date',
                'campaign.id': 'campaign_id',
                'campaign.name': 'campaign_name',
                'ad_group.id': 'ad_group_id',
                'ad_group.name': 'ad_group_name',
                'metrics.impressions': 'impressions',
                'metrics.clicks': 'clicks',
                'metrics.ctr': 'ctr',
                'metrics.cost_micros': 'cost_micros',
                'metrics.average_cpc': 'avg_cpc',
                'metrics.conversions': 'conversions',
                'metrics.conversions_value': 'conversion_value'
            }
            
            df = df.rename(columns=column_mapping)
            
            # Validate data
            df = validate_dataframe(df, "ad_group")
            
            logger.info(f"Fetched {len(df)} ad group rows")
            return df
            
        except Exception as e:
            logger.error(f"Error fetching ad group data: {e}")
            raise
    
    def fetch_keyword_data(self, start_date: str, end_date: str) -> pd.DataFrame:
        """Fetch keyword level data"""
        try:
            query = self.query_builder.build_keyword_query(start_date, end_date)
            customer_id = self.customer_id or os.getenv("GOOGLE_ADS_CUSTOMER_ID")
            
            if not customer_id:
                raise ValueError("Customer ID not found")
            
            results = self._execute_query(query, customer_id)
            
            if not results:
                logger.warning("No keyword data returned")
                return pd.DataFrame()
            
            # Convert to DataFrame
            df = pd.DataFrame(results)
            
            # Rename columns to match schema
            column_mapping = {
                'segments.date': 'date',
                'campaign.id': 'campaign_id',
                'campaign.name': 'campaign_name',
                'ad_group.id': 'ad_group_id',
                'ad_group.name': 'ad_group_name',
                'ad_group_criterion.keyword.text': 'keyword',
                'metrics.impressions': 'impressions',
                'metrics.clicks': 'clicks',
                'metrics.ctr': 'ctr',
                'metrics.cost_micros': 'cost_micros',
                'metrics.average_cpc': 'avg_cpc',
                'metrics.conversions': 'conversions',
                'metrics.conversions_value': 'conversion_value'
            }
            
            df = df.rename(columns=column_mapping)
            
            # Validate data
            df = validate_dataframe(df, "keyword")
            
            logger.info(f"Fetched {len(df)} keyword rows")
            return df
            
        except Exception as e:
            logger.error(f"Error fetching keyword data: {e}")
            raise
    
    def fetch_ads_data(self, start_date: str, end_date: str, levels: List[str]) -> Dict[str, pd.DataFrame]:
        """Fetch data for multiple levels"""
        results = {}
        
        for level in levels:
            try:
                if level == "campaign":
                    results[level] = self.fetch_campaign_data(start_date, end_date)
                elif level == "ad_group":
                    results[level] = self.fetch_ad_group_data(start_date, end_date)
                elif level == "keyword":
                    results[level] = self.fetch_keyword_data(start_date, end_date)
                else:
                    logger.warning(f"Unknown level: {level}")
                    continue
                
                # Cache raw data
                self._cache_raw_data(results[level], level, start_date, end_date)
                
            except Exception as e:
                logger.error(f"Error fetching {level} data: {e}")
                results[level] = pd.DataFrame()
        
        return results
    
    def _cache_raw_data(self, df: pd.DataFrame, level: str, start_date: str, end_date: str) -> None:
        """Cache raw data to CSV file"""
        try:
            if df.empty:
                return
            
            cache_dir = "data/ads/raw"
            os.makedirs(cache_dir, exist_ok=True)
            
            filename = f"{level}_{start_date}_{end_date}.csv"
            filepath = os.path.join(cache_dir, filename)
            
            df.to_csv(filepath, index=False)
            logger.info(f"Cached {level} data to {filepath}")
            
        except Exception as e:
            logger.error(f"Error caching {level} data: {e}")
    
    def get_cached_data(self, level: str, start_date: str, end_date: str) -> Optional[pd.DataFrame]:
        """Get cached data if available"""
        try:
            cache_dir = "data/ads/raw"
            filename = f"{level}_{start_date}_{end_date}.csv"
            filepath = os.path.join(cache_dir, filename)
            
            if os.path.exists(filepath):
                df = pd.read_csv(filepath)
                logger.info(f"Loaded cached {level} data from {filepath}")
                return df
            
            return None
            
        except Exception as e:
            logger.error(f"Error loading cached {level} data: {e}")
            return None


def fetch_ads_data(start_date: str, end_date: str, levels: List[str], 
                   use_cache: bool = True, config_path: str = "config/google_ads.yaml") -> Dict[str, pd.DataFrame]:
    """Convenience function to fetch Google Ads data"""
    fetcher = GoogleAdsDataFetcher(config_path)
    
    results = {}
    
    for level in levels:
        try:
            # Check cache first
            if use_cache:
                cached_data = fetcher.get_cached_data(level, start_date, end_date)
                if cached_data is not None:
                    results[level] = cached_data
                    continue
            
            # Fetch from API
            if level == "campaign":
                results[level] = fetcher.fetch_campaign_data(start_date, end_date)
            elif level == "ad_group":
                results[level] = fetcher.fetch_ad_group_data(start_date, end_date)
            elif level == "keyword":
                results[level] = fetcher.fetch_keyword_data(start_date, end_date)
            else:
                logger.warning(f"Unknown level: {level}")
                results[level] = pd.DataFrame()
                
        except Exception as e:
            logger.error(f"Error fetching {level} data: {e}")
            results[level] = pd.DataFrame()
    
    return results


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Fetch Google Ads data")
    parser.add_argument("--start", required=True, help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end", required=True, help="End date (YYYY-MM-DD)")
    parser.add_argument("--levels", default="campaign,ad_group,keyword", 
                       help="Comma-separated list of levels to fetch")
    parser.add_argument("--config", default="config/google_ads.yaml", 
                       help="Config file path")
    parser.add_argument("--no-cache", action="store_true", help="Don't use cache")
    
    args = parser.parse_args()
    
    # Setup logging
    logging.basicConfig(level=logging.INFO)
    
    # Parse levels
    levels = [level.strip() for level in args.levels.split(",")]
    
    try:
        print(f"Fetching Google Ads data from {args.start} to {args.end}")
        print(f"Levels: {levels}")
        
        results = fetch_ads_data(
            start_date=args.start,
            end_date=args.end,
            levels=levels,
            use_cache=not args.no_cache,
            config_path=args.config
        )
        
        for level, df in results.items():
            print(f"{level}: {len(df)} rows")
            if not df.empty:
                print(f"  Columns: {list(df.columns)}")
                print(f"  Date range: {df['date'].min()} to {df['date'].max()}")
        
        print("✅ Data fetch completed successfully")
        
    except Exception as e:
        print(f"❌ Error fetching data: {e}")
        sys.exit(1)
