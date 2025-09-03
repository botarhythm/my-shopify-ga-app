#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Google Ads Data Transformer
Transforms raw Google Ads data into normalized format with derived metrics
"""

import os
import sys
import pandas as pd
import numpy as np
import logging
from typing import Dict, Optional
from datetime import datetime
import json

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.ads.schemas import validate_dataframe

logger = logging.getLogger(__name__)


class GoogleAdsDataTransformer:
    """Transforms Google Ads data with derived metrics and normalization"""
    
    def __init__(self):
        """Initialize transformer"""
        pass
    
    def transform_campaign_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Transform campaign level data"""
        if df.empty:
            return df
        
        try:
            # Convert cost_micros to JPY
            df['cost'] = df['cost_micros'].astype(float) / 1_000_000
            
            # Calculate derived metrics
            df['cpc'] = np.where(df['clicks'] > 0, df['cost'] / df['clicks'], 0)
            df['cpa'] = np.where(df['conversions'] > 0, df['cost'] / df['conversions'], 0)
            df['roas'] = np.where(df['cost'] > 0, df['conversion_value'] / df['cost'], 0)
            
            # Ensure non-negative values
            numeric_columns = ['cost', 'cpc', 'cpa', 'roas', 'ctr', 'avg_cpc', 'conversions', 'conversion_value']
            for col in numeric_columns:
                if col in df.columns:
                    df[col] = df[col].clip(lower=0)
            
            # Round numeric values
            df['cost'] = df['cost'].round(2)
            df['cpc'] = df['cpc'].round(2)
            df['cpa'] = df['cpa'].round(2)
            df['roas'] = df['roas'].round(2)
            df['ctr'] = df['ctr'].round(4)
            df['avg_cpc'] = df['avg_cpc'].round(2)
            df['conversions'] = df['conversions'].round(2)
            df['conversion_value'] = df['conversion_value'].round(2)
            
            # Validate transformed data
            df = validate_dataframe(df, "campaign")
            
            logger.info(f"Transformed {len(df)} campaign rows")
            return df
            
        except Exception as e:
            logger.error(f"Error transforming campaign data: {e}")
            return df
    
    def transform_ad_group_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Transform ad group level data"""
        if df.empty:
            return df
        
        try:
            # Convert cost_micros to JPY
            df['cost'] = df['cost_micros'].astype(float) / 1_000_000
            
            # Calculate derived metrics
            df['cpc'] = np.where(df['clicks'] > 0, df['cost'] / df['clicks'], 0)
            df['cpa'] = np.where(df['conversions'] > 0, df['cost'] / df['conversions'], 0)
            df['roas'] = np.where(df['cost'] > 0, df['conversion_value'] / df['cost'], 0)
            
            # Ensure non-negative values
            numeric_columns = ['cost', 'cpc', 'cpa', 'roas', 'ctr', 'avg_cpc', 'conversions', 'conversion_value']
            for col in numeric_columns:
                if col in df.columns:
                    df[col] = df[col].clip(lower=0)
            
            # Round numeric values
            df['cost'] = df['cost'].round(2)
            df['cpc'] = df['cpc'].round(2)
            df['cpa'] = df['cpa'].round(2)
            df['roas'] = df['roas'].round(2)
            df['ctr'] = df['ctr'].round(4)
            df['avg_cpc'] = df['avg_cpc'].round(2)
            df['conversions'] = df['conversions'].round(2)
            df['conversion_value'] = df['conversion_value'].round(2)
            
            # Validate transformed data
            df = validate_dataframe(df, "ad_group")
            
            logger.info(f"Transformed {len(df)} ad group rows")
            return df
            
        except Exception as e:
            logger.error(f"Error transforming ad group data: {e}")
            return df
    
    def transform_keyword_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Transform keyword level data"""
        if df.empty:
            return df
        
        try:
            # Convert cost_micros to JPY
            df['cost'] = df['cost_micros'].astype(float) / 1_000_000
            
            # Calculate derived metrics
            df['cpc'] = np.where(df['clicks'] > 0, df['cost'] / df['clicks'], 0)
            df['cpa'] = np.where(df['conversions'] > 0, df['cost'] / df['conversions'], 0)
            df['roas'] = np.where(df['cost'] > 0, df['conversion_value'] / df['cost'], 0)
            
            # Ensure non-negative values
            numeric_columns = ['cost', 'cpc', 'cpa', 'roas', 'ctr', 'avg_cpc', 'conversions', 'conversion_value']
            for col in numeric_columns:
                if col in df.columns:
                    df[col] = df[col].clip(lower=0)
            
            # Round numeric values
            df['cost'] = df['cost'].round(2)
            df['cpc'] = df['cpc'].round(2)
            df['cpa'] = df['cpa'].round(2)
            df['roas'] = df['roas'].round(2)
            df['ctr'] = df['ctr'].round(4)
            df['avg_cpc'] = df['avg_cpc'].round(2)
            df['conversions'] = df['conversions'].round(2)
            df['conversion_value'] = df['conversion_value'].round(2)
            
            # Validate transformed data
            df = validate_dataframe(df, "keyword")
            
            logger.info(f"Transformed {len(df)} keyword rows")
            return df
            
        except Exception as e:
            logger.error(f"Error transforming keyword data: {e}")
            return df
    
    def transform_ads_data(self, data_dict: Dict[str, pd.DataFrame]) -> Dict[str, pd.DataFrame]:
        """Transform data for multiple levels"""
        transformed_data = {}
        
        for level, df in data_dict.items():
            try:
                if level == "campaign":
                    transformed_data[level] = self.transform_campaign_data(df)
                elif level == "ad_group":
                    transformed_data[level] = self.transform_ad_group_data(df)
                elif level == "keyword":
                    transformed_data[level] = self.transform_keyword_data(df)
                else:
                    logger.warning(f"Unknown level: {level}")
                    transformed_data[level] = df
                    
            except Exception as e:
                logger.error(f"Error transforming {level} data: {e}")
                transformed_data[level] = df
        
        return transformed_data
    
    def save_processed_data(self, data_dict: Dict[str, pd.DataFrame], start_date: str, end_date: str) -> None:
        """Save processed data to parquet files"""
        try:
            processed_dir = "data/ads/processed"
            os.makedirs(processed_dir, exist_ok=True)
            
            for level, df in data_dict.items():
                if df.empty:
                    continue
                
                filename = f"{level}_{start_date}_{end_date}.parquet"
                filepath = os.path.join(processed_dir, filename)
                
                df.to_parquet(filepath, index=False)
                logger.info(f"Saved processed {level} data to {filepath}")
                
        except Exception as e:
            logger.error(f"Error saving processed data: {e}")
    
    def load_processed_data(self, level: str, start_date: str, end_date: str) -> Optional[pd.DataFrame]:
        """Load processed data from parquet file"""
        try:
            processed_dir = "data/ads/processed"
            filename = f"{level}_{start_date}_{end_date}.parquet"
            filepath = os.path.join(processed_dir, filename)
            
            if os.path.exists(filepath):
                df = pd.read_parquet(filepath)
                logger.info(f"Loaded processed {level} data from {filepath}")
                return df
            
            return None
            
        except Exception as e:
            logger.error(f"Error loading processed {level} data: {e}")
            return None


def transform_ads_data(data_dict: Dict[str, pd.DataFrame], start_date: str, end_date: str, 
                      save_processed: bool = True) -> Dict[str, pd.DataFrame]:
    """Convenience function to transform Google Ads data"""
    transformer = GoogleAdsDataTransformer()
    
    # Transform data
    transformed_data = transformer.transform_ads_data(data_dict)
    
    # Save processed data
    if save_processed:
        transformer.save_processed_data(transformed_data, start_date, end_date)
    
    return transformed_data


def load_processed_ads_data(levels: list, start_date: str, end_date: str) -> Dict[str, pd.DataFrame]:
    """Load processed data for multiple levels"""
    transformer = GoogleAdsDataTransformer()
    data_dict = {}
    
    for level in levels:
        df = transformer.load_processed_data(level, start_date, end_date)
        data_dict[level] = df if df is not None else pd.DataFrame()
    
    return data_dict


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Transform Google Ads data")
    parser.add_argument("--start", required=True, help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end", required=True, help="End date (YYYY-MM-DD)")
    parser.add_argument("--levels", default="campaign,ad_group,keyword", 
                       help="Comma-separated list of levels to transform")
    parser.add_argument("--no-save", action="store_true", help="Don't save processed data")
    
    args = parser.parse_args()
    
    # Setup logging
    logging.basicConfig(level=logging.INFO)
    
    # Parse levels
    levels = [level.strip() for level in args.levels.split(",")]
    
    try:
        print(f"Transforming Google Ads data from {args.start} to {args.end}")
        print(f"Levels: {levels}")
        
        # Load raw data
        from src.ads.fetch_ads import fetch_ads_data
        
        raw_data = fetch_ads_data(
            start_date=args.start,
            end_date=args.end,
            levels=levels,
            use_cache=True
        )
        
        # Transform data
        transformed_data = transform_ads_data(
            data_dict=raw_data,
            start_date=args.start,
            end_date=args.end,
            save_processed=not args.no_save
        )
        
        # Print summary
        for level, df in transformed_data.items():
            print(f"{level}: {len(df)} rows")
            if not df.empty:
                print(f"  Cost range: {df['cost'].min():.2f} - {df['cost'].max():.2f}")
                print(f"  ROAS range: {df['roas'].min():.2f} - {df['roas'].max():.2f}")
                print(f"  CPC range: {df['cpc'].min():.2f} - {df['cpc'].max():.2f}")
        
        print("✅ Data transformation completed successfully")
        
    except Exception as e:
        print(f"❌ Error transforming data: {e}")
        sys.exit(1)
