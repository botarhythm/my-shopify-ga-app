#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Pydantic schemas for Google Ads data
Provides typed DataFrames and validation
"""

from typing import Optional, List, Dict, Any
from datetime import datetime, date
from pydantic import BaseModel, Field, validator
import pandas as pd
import logging

logger = logging.getLogger(__name__)


class CampaignRow(BaseModel):
    """Schema for campaign level data"""
    date: str = Field(..., description="Date in YYYY-MM-DD format")
    campaign_id: str = Field(..., description="Campaign ID")
    campaign_name: str = Field(..., description="Campaign name")
    impressions: int = Field(0, description="Number of impressions")
    clicks: int = Field(0, description="Number of clicks")
    ctr: float = Field(0.0, description="Click-through rate")
    cost: float = Field(0.0, description="Cost in JPY")
    avg_cpc: float = Field(0.0, description="Average cost per click")
    conversions: float = Field(0.0, description="Number of conversions")
    conversion_value: float = Field(0.0, description="Conversion value")
    cpc: float = Field(0.0, description="Calculated cost per click")
    cpa: float = Field(0.0, description="Calculated cost per acquisition")
    roas: float = Field(0.0, description="Return on ad spend")
    
    @validator('date')
    def validate_date_format(cls, v):
        """Validate date format"""
        try:
            datetime.strptime(v, "%Y-%m-%d")
            return v
        except ValueError:
            raise ValueError("Date must be in YYYY-MM-DD format")
    
    @validator('ctr', 'cost', 'avg_cpc', 'conversions', 'conversion_value', 'cpc', 'cpa', 'roas')
    def validate_non_negative(cls, v):
        """Validate non-negative values"""
        if v < 0:
            return 0.0
        return v
    
    @validator('impressions', 'clicks')
    def validate_non_negative_int(cls, v):
        """Validate non-negative integers"""
        if v < 0:
            return 0
        return v


class AdGroupRow(BaseModel):
    """Schema for ad group level data"""
    date: str = Field(..., description="Date in YYYY-MM-DD format")
    campaign_id: str = Field(..., description="Campaign ID")
    campaign_name: str = Field(..., description="Campaign name")
    ad_group_id: str = Field(..., description="Ad group ID")
    ad_group_name: str = Field(..., description="Ad group name")
    impressions: int = Field(0, description="Number of impressions")
    clicks: int = Field(0, description="Number of clicks")
    ctr: float = Field(0.0, description="Click-through rate")
    cost: float = Field(0.0, description="Cost in JPY")
    avg_cpc: float = Field(0.0, description="Average cost per click")
    conversions: float = Field(0.0, description="Number of conversions")
    conversion_value: float = Field(0.0, description="Conversion value")
    cpc: float = Field(0.0, description="Calculated cost per click")
    cpa: float = Field(0.0, description="Calculated cost per acquisition")
    roas: float = Field(0.0, description="Return on ad spend")
    
    @validator('date')
    def validate_date_format(cls, v):
        """Validate date format"""
        try:
            datetime.strptime(v, "%Y-%m-%d")
            return v
        except ValueError:
            raise ValueError("Date must be in YYYY-MM-DD format")
    
    @validator('ctr', 'cost', 'avg_cpc', 'conversions', 'conversion_value', 'cpc', 'cpa', 'roas')
    def validate_non_negative(cls, v):
        """Validate non-negative values"""
        if v < 0:
            return 0.0
        return v
    
    @validator('impressions', 'clicks')
    def validate_non_negative_int(cls, v):
        """Validate non-negative integers"""
        if v < 0:
            return 0
        return v


class KeywordRow(BaseModel):
    """Schema for keyword level data"""
    date: str = Field(..., description="Date in YYYY-MM-DD format")
    campaign_id: str = Field(..., description="Campaign ID")
    campaign_name: str = Field(..., description="Campaign name")
    ad_group_id: str = Field(..., description="Ad group ID")
    ad_group_name: str = Field(..., description="Ad group name")
    keyword: str = Field(..., description="Keyword text")
    impressions: int = Field(0, description="Number of impressions")
    clicks: int = Field(0, description="Number of clicks")
    ctr: float = Field(0.0, description="Click-through rate")
    cost: float = Field(0.0, description="Cost in JPY")
    avg_cpc: float = Field(0.0, description="Average cost per click")
    conversions: float = Field(0.0, description="Number of conversions")
    conversion_value: float = Field(0.0, description="Conversion value")
    cpc: float = Field(0.0, description="Calculated cost per click")
    cpa: float = Field(0.0, description="Calculated cost per acquisition")
    roas: float = Field(0.0, description="Return on ad spend")
    
    @validator('date')
    def validate_date_format(cls, v):
        """Validate date format"""
        try:
            datetime.strptime(v, "%Y-%m-%d")
            return v
        except ValueError:
            raise ValueError("Date must be in YYYY-MM-DD format")
    
    @validator('ctr', 'cost', 'avg_cpc', 'conversions', 'conversion_value', 'cpc', 'cpa', 'roas')
    def validate_non_negative(cls, v):
        """Validate non-negative values"""
        if v < 0:
            return 0.0
        return v
    
    @validator('impressions', 'clicks')
    def validate_non_negative_int(cls, v):
        """Validate non-negative integers"""
        if v < 0:
            return 0
        return v


class AdsCampaignDailyFact(BaseModel):
    """Schema for daily campaign fact table"""
    date: str = Field(..., description="Date in YYYY-MM-DD format")
    campaign_id: str = Field(..., description="Campaign ID")
    campaign_name: str = Field(..., description="Campaign name")
    ads_cost: float = Field(0.0, description="Google Ads cost")
    ads_clicks: int = Field(0, description="Google Ads clicks")
    ads_impressions: int = Field(0, description="Google Ads impressions")
    conversions: float = Field(0.0, description="Google Ads conversions")
    conv_value: float = Field(0.0, description="Google Ads conversion value")
    cpc: float = Field(0.0, description="Cost per click")
    cpa: float = Field(0.0, description="Cost per acquisition")
    roas: float = Field(0.0, description="Return on ad spend")
    
    @validator('date')
    def validate_date_format(cls, v):
        """Validate date format"""
        try:
            datetime.strptime(v, "%Y-%m-%d")
            return v
        except ValueError:
            raise ValueError("Date must be in YYYY-MM-DD format")


class AdsCampaignGA4Bridge(BaseModel):
    """Schema for Ads-GA4 bridge table"""
    date: str = Field(..., description="Date in YYYY-MM-DD format")
    campaign_name: str = Field(..., description="Campaign name")
    source: str = Field("google", description="Traffic source")
    medium: str = Field("cpc", description="Traffic medium")
    sessions: int = Field(0, description="GA4 sessions")
    engaged_sessions: int = Field(0, description="GA4 engaged sessions")
    pdp_views: int = Field(0, description="Product detail page views")
    add_to_cart: int = Field(0, description="Add to cart events")
    purchases: int = Field(0, description="Purchase events")
    ga4_revenue: float = Field(0.0, description="GA4 revenue")
    
    @validator('date')
    def validate_date_format(cls, v):
        """Validate date format"""
        try:
            datetime.strptime(v, "%Y-%m-%d")
            return v
        except ValueError:
            raise ValueError("Date must be in YYYY-MM-DD format")


class AdsCampaignShopifySales(BaseModel):
    """Schema for Ads-Shopify sales table"""
    date: str = Field(..., description="Date in YYYY-MM-DD format")
    campaign_name: str = Field(..., description="Campaign name")
    shopify_orders: int = Field(0, description="Number of Shopify orders")
    shopify_revenue: float = Field(0.0, description="Shopify revenue")
    top_products: str = Field("[]", description="Top products JSON string")
    
    @validator('date')
    def validate_date_format(cls, v):
        """Validate date format"""
        try:
            datetime.strptime(v, "%Y-%m-%d")
            return v
        except ValueError:
            raise ValueError("Date must be in YYYY-MM-DD format")


class AdsCampaignRollup(BaseModel):
    """Schema for denormalized campaign rollup table"""
    date: str = Field(..., description="Date in YYYY-MM-DD format")
    campaign_id: str = Field(..., description="Campaign ID")
    campaign_name: str = Field(..., description="Campaign name")
    ads_cost: float = Field(0.0, description="Google Ads cost")
    ads_clicks: int = Field(0, description="Google Ads clicks")
    ads_impressions: int = Field(0, description="Google Ads impressions")
    ads_conversions: float = Field(0.0, description="Google Ads conversions")
    ads_conv_value: float = Field(0.0, description="Google Ads conversion value")
    ga4_sessions: int = Field(0, description="GA4 sessions")
    ga4_purchases: int = Field(0, description="GA4 purchases")
    ga4_revenue: float = Field(0.0, description="GA4 revenue")
    shopify_orders: int = Field(0, description="Shopify orders")
    shopify_revenue: float = Field(0.0, description="Shopify revenue")
    cpc: float = Field(0.0, description="Cost per click")
    cpa: float = Field(0.0, description="Cost per acquisition")
    roas: float = Field(0.0, description="Return on ad spend")
    
    @validator('date')
    def validate_date_format(cls, v):
        """Validate date format"""
        try:
            datetime.strptime(v, "%Y-%m-%d")
            return v
        except ValueError:
            raise ValueError("Date must be in YYYY-MM-DD format")


class AdsDataValidator:
    """Validator for Google Ads data"""
    
    @staticmethod
    def validate_campaign_data(df: pd.DataFrame) -> pd.DataFrame:
        """Validate and clean campaign data"""
        try:
            # Convert to list of CampaignRow objects
            rows = []
            for _, row in df.iterrows():
                try:
                    campaign_row = CampaignRow(**row.to_dict())
                    rows.append(campaign_row.dict())
                except Exception as e:
                    logger.warning(f"Invalid campaign row: {e}")
                    continue
            
            # Convert back to DataFrame
            validated_df = pd.DataFrame(rows)
            logger.info(f"Validated {len(validated_df)} campaign rows")
            return validated_df
            
        except Exception as e:
            logger.error(f"Error validating campaign data: {e}")
            return df
    
    @staticmethod
    def validate_ad_group_data(df: pd.DataFrame) -> pd.DataFrame:
        """Validate and clean ad group data"""
        try:
            # Convert to list of AdGroupRow objects
            rows = []
            for _, row in df.iterrows():
                try:
                    ad_group_row = AdGroupRow(**row.to_dict())
                    rows.append(ad_group_row.dict())
                except Exception as e:
                    logger.warning(f"Invalid ad group row: {e}")
                    continue
            
            # Convert back to DataFrame
            validated_df = pd.DataFrame(rows)
            logger.info(f"Validated {len(validated_df)} ad group rows")
            return validated_df
            
        except Exception as e:
            logger.error(f"Error validating ad group data: {e}")
            return df
    
    @staticmethod
    def validate_keyword_data(df: pd.DataFrame) -> pd.DataFrame:
        """Validate and clean keyword data"""
        try:
            # Convert to list of KeywordRow objects
            rows = []
            for _, row in df.iterrows():
                try:
                    keyword_row = KeywordRow(**row.to_dict())
                    rows.append(keyword_row.dict())
                except Exception as e:
                    logger.warning(f"Invalid keyword row: {e}")
                    continue
            
            # Convert back to DataFrame
            validated_df = pd.DataFrame(rows)
            logger.info(f"Validated {len(validated_df)} keyword rows")
            return validated_df
            
        except Exception as e:
            logger.error(f"Error validating keyword data: {e}")
            return df


def get_schema_for_level(level: str) -> type:
    """Get the appropriate schema for a given level"""
    schemas = {
        "campaign": CampaignRow,
        "ad_group": AdGroupRow,
        "keyword": KeywordRow
    }
    
    if level not in schemas:
        raise ValueError(f"Unknown level: {level}")
    
    return schemas[level]


def validate_dataframe(df: pd.DataFrame, level: str) -> pd.DataFrame:
    """Validate DataFrame using appropriate schema"""
    validator = AdsDataValidator()
    
    if level == "campaign":
        return validator.validate_campaign_data(df)
    elif level == "ad_group":
        return validator.validate_ad_group_data(df)
    elif level == "keyword":
        return validator.validate_keyword_data(df)
    else:
        raise ValueError(f"Unknown level: {level}")


if __name__ == "__main__":
    # Test schema validation
    test_data = {
        "date": "2025-08-30",
        "campaign_id": "123456789",
        "campaign_name": "Test Campaign",
        "impressions": 1000,
        "clicks": 50,
        "ctr": 0.05,
        "cost": 5000.0,
        "avg_cpc": 100.0,
        "conversions": 5.0,
        "conversion_value": 25000.0,
        "cpc": 100.0,
        "cpa": 1000.0,
        "roas": 5.0
    }
    
    try:
        campaign_row = CampaignRow(**test_data)
        print("✅ Schema validation successful")
        print(f"Campaign: {campaign_row.campaign_name}")
        print(f"ROAS: {campaign_row.roas}")
    except Exception as e:
        print(f"❌ Schema validation failed: {e}")
