#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Google Ads Query Language (GAQL) Builders
Constructs GAQL queries for different levels and date ranges
"""

from typing import List, Dict, Any
from datetime import datetime, timedelta
import yaml
import logging

logger = logging.getLogger(__name__)


class GAQLQueryBuilder:
    """Builds GAQL queries for Google Ads API"""
    
    def __init__(self, config_path: str = "config/google_ads.yaml"):
        """Initialize with config file"""
        self.config_path = config_path
        self.config = self._load_config()
    
    def _load_config(self) -> Dict[str, Any]:
        """Load configuration from YAML file"""
        try:
            with open(self.config_path, 'r', encoding='utf-8') as f:
                return yaml.safe_load(f)
        except FileNotFoundError:
            logger.warning(f"Config file {self.config_path} not found, using defaults")
            return self._get_default_config()
        except Exception as e:
            logger.error(f"Error loading config: {e}")
            return self._get_default_config()
    
    def _get_default_config(self) -> Dict[str, Any]:
        """Get default query configuration"""
        return {
            "queries": {
                "campaign": {
                    "fields": [
                        "segments.date",
                        "campaign.id",
                        "campaign.name",
                        "metrics.impressions",
                        "metrics.clicks",
                        "metrics.ctr",
                        "metrics.cost_micros",
                        "metrics.average_cpc",
                        "metrics.conversions",
                        "metrics.conversions_value"
                    ]
                },
                "ad_group": {
                    "fields": [
                        "segments.date",
                        "campaign.id",
                        "campaign.name",
                        "ad_group.id",
                        "ad_group.name",
                        "metrics.impressions",
                        "metrics.clicks",
                        "metrics.ctr",
                        "metrics.cost_micros",
                        "metrics.average_cpc",
                        "metrics.conversions",
                        "metrics.conversions_value"
                    ]
                },
                "keyword": {
                    "fields": [
                        "segments.date",
                        "campaign.id",
                        "campaign.name",
                        "ad_group.id",
                        "ad_group.name",
                        "ad_group_criterion.keyword.text",
                        "metrics.impressions",
                        "metrics.clicks",
                        "metrics.ctr",
                        "metrics.cost_micros",
                        "metrics.average_cpc",
                        "metrics.conversions",
                        "metrics.conversions_value"
                    ]
                }
            }
        }
    
    def build_campaign_query(self, start_date: str, end_date: str) -> str:
        """Build GAQL query for campaign level data"""
        fields = self.config["queries"]["campaign"]["fields"]
        return self._build_query("campaign", fields, start_date, end_date)
    
    def build_ad_group_query(self, start_date: str, end_date: str) -> str:
        """Build GAQL query for ad group level data"""
        fields = self.config["queries"]["ad_group"]["fields"]
        return self._build_query("ad_group", fields, start_date, end_date)
    
    def build_keyword_query(self, start_date: str, end_date: str) -> str:
        """Build GAQL query for keyword level data"""
        fields = self.config["queries"]["keyword"]["fields"]
        return self._build_query("keyword_view", fields, start_date, end_date)
    
    def _build_query(self, resource: str, fields: List[str], start_date: str, end_date: str) -> str:
        """Build GAQL query with given parameters"""
        # Validate dates
        self._validate_date_format(start_date)
        self._validate_date_format(end_date)
        
        # Build SELECT clause
        select_clause = "SELECT\n  " + ",\n  ".join(fields)
        
        # Build FROM clause
        from_clause = f"FROM {resource}"
        
        # Build WHERE clause
        where_clause = f"WHERE segments.date BETWEEN '{start_date}' AND '{end_date}'"
        
        # Combine into full query
        query = f"{select_clause}\n{from_clause}\n{where_clause}"
        
        logger.debug(f"Built GAQL query for {resource}: {query}")
        return query
    
    def _validate_date_format(self, date_str: str) -> None:
        """Validate date format is YYYY-MM-DD"""
        try:
            datetime.strptime(date_str, "%Y-%m-%d")
        except ValueError:
            raise ValueError(f"Invalid date format: {date_str}. Expected YYYY-MM-DD")
    
    def get_default_date_range(self, days: int = None) -> tuple[str, str]:
        """Get default date range (last N days)"""
        if days is None:
            days = self.config.get("date_range", {}).get("default_days", 90)
        
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days)
        
        return start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d")
    
    def get_available_fields(self, level: str) -> List[str]:
        """Get available fields for a given level"""
        if level not in self.config["queries"]:
            raise ValueError(f"Unknown level: {level}")
        
        return self.config["queries"][level]["fields"]


class GAQLQueries:
    """Predefined GAQL queries for common use cases"""
    
    @staticmethod
    def campaign_performance(start_date: str, end_date: str) -> str:
        """Campaign performance query"""
        return f"""
        SELECT
          segments.date,
          campaign.id,
          campaign.name,
          metrics.impressions,
          metrics.clicks,
          metrics.ctr,
          metrics.cost_micros,
          metrics.average_cpc,
          metrics.conversions,
          metrics.conversions_value
        FROM campaign
        WHERE segments.date BETWEEN '{start_date}' AND '{end_date}'
        """
    
    @staticmethod
    def ad_group_performance(start_date: str, end_date: str) -> str:
        """Ad group performance query"""
        return f"""
        SELECT
          segments.date,
          campaign.id,
          campaign.name,
          ad_group.id,
          ad_group.name,
          metrics.impressions,
          metrics.clicks,
          metrics.ctr,
          metrics.cost_micros,
          metrics.average_cpc,
          metrics.conversions,
          metrics.conversions_value
        FROM ad_group
        WHERE segments.date BETWEEN '{start_date}' AND '{end_date}'
        """
    
    @staticmethod
    def keyword_performance(start_date: str, end_date: str) -> str:
        """Keyword performance query"""
        return f"""
        SELECT
          segments.date,
          campaign.id,
          campaign.name,
          ad_group.id,
          ad_group.name,
          ad_group_criterion.keyword.text,
          metrics.impressions,
          metrics.clicks,
          metrics.ctr,
          metrics.cost_micros,
          metrics.average_cpc,
          metrics.conversions,
          metrics.conversions_value
        FROM keyword_view
        WHERE segments.date BETWEEN '{start_date}' AND '{end_date}'
        """
    
    @staticmethod
    def campaign_with_conversion_actions(start_date: str, end_date: str) -> str:
        """Campaign query with conversion action details"""
        return f"""
        SELECT
          segments.date,
          campaign.id,
          campaign.name,
          conversion_action.name,
          metrics.impressions,
          metrics.clicks,
          metrics.ctr,
          metrics.cost_micros,
          metrics.average_cpc,
          metrics.conversions,
          metrics.conversions_value
        FROM campaign
        WHERE segments.date BETWEEN '{start_date}' AND '{end_date}'
        """
    
    @staticmethod
    def search_terms_performance(start_date: str, end_date: str) -> str:
        """Search terms performance query"""
        return f"""
        SELECT
          segments.date,
          campaign.id,
          campaign.name,
          ad_group.id,
          ad_group.name,
          search_term_view.search_term,
          metrics.impressions,
          metrics.clicks,
          metrics.ctr,
          metrics.cost_micros,
          metrics.average_cpc,
          metrics.conversions,
          metrics.conversions_value
        FROM search_term_view
        WHERE segments.date BETWEEN '{start_date}' AND '{end_date}'
        """


def build_query(level: str, start_date: str, end_date: str, config_path: str = "config/google_ads.yaml") -> str:
    """Convenience function to build GAQL query"""
    builder = GAQLQueryBuilder(config_path)
    
    if level == "campaign":
        return builder.build_campaign_query(start_date, end_date)
    elif level == "ad_group":
        return builder.build_ad_group_query(start_date, end_date)
    elif level == "keyword":
        return builder.build_keyword_query(start_date, end_date)
    else:
        raise ValueError(f"Unknown level: {level}")


if __name__ == "__main__":
    # Test query building
    builder = GAQLQueryBuilder()
    start_date, end_date = builder.get_default_date_range(7)
    
    print("Campaign Query:")
    print(builder.build_campaign_query(start_date, end_date))
    print("\nAd Group Query:")
    print(builder.build_ad_group_query(start_date, end_date))
    print("\nKeyword Query:")
    print(builder.build_keyword_query(start_date, end_date))
