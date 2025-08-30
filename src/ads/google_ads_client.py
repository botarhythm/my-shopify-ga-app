#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Google Ads API Client Factory
Handles OAuth authentication and client creation
"""

import os
import yaml
from typing import Optional, Dict, Any
from google.ads.googleads.client import GoogleAdsClient
from google.ads.googleads.errors import GoogleAdsException
import logging

logger = logging.getLogger(__name__)


class GoogleAdsClientFactory:
    """Google Ads API client factory with OAuth support"""
    
    def __init__(self, config_path: str = "config/google_ads.yaml"):
        """Initialize with config file path"""
        self.config_path = config_path
        self.config = self._load_config()
        self._client: Optional[GoogleAdsClient] = None
    
    def _load_config(self) -> Dict[str, Any]:
        """Load configuration from YAML file"""
        try:
            with open(self.config_path, 'r', encoding='utf-8') as f:
                config = yaml.safe_load(f)
            
            # Replace environment variables
            self._replace_env_vars(config)
            return config
        except FileNotFoundError:
            logger.warning(f"Config file {self.config_path} not found, using defaults")
            return self._get_default_config()
        except Exception as e:
            logger.error(f"Error loading config: {e}")
            return self._get_default_config()
    
    def _replace_env_vars(self, config: Dict[str, Any]) -> None:
        """Replace ${VAR} placeholders with environment variables"""
        def replace_in_dict(d: Dict[str, Any]) -> None:
            for key, value in d.items():
                if isinstance(value, str) and value.startswith("${") and value.endswith("}"):
                    env_var = value[2:-1]
                    d[key] = os.getenv(env_var, "")
                elif isinstance(value, dict):
                    replace_in_dict(value)
        
        replace_in_dict(config)
    
    def _get_default_config(self) -> Dict[str, Any]:
        """Get default configuration"""
        return {
            "customer": {
                "customer_id": os.getenv("GOOGLE_ADS_CUSTOMER_ID", ""),
                "login_customer_id": os.getenv("GOOGLE_ADS_LOGIN_CUSTOMER_ID", ""),
                "developer_token": os.getenv("GOOGLE_ADS_DEVELOPER_TOKEN", "")
            },
            "api": {
                "use_proto_plus": True,
                "test_account": True
            }
        }
    
    def _validate_credentials(self) -> bool:
        """Validate that required credentials are present"""
        required_vars = [
            "GOOGLE_ADS_CLIENT_ID",
            "GOOGLE_ADS_CLIENT_SECRET", 
            "GOOGLE_ADS_REFRESH_TOKEN",
            "GOOGLE_ADS_DEVELOPER_TOKEN",
            "GOOGLE_ADS_CUSTOMER_ID"
        ]
        
        missing_vars = []
        for var in required_vars:
            if not os.getenv(var):
                missing_vars.append(var)
        
        if missing_vars:
            logger.error(f"Missing required environment variables: {missing_vars}")
            return False
        
        return True
    
    def create_client(self) -> GoogleAdsClient:
        """Create and return Google Ads client"""
        if not self._validate_credentials():
            raise ValueError("Missing required Google Ads credentials")
        
        try:
            # Create client configuration
            client_config = {
                "developer_token": os.getenv("GOOGLE_ADS_DEVELOPER_TOKEN"),
                "client_id": os.getenv("GOOGLE_ADS_CLIENT_ID"),
                "client_secret": os.getenv("GOOGLE_ADS_CLIENT_SECRET"),
                "refresh_token": os.getenv("GOOGLE_ADS_REFRESH_TOKEN"),
                "use_proto_plus": True  # Required for newer versions
            }
            
            # Create client using configuration
            client = GoogleAdsClient.load_from_dict(client_config)
            
            # Validate client by making a test request
            self._test_client(client)
            
            self._client = client
            logger.info("Google Ads client created successfully")
            return client
            
        except GoogleAdsException as e:
            logger.error(f"Google Ads API error: {e}")
            raise
        except Exception as e:
            logger.error(f"Error creating Google Ads client: {e}")
            raise
    
    def _test_client(self, client: GoogleAdsClient) -> None:
        """Test client by making a simple query"""
        try:
            customer_id = self.config["customer"]["customer_id"]
            if not customer_id:
                customer_id = os.getenv("GOOGLE_ADS_CUSTOMER_ID")
            
            if not customer_id:
                raise ValueError("Customer ID not found in config or environment")
            
            # Simple test query for test account
            ga_service = client.get_service("GoogleAdsService")
            query = f"""
                SELECT 
                    customer.id,
                    customer.descriptive_name
                FROM customer 
                WHERE customer.id = {customer_id}
                LIMIT 1
            """
            
            response = ga_service.search(customer_id=customer_id, query=query)
            
            # Check if we got a response
            for row in response:
                logger.info(f"Connected to Google Ads account: {row.customer.descriptive_name}")
                break
            else:
                logger.warning("No customer data returned from test query")
                
        except Exception as e:
            logger.error(f"Client test failed: {e}")
            raise
    
    def get_customer_id(self) -> str:
        """Get customer ID from config"""
        customer_id = self.config["customer"]["customer_id"]
        if not customer_id:
            customer_id = os.getenv("GOOGLE_ADS_CUSTOMER_ID")
        
        if not customer_id:
            raise ValueError("Customer ID not found in config or environment")
        
        return customer_id
    
    def get_login_customer_id(self) -> Optional[str]:
        """Get login customer ID if using manager account"""
        login_customer_id = self.config["customer"]["login_customer_id"]
        if not login_customer_id:
            login_customer_id = os.getenv("GOOGLE_ADS_LOGIN_CUSTOMER_ID")
        
        return login_customer_id if login_customer_id else None


def create_google_ads_client(config_path: str = "config/google_ads.yaml") -> GoogleAdsClient:
    """Convenience function to create Google Ads client"""
    factory = GoogleAdsClientFactory(config_path)
    return factory.create_client()


if __name__ == "__main__":
    # Test client creation
    try:
        client = create_google_ads_client()
        print("✅ Google Ads client created successfully")
    except Exception as e:
        print(f"❌ Error creating client: {e}")
