import os
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables from .env file
env_path = Path(__file__).parent.parent.parent / '.env'
load_dotenv(env_path)

class Settings:
    # Project Paths
    ROOT_DIR = Path(__file__).parent.parent.parent
    DATA_DIR = ROOT_DIR / "data"
    _db_path = os.getenv("DUCKDB_PATH", str(DATA_DIR / "commerce.duckdb"))
    DB_PATH = str(ROOT_DIR / _db_path) if _db_path and not os.path.isabs(_db_path) else _db_path

    # Shopify
    SHOPIFY_SHOP_URL = os.getenv("SHOPIFY_SHOP_URL")
    SHOPIFY_ACCESS_TOKEN = os.getenv("SHOPIFY_ACCESS_TOKEN")

    # Square
    SQUARE_ACCESS_TOKEN = os.getenv("SQUARE_ACCESS_TOKEN")
    SQUARE_LOCATION_ID = os.getenv("SQUARE_LOCATION_ID")

    # GA4
    GA4_PROPERTY_ID = os.getenv("GA4_PROPERTY_ID")
    _creds_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    GOOGLE_APPLICATION_CREDENTIALS = str(ROOT_DIR / _creds_path) if _creds_path and not os.path.isabs(_creds_path) else _creds_path

    # Google Ads
    GOOGLE_ADS_DEVELOPER_TOKEN = os.getenv("GOOGLE_ADS_DEVELOPER_TOKEN")
    GOOGLE_ADS_CLIENT_ID = os.getenv("GOOGLE_ADS_CLIENT_ID")
    GOOGLE_ADS_CLIENT_SECRET = os.getenv("GOOGLE_ADS_CLIENT_SECRET")
    GOOGLE_ADS_REFRESH_TOKEN = os.getenv("GOOGLE_ADS_REFRESH_TOKEN")
    GOOGLE_ADS_LOGIN_CUSTOMER_ID = os.getenv("GOOGLE_ADS_LOGIN_CUSTOMER_ID")
    GOOGLE_ADS_CUSTOMER_ID = os.getenv("GOOGLE_ADS_CUSTOMER_ID")

    # App
    TIMEZONE = os.getenv("TIMEZONE", "Asia/Tokyo")

    @classmethod
    def validate(cls):
        """Validate that critical environment variables are set."""
        missing = []
        if not cls.SHOPIFY_ACCESS_TOKEN: missing.append("SHOPIFY_ACCESS_TOKEN")
        if not cls.SQUARE_ACCESS_TOKEN: missing.append("SQUARE_ACCESS_TOKEN")
        # Add others as needed
        
        if missing:
            print(f"Warning: Missing environment variables: {', '.join(missing)}")

settings = Settings()
