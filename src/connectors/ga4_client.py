import os
import pickle
import pandas as pd
from typing import List, Optional, Dict, Any
from google.analytics.data_v1beta import BetaAnalyticsDataClient, DateRange, Metric, Dimension, RunReportRequest
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from src.config.settings import settings

SCOPES = ['https://www.googleapis.com/auth/analytics.readonly']

class GA4Client:
    def __init__(self):
        self.property_id = settings.GA4_PROPERTY_ID
        if not self.property_id:
            raise ValueError("GA4_PROPERTY_ID not configured")
        
        self.client = self._get_client()

    def _get_client(self) -> BetaAnalyticsDataClient:
        """Get authenticated GA4 client."""
        # 1. Try Service Account (Preferred for server)
        if settings.GOOGLE_APPLICATION_CREDENTIALS and os.path.exists(settings.GOOGLE_APPLICATION_CREDENTIALS):
            try:
                return BetaAnalyticsDataClient.from_service_account_json(settings.GOOGLE_APPLICATION_CREDENTIALS)
            except Exception as e:
                # If it fails (e.g. it's an OAuth client secret file), fall back to OAuth
                print(f"Service Account load failed (might be OAuth secret): {e}")
        
        # 2. Try OAuth Token (Local dev)
        creds = self._get_oauth_credentials()
        return BetaAnalyticsDataClient(credentials=creds)

    def _get_oauth_credentials(self):
        """Get OAuth credentials from pickle or flow."""
        creds = None
        token_path = settings.DATA_DIR / "ga4_token.pickle"
        
        if os.path.exists(token_path):
            with open(token_path, 'rb') as token:
                creds = pickle.load(token)
        
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                # This requires user interaction, might fail in headless env
                # But keeping it for local dev compatibility
                client_secret = settings.GOOGLE_ADS_CLIENT_SECRET # Using Ads secret as fallback? Or should have specific GA secret?
                # The old code used GOOGLE_APPLICATION_CREDENTIALS for client secret path in flow, which is confusing.
                # I'll assume standard flow if needed, but warn.
                if not settings.GOOGLE_APPLICATION_CREDENTIALS:
                     raise ValueError("No credentials found. Set GOOGLE_APPLICATION_CREDENTIALS.")
                
                flow = InstalledAppFlow.from_client_secrets_file(
                    settings.GOOGLE_APPLICATION_CREDENTIALS, SCOPES)
                creds = flow.run_local_server(port=0)
            
            # Save token
            with open(token_path, 'wb') as token:
                pickle.dump(creds, token)
        
        return creds

    def fetch_report(self, start_date: str, end_date: str, dimensions: List[str], metrics: List[str]) -> pd.DataFrame:
        """
        Fetch GA4 report.
        start_date: YYYY-MM-DD
        end_date: YYYY-MM-DD
        """
        request = RunReportRequest(
            property=f"properties/{self.property_id}",
            date_ranges=[DateRange(start_date=start_date, end_date=end_date)],
            dimensions=[Dimension(name=d) for d in dimensions],
            metrics=[Metric(name=m) for m in metrics],
        )

        try:
            response = self.client.run_report(request)
        except Exception as e:
            print(f"GA4 API Error: {e}")
            return pd.DataFrame()

        rows = []
        for row in response.rows:
            item = {}
            for i, d in enumerate(dimensions):
                item[d] = row.dimension_values[i].value
            for i, m in enumerate(metrics):
                # Try to convert to float/int
                val = row.metric_values[i].value
                try:
                    if '.' in val:
                        item[m] = float(val)
                    else:
                        item[m] = int(val)
                except ValueError:
                    item[m] = val
            rows.append(item)
        
        return pd.DataFrame(rows)

ga4_client = GA4Client()
