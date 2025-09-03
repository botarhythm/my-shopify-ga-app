#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Google Ads API OAuth 2.0 Setup
Creates new refresh token with proper scopes for Google Ads API
"""

import os
import pickle
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials

# Google Ads API scopes
SCOPES = [
    'https://www.googleapis.com/auth/adwords',
    'https://www.googleapis.com/auth/analytics.readonly'
]

def setup_google_ads_oauth():
    """Setup OAuth 2.0 for Google Ads API"""
    
    creds = None
    token_path = 'data/raw/token.pickle'
    client_secret_path = 'data/raw/client_secret_159450887000-7ic0t1o3jef858l192rodo6fju1b62qf.apps.googleusercontent.com.json'
    
    # Load existing token if available
    if os.path.exists(token_path):
        with open(token_path, 'rb') as token:
            creds = pickle.load(token)
    
    # If there are no (valid) credentials available, let the user log in
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            print("Refreshing expired token...")
            creds.refresh(Request())
        else:
            print("Creating new OAuth 2.0 credentials...")
            flow = InstalledAppFlow.from_client_secrets_file(
                client_secret_path, SCOPES)
            creds = flow.run_local_server(port=0)
        
        # Save the credentials for the next run
        with open(token_path, 'wb') as token:
            pickle.dump(creds, token)
    
    print("✅ OAuth 2.0 setup completed successfully!")
    print(f"Refresh token: {creds.refresh_token}")
    print(f"Token expired: {creds.expired}")
    print(f"Scopes: {creds.scopes}")
    
    return creds

if __name__ == "__main__":
    try:
        creds = setup_google_ads_oauth()
        print("\n🎉 Google Ads API OAuth 2.0 setup completed!")
        print("You can now use the Google Ads API with proper authentication.")
    except Exception as e:
        print(f"❌ Error during OAuth setup: {e}")
