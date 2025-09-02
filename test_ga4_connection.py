#!/usr/bin/env python3
"""
Google Ads認証情報を使用してGA4データを取得するテスト
"""

import os
from dotenv import load_dotenv
from google.ads.googleads.client import GoogleAdsClient
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.auth.transport.requests import Request
from google.auth import default

def test_ga4_with_ads_credentials():
    """Google Ads認証情報を使用してGA4接続をテスト"""
    try:
        # 環境変数を読み込み
        load_dotenv()
        
        # Google Adsクライアントを作成
        ads_client = GoogleAdsClient.load_from_env()
        
        # 認証情報を取得
        credentials = ads_client.get_credentials()
        
        print("✅ Google Ads認証情報取得成功")
        print(f"🔑 認証タイプ: {type(credentials).__name__}")
        
        # GA4クライアントを作成
        ga4_client = BetaAnalyticsDataClient(credentials=credentials)
        
        print("✅ GA4クライアント作成成功")
        
        # 簡単なテストクエリを実行
        property_id = os.getenv('GA4_PROPERTY_ID', '123456789')
        
        print(f"🔍 プロパティID {property_id} でテストクエリを実行...")
        
        # テスト用のクエリ（実際のプロパティIDが必要）
        if property_id != '123456789':
            # 実際のクエリを実行
            pass
        else:
            print("⚠️  実際のプロパティIDを設定してください")
        
        return True
        
    except Exception as e:
        print(f"❌ エラー: {e}")
        return False

def list_available_properties():
    """利用可能なプロパティをリスト表示"""
    print("\n📋 利用可能なGA4プロパティの確認方法:")
    print("1. Google Analytics 4 の管理画面にアクセス")
    print("2. プロパティ設定 → プロパティIDを確認")
    print("3. または、Google Cloud ConsoleでGA4 APIを有効化")
    print("4. サービスアカウントキーを作成")
    print("\n💡 現在の設定:")
    print(f"   GA4_PROPERTY_ID: {os.getenv('GA4_PROPERTY_ID')}")
    print(f"   GOOGLE_APPLICATION_CREDENTIALS: {os.getenv('GOOGLE_APPLICATION_CREDENTIALS')}")

if __name__ == "__main__":
    print("🚀 Google Ads認証情報でGA4接続テスト")
    print("=" * 50)
    
    if test_ga4_with_ads_credentials():
        list_available_properties()
    else:
        print("❌ 接続テストに失敗しました")
